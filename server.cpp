#include "server.h"
#include "raft_state.h"
#include "server_info.h"

#include "generated/AppendEntriesReply_generated.h"
#include "generated/AppendEntries_generated.h"
#include "generated/RequestVoteReply_generated.h"
#include "generated/RequestVote_generated.h"

#include <cstring>
#include <cstdint>
#include <cstdio>
#include <cstdlib>

#include <chrono>
#include <future>
#include <memory>
#include <random>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <condition_variable>
#include <mutex>
#include <sstream>

#include <netdb.h>
#include <sys/socket.h>
#include <sys/types.h>

using namespace std::chrono_literals;

namespace {
size_t BUFSIZE = 1024;
std::uniform_int_distribution<> dist(150, 300);
std::random_device rd{};
std::mt19937 gen(rd());
int GetRandomDuration() {
  return dist(gen);
}

std::vector<ServerInfo> ExtractServerInfo(const std::string& si) {
  size_t i{};

  std::istringstream istr{si};
  std::vector<ServerInfo> infos;
  for (std::string line; std::getline(istr, line, ',');) {
    auto delim_pos = line.find(':');
    std::string ip = line.substr(0, delim_pos);
    uint16_t port = line.substr(delim_pos);
    infos.emplace_back(std::move(ip), port);
  }
  return infos;
}

void FillPeerInfo(const SenderInfo& sender_info, sockaddr_storage& peer_addr) {

}
} // end anonymous namespace

Server::Server(int cluster_size, int server_idx, std::string server_info)
    : praftservice_{std::make_unique<RaftService>()},
    numservers_{cluster_size},
    servers_{server_info} {
  
  servers_ = ExtractServerInfo(server_info);

  std::string port{std::to_string(servers_[server_idx].port)};

  // setup a UDP connection listener
  struct addrinfo hints, *res;
  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_DGRAM;
  hints.ai_flags = AI_PASSIVE;
  hints.ai_protocol = 0; // any protocol
  hints.ai_canonname = nullptr;
  hints.ai_addr = nullptr;
  hints.ai_next = nullptr;

  // if ai_passive is specified in hints.ai_flags and node is NULL then
  // then returned socket will bind to INADDR_ANY
  int s = getaddrinfo(nullptr, port.c_str(), hints, res);
  if (s != 0) {
    perror("getaddrinfo");
    exit(EXIT_FAILURE);
  }

  sockfd_ = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
  if (sockfd_ == -1) {
    perror("socket");
    exit(EXIT_FAILURE);
  }

  if (bind(sockfd_, res->ai_addr, res->ai_addrlen) == -1) {
    perror("bind");
    exit(EXIT_FAILURE);
  }

  freeaddrinfo(res);

  // TODO install a signal handler from cluster
  // listen to sigquit, sigterm

  start();
}

Server::~Server() {
  close(sockfd_);
}

// in a infinite loop
//      listen to message on port
//      on receiving message
//         if received message is Request
void Server::start() {
  while (true) {
    if (praftservice_->state().state_ == State::Follower) {
      praftservice_->state().state_ = doFollowerLoop();
    } else if (praftservice_->state().state_ == State::Candidate) {
      praftservice_->state().state_ = doCandidateLoop();
    } else if (praftservice_->state().state_ == State::Leader) {
      praftservice_->state().state_ = doLeaderLoop();
    }
  }
}

/* ================== FOLLOWER ============== */
State Server::doFollowerLoop() {
  char buf[BUFSIZE];

  while (true) {
    struct sockaddr_storage peer_addr;
    socklen_t peer_addrlen{sizeof(peer_addr)};

    char host[NI_MAXHOST], service[NI_MAXSERV];

    ssize_t n{-1};
    {
      std::unique_lock lck{timer_mutex_};
      std::chrono::milliseconds timeout_duration{GetRandomDuration()};

      auto ret = timer_cv_.wait(lck, timeout_duration, [&]() {
        return n > 0;
      });

      if (ret == std::cv_status::timeout) {
        return State::Candidate;
      }
    }

    n = ::recvfrom(sockfd_, buf, BUFSIZE, 0,
          (struct sockaddr *)&peer_addr, &peer_addrlen);

    if (n > 0) {
      std::string_view req{buf, n};     
      auto async_func = [&]() {
        char host[NI_MAXHOST], service[NI_MAXSERV];
        int s = ::getnameinfo((struct sockaddr *)&peer_addr, peer_addrlen, host,
                              NI_MAXHOST, service, NI_MAXSERV, NI_NUMERICSERV);
        SenderInfo si{.peer_addr = &peer_addr, .peer_addrlen = peer_addrlen};

        if (praftservice_->hasHasHeartBeatInRequest(req)) {          
          sendHeartBeatResponse(si);
          timer_cv_.notify_one();
        } else if (praftservice_->hasRequestVoteInRequest(req)) {
          sendRequestVoteResponse(si);
          timer_cv_.notify_one();
        }
      };

      [[maybe_unused]] auto req_fut = std::async(std::launch::async, async_func);

    } else { 
      return State::Candidate;
    }   
  }
  return State::Candidate;
}
/* ================ END FOLLOWER ============== */

/* ================ START CANDIDATE ============== */
void Server::doCandidateRequestVotes() {
  while (true) {
    for (size_t i{}; i < numservers_; ++i) {
      [[maybe_unused]] auto fut =
          std::async(std::launch::async, [&]() { sendRequestVote(i); });
    }

    size_t num_votes{};
    const size_t required_votes = numservers_ / 2 + 1;
    // gather votes before timer expiry
    {
      std::unique_lock lck{candidate_loop_mutex_};
      std::chrono::milliseconds timeout_duration{GetRandomDuration()};

      bool has_votes = timer_cv_.wait(lck, timeout_duration, [&]() {
        int n = ::recvfrom(sockfd_, buf, BUFSIZE, 0,
                           (struct sockaddr *)&peer_addr, &peer_addrlen);


        if (n > 0 && hasVoteInRequestVoteResponse(std::string_view{buf, n})) {
          ++num_votes;
        }
        if (num_votes >= required_votes) {
          return true;
        }
        return false;
      });

      if (has_votes) {
        praftservice_->state().state_ = State::Leader;
        candidate_cv_.notify_one();
        break;
      }
    }
  }
}

/*
While waiting for votes, a candidate may receive a AppendEntries RPC from another 
server claiming to be leader. If the leader’s term (included in its RPC) is at least
as large as the candidate’s current term, then the candidate recognizes the leader as 
legitimate and returns to follower state. If the term in the RPC is smaller than the 
candidate’s current term, then the candidate rejects the RPC and continues in candidate state.
*/
void Server::doCandidateListen() {
  char buf[BUFSIZE];
  while (true) {
    ssize_t n = recv(sockfd_, buf, BUFSIZE, 0);
    if (n > 0 && hasNewLeaderInResponse(std::string_view{buf, n})) {
      {
        std::lock_guard lck{candidate_loop_mutex_};
        praftservice_->state().state_ = State::Follower;
      }
      candidate_loop_cv_.notify_one();
    }
  }
}

State Server::doCandidateLoop() {
  ++praftservice_->state().current_term_;
  praftservice_->state().state_ = State::Candidate;

  std::jthread request_votes_thr{&Server::doCandidateRequestVotes, this};
  std::jthread listener_thr{&Server::doCandidateListen, this};

  std::unique_lock lck{candidate_loop_mutex_};
  candidate_loop_cv.wait(lck, [&]() {
    return praftservice_->state().state_ == State::Follower ||
     praftservice_->state().state_ == State::Leader;
  });

  return praftservice_->state().state_;
}
/* ================ END CANDIDATE ============== */

/* ================ START LEADER ============== */
/*
Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server
repeat during idle periods to prevent election timeouts
*/
State Server::doLeaderLoop() {
  while (true) {
    
    // timeout duration is fixed between a set of heartbeats 
    std::chrono::milliseconds timeout_duration{GetRandomDuration()};
    for (size_t i{}; i < numservers_; ++i) {
      [[maybe_unused]] auto fut =
          std::async(std::launch::async, [&]() { sendHeartBeat(i); });
    }

    size_t num_responses{};
    const size_t required_responses = numservers_ / 2 + 1;
    // gather votes before timer expiry
    {
      std::unique_lock lck{leader_loop_mutex_};

      const bool has_votes = leader_loop_cv_.wait(lck, timeout_duration, [&]() {
        int n = ::recvfrom(sockfd_, buf, BUFSIZE, 0,
                           (struct sockaddr *)&peer_addr, &peer_addrlen);

        if (n > 0 && praftservice_->hasHeartBeatInResponse(std::string_view{buf, n})) {
          ++num_responses;
        }
        if (num_responses >= required_votes) {
          return true;
        }
        return false;
      });

      if (has_votes) {
        praftservice_->state().state_ = State::Leader;
        continue;
      }
    }
  }
}
/* ================ END LEADER ================= */

// ================= SENDERS ================= */
void Server::sendRequestVoteResponse(size_t server_idx) {

}

void Server::sendAppendEntriesResponse(size_t server_idx) {

}

/*
To begin an election, a follower increments its current term and transitions to
candidate state. It then votes for itself and issues RequestVote RPCs in parallel
to each of the other servers in the cluster. A candidate continues in this state
until one of three things happens:
(a) it wins the election, 
(b) another server establishes itself as leader, or
(c) a period of time goes by with no winner.

A candidate wins an election if it receives votes from a majority of the servers
in the full cluster for the same term. Each server will vote for at most on candidate
in a given term, on a first-come-first-served basis.

Once a candidate wins an election, it becomes leader. It then sends heartbeat messages
to all of the other servers to establish its authority and prevent new elections.
*/
void Server::sendRequestVote(SenderInfo sender_info) {
  char host[NI_MAXHOST], service[NI_MAXSERV];
  struct sockaddr_storage peer_addr;
  socklen_t peer_addrlen = sizeof(peer_addr);

  FillPeerInfo(sender_info, peer_addr);

  const auto vote_term = praftservice_->state()->current_term_;

  flatbuffers::FlatBufferBuilder builder(1024);
  RequestVoteRPCBuilder builder{builder};
  builder.add_term(vote_term);
  builder.add_candidate_id(id_);
  auto o = builder.Finish();

  uint8_t* buf = o.GetBufferPointer();
  int size = builder.GetSize();

  if (sendto(sockfd_, buf, size, 0, (struct sockaddr*)&peer_addr, peer_addrlen) != size) {
    perror("sendto");
    fprintf(stderr, "error sending REQUEST VOTE");
  }
}

void Server::sendHeartBeat(size_t server_idx) {
  flatbuffers::FlatBufferBuilder fbb(1024);

  auto off = CreateAppendEntriesRPC(
      fbb,
      praftservice_->state().current_term,
      praftservice_->state().id_);

  uint8_t* buf = fbb.GetBufferPointer();
  int size = fbb.size();

  char host[NI_MAXHOST], service[NI_MAXSERV];
  struct sockaddr_storage peer_addr;
  socklen_t peer_addrlen = sizeof(peer_addr);

  auto si = servers_[server_idx];
  FillPeerInfo(si, peer_addr);

  if (sendto(sockfd_, buf, size, 0, (struct sockaddr*)&peer_addr, peer_addrlen) != size) {
    perror("sendto");
    fprintf(stderr, "error sending REQUEST VOTE");
  }
}