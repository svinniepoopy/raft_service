#include "server.h"
#include "raft_service.h"
#include "raft_state.h"
#include "server_info.h"

// #include "flatbuffers/include/flatbuffers//flatbuffers.h"
#include "generated/AppendEntriesReply_generated.h"
#include "generated/AppendEntries_generated.h"
#include "generated/RequestVoteReply_generated.h"
#include "generated/RequestVote_generated.h"

#include <cstring>
#include <cstdint>
#include <cstdio>
#include <cstdlib>

#include <iostream>
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
  std::istringstream istr{si};
  std::vector<ServerInfo> infos;
  for (std::string line; std::getline(istr, line, ',');) {
    auto delim_pos = line.find(':');
    std::string ip = line.substr(0, delim_pos);
    uint16_t port = std::stoi(line.substr(delim_pos));
    infos.emplace_back(std::move(ip), port);
  }
  return infos;
}
} // end anonymous namespace

Server::Server(int cluster_size, int server_idx, std::string server_info)
    : praftservice_{std::make_unique<RaftService>()},
    numservers_{cluster_size} {
  
  servers_ = ExtractServerInfo(server_info);

  std::string port{std::to_string(servers_[server_idx].port_)};

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
  int s = ::getaddrinfo(nullptr, port.c_str(), &hints, &res);
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
  ::close(sockfd_);
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
  while (true) {
    struct sockaddr_storage peer_addr;
    socklen_t peer_addrlen{sizeof(peer_addr)};

    ssize_t n{-1};
    {
      std::unique_lock lck{timer_mutex_};
      std::chrono::milliseconds timeout_duration{GetRandomDuration()};

      auto ret = timer_cv_.wait_for(lck, timeout_duration, [&]() {
        return n > 0;
      });

      if (!ret) {
        return State::Candidate;
      }
    }

    char buf[BUFSIZE];
    n = ::recvfrom(sockfd_, buf, BUFSIZE, 0,
          (struct sockaddr *)&peer_addr, &peer_addrlen);

    if (n > 0) {
      size_t len = static_cast<size_t>(n);
      std::string_view req{buf, len};     
      auto async_func = [&]() {
        SenderInfo si{.peer_addr = &peer_addr, .peer_addrlen = peer_addrlen};
        if (praftservice_->hasHeartBeatInRequest(req)) {          
          sendHeartBeatResponse(req, si);
          timer_cv_.notify_one();
        } else if (praftservice_->hasRequestVoteRequest(req)) {
          sendRequestVoteResponse(req, si);
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

int Server::ReceiveHelper(int sockfd, char* buf, size_t size, SenderInfo& si) {
  return ::recvfrom(sockfd, buf, size, 0,
                        (struct sockaddr *)&si.peer_addr, &si.peer_addrlen);             
}

/* ================ START CANDIDATE ============== */
void Server::doCandidateRequestVotes() {
  const size_t required_votes = numservers_ / 2 + 1;

  while (true) {
    for (int i{}; i < numservers_; ++i) {
      auto si = servers_[i];
      [[maybe_unused]] auto fut = std::async(std::launch::async, [&]() { sendRequestVote(si); });
    }

    size_t num_votes{};
    // gather votes before timer expiry
    bool has_votes{false};
    {
      std::unique_lock lck{candidate_loop_mutex_};
      std::chrono::milliseconds timeout_duration{GetRandomDuration()};
      has_votes = timer_cv_.wait_for(lck, timeout_duration, [&]() {
        return num_votes >= required_votes;
      });
    }

    char buf[BUFSIZE];
    struct sockaddr_storage peer_addr;
    socklen_t peer_addrlen{sizeof(peer_addr)};
    int n;
    SenderInfo si{&peer_addr, peer_addrlen};
    while ((n = ReceiveHelper(sockfd_, buf, BUFSIZE, si)) > 0) {
      auto async_func = [&]() {        
        if (praftservice_->hasVoteInRequestVoteResponse(std::string_view{buf, n})) {
          ++num_votes;
        }
      };
      [[maybe_unused]] auto req_fut = std::async(std::launch::async, async_func);
      if (num_votes >= required_votes) {
        std::lock_guard lck{candidate_loop_mutex_};
        praftservice_->state().state_ = State::Leader;
      }
      if (num_votes >= required_votes) {
        break;
      }
    }
    if (has_votes) {
      break;
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
    ssize_t n = ::recv(sockfd_, buf, BUFSIZE, 0);
    if (n > 0 && praftservice_->hasNewLeaderInResponse(std::string_view{buf, n})) {
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
  candidate_loop_cv_.wait(lck, [&]() {
    return praftservice_->state().state_ == State::Follower ||
     praftservice_->state().state_ == State::Leader;
  });

  request_votes_thr.join();
  listener_thr.join();
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
    for (int i{}; i < numservers_; ++i) {
      [[maybe_unused]] auto fut =
          std::async(std::launch::async, [&]() { sendHeartBeat(i); });
    }

    size_t num_responses{};
    const size_t required_votes = numservers_ / 2 + 1;

    bool has_heartbeats{false};
    {
      std::unique_lock lck{leader_loop_mutex_};
      std::chrono::milliseconds timeout_duration{GetRandomDuration()};
      has_heartbeats = timer_cv_.wait_for(lck, timeout_duration, [&]() {
        return num_responses >= required_votes;
      });
    }

    char buf[BUFSIZE];
    struct sockaddr_storage peer_addr;
    socklen_t peer_addrlen{sizeof(peer_addr)};
    int n;
    SenderInfo si{&peer_addr, peer_addrlen};
    while ((n = ReceiveHelper(sockfd_, buf, BUFSIZE, si)) > 0) {
      auto recv_func = [&]() {
        if (praftservice_->hasHeartBeatInResponse(std::string_view{buf, n})) {
          ++num_responses;
        }
      };
      
      auto recv_fut = std::async(std::launch::async, recv_func);
      if (num_responses >= required_votes) {
        break;
      }
    } 
    if (!has_heartbeats) {
      return State::Follower;
    }
  }
  return State::Follower;
}
/* ================ END LEADER ================= */

// ================= SENDERS ================= */
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
void Server::sendRequestVote(const ServerInfo& server_info) {
  struct hostent *he;
  if ((he = gethostbyname("localhost")) == NULL) { // get the host info
    perror("gethostbyname");
    std::exit(EXIT_FAILURE);
  }
  struct sockaddr_in their_addr; // connector's address info
  their_addr.sin_family = AF_INET;     // host byte order
  their_addr.sin_port = htons(server_info.port_); // network byte order
  their_addr.sin_addr = *((struct in_addr *)he->h_addr);
  memset(their_addr.sin_zero, '\0', sizeof their_addr.sin_zero);

  const auto vote_term = praftservice_->state().current_term_;

  flatbuffers::FlatBufferBuilder fbb(1024);
  RequestVoteRPCBuilder reply_builder{fbb};
  reply_builder.add_term(vote_term);

  const int id = praftservice_->state().id_;
  reply_builder.add_candidate_id(id);
  reply_builder.Finish();

  uint8_t* buf = fbb.GetBufferPointer();
  int size = fbb.GetSize();

  if (sendto(sockfd_, buf, size, 0, (struct sockaddr*)&their_addr, sizeof(their_addr)) != size) {
    perror("sendto");
    fprintf(stderr, "error sending REQUEST VOTE");
  }
}

void Server::sendRequestVoteResponse(std::string_view request, const SenderInfo& sender_info) {
  const RequestVoteRPC* preq = GetRequestVoteRPC(static_cast<const void*>(request.data()));
  if (!preq) {
    std::cerr << "sendRequestVoteResponse preq null\n";
  }

  flatbuffers::FlatBufferBuilder fbb{1024};
  RequestVoteRPCReplyBuilder reply_builder{fbb};

  int curr_term = praftservice_->state().current_term_;
  if (preq->term() < curr_term) {
    reply_builder.add_vote_granted(false);
  } else  if ((!praftservice_->state().voted_for_ 
      || *praftservice_->state().voted_for_ == preq->candidate_id()) &&
      preq->last_log_index() >= praftservice_->state().commit_index_) {
        reply_builder.add_vote_granted(true);
  }
  
  reply_builder.Finish();
  
  uint8_t* reply_buf = fbb.GetBufferPointer();
  int size = fbb.GetSize();
  
  int ret = ::sendto(sockfd_, reply_buf, size, 0,
    (struct sockaddr*)&sender_info.peer_addr, sender_info.peer_addrlen);
  if (ret != size) {
    std::cerr << "error sending heartbeat response\n";
  }
}

void Server::sendHeartBeat(size_t server_idx) {
  flatbuffers::FlatBufferBuilder fbb(1024);

  auto off = CreateAppendEntriesRPC(
      fbb,
      praftservice_->state().current_term_,
      praftservice_->state().id_);

  uint8_t* buf = fbb.GetBufferPointer();
  int size = fbb.GetSize();

  auto server_info = servers_[server_idx];

  struct hostent *he;
  if ((he = gethostbyname("localhost")) == NULL) { // get the host info
    perror("gethostbyname");
    std::exit(EXIT_FAILURE);
  }
  struct sockaddr_in their_addr; // connector's address info
  their_addr.sin_family = AF_INET;     // host byte order
  their_addr.sin_port = htons(server_info.port_); // network byte order
  their_addr.sin_addr = *((struct in_addr *)he->h_addr);
  memset(their_addr.sin_zero, '\0', sizeof their_addr.sin_zero);

  //FillPeerInfo(si, &peer_addr);

  if (sendto(sockfd_, buf, size, 0, (struct sockaddr*)&their_addr, sizeof(their_addr)) != size) {
    perror("sendto");
    fprintf(stderr, "error sending REQUEST VOTE");
  }
}

void Server::sendHeartBeatResponse(std::string_view request, const SenderInfo& sender_info) {
  const void* pbuf = static_cast<const void*>(request.data());  
  const AppendEntriesRPC* preq = GetAppendEntriesRPC(pbuf);

  if (!preq) {
    std::cerr << "request not of type sendHeartBeatResponse\n";
    return;
  }

  flatbuffers::FlatBufferBuilder fbb{1024};
  AppendEntriesRPCReplyBuilder reply_builder{fbb};

  int curr_term = praftservice_->state().current_term_;
  if (preq->term() < curr_term) { 
    reply_builder.add_term(curr_term);
    reply_builder.add_success(false);
  }

  reply_builder.Finish();
  
  uint8_t* reply_buf = fbb.GetBufferPointer();
  int size = fbb.GetSize();
  
  int ret = ::sendto(sockfd_, reply_buf, size, 0,
    (struct sockaddr*)sender_info.peer_addr, sender_info.peer_addrlen);
  if (ret != size) {
    std::cerr << "error sending heartbeat response\n";
  }
}