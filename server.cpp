#include "server.h"
#include "raft_state.h"
#include "server_info.h"

#include "generated/AppendEntriesReply_generated.h"
#include "generated/AppendEntries_generated.h"
#include "generated/RequestVoteReply_generated.h"
#include "generated/RequestVote_generated.h"
#include <condition_variable>
#include <mutex>
#include <sstream>

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <future>
#include <memory>
#include <random>
#include <string.h>
#include <sstream>
#include <string>
#include <string_view>
#include <stop_token>
#include <thread>

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

} // end anonymous namespace

Server::Server(int cluster_size, std::string host, std::string port,
               std::string server_info)
    : praftservice_{std::make_unique<RaftService>()} {

  servers_ = ExtractServerInfo(server_info);

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

// follower loop
// if timer expired then become candidate
// else if message(s) received within timer
// process messages from:
//   - other candidates
//   - leaders
// messages may arrive concurrently
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
        n = ::recvfrom(sockfd_, buf, BUFSIZE, 0,
          (struct sockaddr *)&peer_addr, &peer_addrlen);
        return n > 0;
      });

      if (ret == std::cv_status::timeout) {
        return State::Candidate;
      }
    }

    int s = getnameinfo((struct sockaddr *)&peer_addr, peer_addrlen, host,
                        NI_MAXHOST, service, NI_MAXSERV, NI_NUMERICSERV);

    timer_cv_.notify_one();

    SenderInfo si{.peer_addr = &peer_addr, .peer_addrlen = peer_addrlen};

    auto fut = std::async(std::launch::async,
                          [&]() { return getNextState(buf, n, si); });
    auto next_state = fut.get();
    if (next_state != State::Follower) {
      // TODO: cancel timer
      return next_state;
    }
  }
  return State::Candidate;
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

void Server::sendRequestVote(size_t server_idx) {

}

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

        if (n > 0 && hasVoteInRequestVoteReply(std::string_view{buf, n})) {
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
    if (n > 0 && hasNewLeaderInReply(std::string_view{buf, n})) {
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

/*Upon election: send initial empty AppendEntries RPCs
(heartbeat) to each server; repeat during idle periods to
prevent election timeouts*/
State Server::doLeaderLoop() {
  while (true) {
    for (size_t i{}; i < numservers_; ++i) {
      [[maybe_unused]] auto fut =
          std::async(std::launch::async, [&]() { sendHeartBeat(i); });
    }

    size_t num_responses{};
    const size_t required_responses = numservers_ / 2 + 1;
    // gather votes before timer expiry
    {
      std::unique_lock lck{leader_loop_mutex_};
      std::chrono::milliseconds timeout_duration{GetRandomDuration()};

      bool has_votes = leader_loop_cv_.wait(lck, timeout_duration, [&]() {
        int n = ::recvfrom(sockfd_, buf, BUFSIZE, 0,
                           (struct sockaddr *)&peer_addr, &peer_addrlen);

        if (n > 0 && hasHeartBeatResponse(std::string_view{buf, n})) {
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

State Server::getNextState(void *buf, size_t n, SenderInfo si) {
  uint8_t *pbufu8 = static_cast<uint8_t *>(buf);

  const AppendEntriesRPC *p_app = GetAppendEntriesRPC(pbufu8);
  if (p_app) {
    auto [nextstate, reply] =
        praftservice_->GetNextStateAppendEntries(p_app);
    sendReply(reply, si);
    return nextstate.state_;
  }
  const RequestVoteRPC *p_reqvote = GetRequestVoteRPC(pbufu8);
  auto [nextstate, reply] =
      praftservice_->GetNextStateRequestVote(p_reqvote);
  sendReply(reply, si);
  return nextstate.state_;
}
