#include "server.h"
#include "raft_service.h"
#include "raft_state.h"
#include "server_info.h"

#include "generated/AppendEntriesReply_generated.h"
#include "generated/AppendEntries_generated.h"
#include "generated/RequestVoteReply_generated.h"
#include "generated/RequestVote_generated.h"

#include <cerrno>
#include <csignal>
#include <cstring>
#include <cstdint>
#include <cstdio>
#include <cstdlib>

#include <ctime>
#include <iostream>
#include <chrono>
#include <future>
#include <memory>
#include <netinet/in.h>
#include <random>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <condition_variable>
#include <mutex>
#include <sstream>
#include <print>
#include <stop_token>
#include <queue>

#include <poll.h>
#include <netdb.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>

using namespace std::chrono_literals;

namespace {
constexpr size_t BUFSIZE = 1024;
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
    uint16_t port = std::stoi(line.substr(delim_pos+1));
    infos.emplace_back(std::move(ip), port);
  }
  return infos;
}
std::string StateToString(State state) {
  if (state == State::Follower) {
    return "Follower";
  }
  if (state == State::Candidate) {
    return "Candidate";
  }
  return "Leader";
}
} // end anonymous namespace

Server::Server(int cluster_size, int server_idx, std::string server_info)
    : praftservice_{std::make_unique<RaftService>()},
    numservers_{cluster_size} {
  
  id_ = server_idx;
  servers_ = ExtractServerInfo(server_info);
  port_ = servers_[id_].port_;

  std::string port{std::to_string(servers_[server_idx].port_)};

  // setup a UDP connection listener
  struct addrinfo hints, *res;
  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_INET;
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
    std::exit(EXIT_FAILURE);
  }

  sockfd_ = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
  if (sockfd_ == -1) {
    perror("socket");
    std::exit(EXIT_FAILURE);
  }

  // int v = fcntl(sockfd_, F_SETFL, O_NONBLOCK);
  
  // set so_reuseaddr
  int optval{1};
  int ret = setsockopt(sockfd_, SOL_SOCKET, SO_REUSEADDR, &optval,
                       sizeof(optval));
  if (ret == -1) {
    perror("setsocketopt");
    std::exit(EXIT_FAILURE);
  }

  if (bind(sockfd_, res->ai_addr, res->ai_addrlen) == -1) {
    perror("bind");
    std::exit(EXIT_FAILURE);
  }

  freeaddrinfo(res);

  // TODO install a signal handler from main 
  // listen to sigquit, sigterm

  std::println("[server@{}]: start. listen at sock={}", port, sockfd_);
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
      std::println("[server@{}]: main loop state={} ----", port_,
                   StateToString(praftservice_->state().state_));
    } else if (praftservice_->state().state_ == State::Candidate) {
      std::println("[server@{}]: main loop state={} ----", port_,
                   StateToString(praftservice_->state().state_));
      praftservice_->state().state_ = doCandidateLoop();
    } else if (praftservice_->state().state_ == State::Leader) {
      std::println("[server@{}]: main loop state={} ----", port_,
                   StateToString(praftservice_->state().state_));
      praftservice_->state().state_ = doLeaderLoop();
    }
  }
}

/* ================== FOLLOWER ============== */
State Server::doFollowerLoop() {
  std::println("[server@{}]: --- enter doFollowerLoop. term={} ----", port_,
               praftservice_->state().current_term_);
    
  bool has_response{false};

  std::jthread follower_loop_thr{[&](std::stop_token stop_token) {
    std::println("[server@{}]: enter doFollowerLoopSendAndListen", port_);

    int n;

    char buf[BUFSIZE];
    struct sockaddr_storage peer_addr;
    socklen_t peer_addrlen{sizeof(peer_addr)};

    State curr_state{State::Follower};
    while (!stop_token.stop_requested()) {
      n = ::recvfrom(sockfd_, buf, BUFSIZE, MSG_DONTWAIT, (struct sockaddr *)&peer_addr,
                     &peer_addrlen);

      if (errno == EWOULDBLOCK || errno == EAGAIN) {
        continue;
      }
      size_t len = static_cast<size_t>(n);
      std::string_view req{buf, len};

      SenderInfo si{.peer_addr = peer_addr, .peer_addrlen = peer_addrlen};
      if (praftservice_->hasHeartBeatInRequest(req)) {
        sendHeartBeatResponse(req, si);
        {
          std::lock_guard lck{timer_mutex_};
          has_response = true;
        }
      } else if (praftservice_->hasRequestVoteRequest(req)) {
        sendRequestVoteResponse(req, si);
        {
          std::lock_guard lck{timer_mutex_};
          has_response = true;
        }
      }
    }
    std::println("[server@{}]: doFollowerLoopSendAndListen - stop_requested", port_);
  }};

  std::jthread waiter_thr{[&](std::stop_token stop_token) {
    while (!stop_token.stop_requested()) {
      std::chrono::milliseconds timeout_duration{GetRandomDuration()};
      std::println("[server@{}]: doFollowerLoopWait: timeout in={}", port_,
                   timeout_duration);
      std::unique_lock lck{timer_mutex_};
      auto status = timer_cv_.wait_for(lck, timeout_duration);

      if (status == std::cv_status::timeout && !has_response) {
        praftservice_->state().state_ = State::Candidate;
        follower_loop_cv_.notify_one();
        std::println("[server@{}]: doFollowerLoopWait: timeout", port_);
        break;
      }
    }
  }};

  std::unique_lock lck{follower_mutex_};
  follower_loop_cv_.wait(lck, [&]() {
    return praftservice_->state().state_ == State::Candidate;
  });

  bool follower_stopped = follower_loop_thr.request_stop();
  if (!follower_stopped) {
    std::println("[server@{}]: Error - follower_stopped false", port_); 
  }
  bool waiter_stopped = waiter_thr.request_stop();
  if (!waiter_stopped) {
    std::println("[server@{}]: Error - waiter_stopped false", port_); 
  }

  std::println("[server@{}]: ---- finish doFollower. term={}, state={} ----",
    port_,
    praftservice_->state().current_term_,
    StateToString(praftservice_->state().state_));

  return praftservice_->state().state_;
}

/* ================ END FOLLOWER ============== */

/* ================ START CANDIDATE ============== */
/*
While waiting for votes, a candidate may receive a AppendEntries RPC from another 
server claiming to be leader. If the leader’s term (included in its RPC) is at least
as large as the candidate’s current term, then the candidate recognizes the leader as 
legitimate and returns to follower state. If the term in the RPC is smaller than the 
candidate’s current term, then the candidate rejects the RPC and continues in candidate state.
*/
State Server::doCandidateLoop() {
  ++praftservice_->state().current_term_;
  praftservice_->state().voted_in_current_term = false;
  praftservice_->state().state_ = State::Candidate;

  std::println("[server@{}]: ---- enter doCandidateLoop. term={} ----",
    port_,
    praftservice_->state().current_term_);

  struct Message {
    std::string msg;
    SenderInfo si;
  };

  std::queue<Message> message_q;

  std::mutex messages_mutex;
  std::condition_variable_any messages_cv;

  std::jthread message_receiver_thr{[&](std::stop_token stop_token) {
      SenderInfo recv_si{};
      recv_si.peer_addrlen = sizeof(struct sockaddr_storage);
      
      struct pollfd* pfds;
      pfds = calloc(1, sizeof(struct pollfd));
      pfds[0].fd = sockfd_;
      pfds[0].events = POLL_IN;

      int ready;
      while (!stop_token.stop_requested()) {

        read = poll(pfds, 1, 100);

        if (pdfs[0].revents != 0) {

          if (pfds[0].revents & POLLIN) {
            n = ::recvfrom(sockfd_, buf, BUFSIZE, 0,
                           (struct sockaddr *)&recv_si.peer_addr,
                           &recv_si.peer_addrlen);

            if (n > 0) {
              message_q.push(std::string{buf, n},
                             {recv_si.peer_addr, recv_si.peer_addrlen});
            }
          }
        }
      }
  }};

  const size_t required_votes = numservers_/ 2 + 1;

  std::jthread message_processing_thr{[&](std::stop_token stop_token) {
    std::println("[server@{}]: start RequestVote. term={}", port_,
                 praftservice_->state().current_term_);

    while (!stop_token.stop_requested()) {
      {
        std::unique_lock lck{messages_mutex};
        auto pred = [&]() {
          return !message_q.empty();
        };

        if (!messages_cv.wait(lck, stop_token, pred)) { 
          return;
        }

        auto recv_data = message_q.front();
        message_q.pop(); 

        std::string_view buf{recv_data.msg};
        if (praftservice_->hasHigherTerm(buf)) {
          {
            std::println("[server@{}]: RequestVotes complete. found higher "
                         "term. curr_term={}",
                         port_, praftservice_->state().current_term_);
            std::lock_guard lck{candidate_loop_mutex_};
            praftservice_->state().state_ = State::Follower;
            curr_state = State::Follower;
          }
          candidate_loop_cv_.notify_one();
          break;
        } else if (praftservice_->hasRequestVoteReply(buf) &&
                   praftservice_->hasVoteInRequestVoteResponse(buf)) {
          ++num_votes;
          if (num_votes >= required_votes) {
            std::println("[server@{}]: RequestVotes complete. change state to "
                         "Leader. term={}",
                         port_, praftservice_->state().current_term_);
            {
              std::lock_guard lck{candidate_loop_mutex_};
              praftservice_->state().state_ = State::Leader;
              curr_state = State::Leader;
            }
            candidate_loop_cv_.notify_one();
            break;
          }
        } else if (praftservice_->hasRequestVoteRequest(buf)) {
          std::println("[server@{}]: respond to requestVote", port_);
          sendRequestVoteResponse(buf, recv_data.si);
        } else if (praftservice_->hasHeartBeatInRequest(buf)) {
          {
            std::lock_guard lck{candidate_loop_mutex_};
            praftservice_->state().state_ = State::Follower;
            curr_state = State::Follower;
          }
          candidate_loop_cv_.notify_one();
          break;
        }
      }
    }
  }};
   
  std::cv_status status{std::cv_status::no_timeout};
  while (true) {
    for (int i{}; i < numservers_; ++i) {
      if (i == id_) {
        continue;
      }
      auto si = servers_[i];
      sendRequestVote(servers_[i]);
    }

    std::chrono::milliseconds timeout_duration{GetRandomDuration()};
    std::println("[server@{}]: doCandidateRequestVote: timeout in={}", port_,
                 timeout_duration);
    std::unique_lock lck{candidate_loop_mutex_};
    status = timer_cv_.wait_for(lck, timeout_duration);

    if (curr_state == State::Follower || curr_state == State::Leader) {
      lck.unlock();
      message_receiver_thr.request_stop();
      message_processing_thr.request_stop();
      break;
    } else {
      std::println("[server@{}]: start doCandidateRequestVote: timeout - start "
                   "new election term",
                   port_);
    }
  }

  std::println("[server@{}]: ---- finish doCandidateLoop. term={}, state={} ----",
    port_,
    praftservice_->state().current_term_,
    StateToString(praftservice_->state().state_));

  return praftservice_->state().state_;
}
/* ================ END CANDIDATE ============== */

/* ================ START LEADER ============== */
/*
Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server
repeat during idle periods to prevent election timeouts
*/
State Server::doLeaderLoop() {
  std::println("[server@{}]: ---- enter doLeaderLoop. term={} ----",
    port_,
    praftservice_->state().current_term_);

  State curr_state{State::Leader};

  const size_t required_votes = (numservers_ - 1) / 2 + 1;
  while (curr_state == State::Leader) {    
    // timeout duration is fixed between a set of heartbeats 
    std::chrono::milliseconds timeout_duration{GetRandomDuration()};
    for (int i{}; i < numservers_; ++i) {
      if (i == id_) {
        continue;
      }
      [[maybe_unused]] auto fut =
          std::async(std::launch::async, [&]() { sendHeartBeat(i); });
    }

    size_t num_responses{};

    auto async_func = [&]() {
      char buf[BUFSIZE];
      int n;
      for (;;) { 
        SenderInfo recv_si{};
        recv_si.peer_addrlen = sizeof(struct sockaddr_storage);
        n = ::recvfrom(sockfd_, buf, BUFSIZE, 0,
                       (struct sockaddr *)&recv_si.peer_addr,
                       &recv_si.peer_addrlen);

        if (praftservice_->hasHigherTerm({buf, n})) {
          std::println("[server@{}]: RequestVotes complete. found higher term. "
                       "curr_term={}",
                       port_, praftservice_->state().current_term_);
          curr_state = State::Follower;
          break;
        }
        if (praftservice_->hasHeartBeatInResponse(std::string_view{buf, n})) {
          ++num_responses;
        }

        if (num_responses >= required_votes) {
          std::println("[server@{}]: ---- Leader: QUORUM established. term={} ---- ", port_,
                       praftservice_->state().current_term_);
                       timer_cv_.notify_one();
          break;
        }
      }
    };

    auto fut = std::async(std::launch::async, async_func); 

    if (curr_state != State::Leader) {
      break;
    }

    bool has_heartbeats{false};
    {
      std::chrono::milliseconds timeout_duration{GetRandomDuration()};
      std::unique_lock lck{leader_loop_mutex_};
      has_heartbeats = timer_cv_.wait_for(lck, timeout_duration, [&]() {
        return num_responses >= required_votes;
      });
    }

    if (!has_heartbeats) {
      std::println(
          "[server@{}]: ---- Leader: !!LOST QUORUM!!. Change to Follower term={} ----",
          port_, praftservice_->state().current_term_);

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
  std::println("[server@{}]: sendRequestVote to={}. term={}",
    port_,
    server_info.port_,
    praftservice_->state().current_term_);

  struct sockaddr_in their_addr; // connector's address info
  memset(&their_addr, 0, sizeof(their_addr));
  their_addr.sin_family = AF_INET;     // host byte order
  their_addr.sin_port = htons(server_info.port_); // network byte order
  their_addr.sin_addr.s_addr = htonl(INADDR_ANY); 

  const auto vote_term = praftservice_->state().current_term_;
  const int candidate_id = praftservice_->state().id_;

  flatbuffers::FlatBufferBuilder fbb(1024);
  auto o = CreateRequestVoteRPC(fbb, vote_term, candidate_id);
  fbb.Finish(o);

  uint8_t* buf = fbb.GetBufferPointer();
  int size = fbb.GetSize();

  if (sendto(sockfd_, buf, size, 0, (struct sockaddr*)&their_addr, sizeof(their_addr)) != size) {
    perror("sendto");
    fprintf(stderr, "error sending REQUEST VOTE");
  }
}

// reply false if term < currentTerm
// If votedFor is null or candidateId, and candidate’s log is at
//    least as up-to-date as receiver’s log, grant vote
// at most one vote in a given term
void Server::sendRequestVoteResponse(std::string_view request, const SenderInfo& sender_info) {
  std::println("[server@{}]: sendRequestVoteResponse term={}",
    port_,
    praftservice_->state().current_term_);

  if (praftservice_->state().voted_in_current_term) {
    std::println("[server@{}]: sendRequestVoteResponse term={} -- not voting in current term. already voted", port_,
                 praftservice_->state().current_term_);
    return;
  }

  const RequestVoteRPC* preq = GetRequestVoteRPC(static_cast<const void*>(request.data()));
  if (!preq) {
    std::cerr << "sendRequestVoteResponse preq null\n";
    return;
  }

  const int term = preq->term();
  const int curr_term = praftservice_->state().current_term_;

  auto last_voted_for = praftservice_->state().voted_for_;

  bool vote_granted{false};

  if (term >= curr_term && 
      (!last_voted_for || 
      (*last_voted_for == preq->candidate_id())) &&
      preq->last_log_index() >= praftservice_->state().commit_index_) {

    std::println("[server@{}]: grating vote to={}, term={}",
      port_,
      term,
      preq->candidate_id());

    praftservice_->state().voted_for_ = preq->candidate_id();
    vote_granted = true;
    praftservice_->state().voted_in_current_term = true;
  }
  
  flatbuffers::FlatBufferBuilder fbb{1024};
  auto o = CreateRequestVoteRPCReply(fbb, curr_term, vote_granted);
  fbb.Finish(o);
  
  uint8_t* reply_buf = fbb.GetBufferPointer();
  int size = fbb.GetSize();
  
  int ret = ::sendto(sockfd_, reply_buf, size, 0,
    (struct sockaddr*)&sender_info.peer_addr, sender_info.peer_addrlen);
  if (ret != size) {
    perror("sendto");
    std::cerr << "error sending requestvote response\n";
  }
}

void Server::sendHeartBeat(size_t server_idx) {
  auto server_info = servers_[server_idx];
  std::println("[server@{}]: sendHeartBeat to={}. term={}",
    port_,
    server_info.port_,
    praftservice_->state().current_term_);

  flatbuffers::FlatBufferBuilder fbb(1024);
  auto off = CreateAppendEntriesRPC(
      fbb,
      praftservice_->state().current_term_,
      praftservice_->state().id_);
  fbb.Finish(off);

  uint8_t* buf = fbb.GetBufferPointer();
  int size = fbb.GetSize();
  
  struct sockaddr_in their_addr; // connector's address info
  memset(&their_addr, 0, sizeof(their_addr));
  their_addr.sin_family = AF_INET;     // host byte order
  their_addr.sin_port = htons(server_info.port_); // network byte order
  their_addr.sin_addr.s_addr = htonl(INADDR_ANY); // network byte order 

  if (sendto(sockfd_, buf, size, 0, (struct sockaddr*)&their_addr, sizeof(their_addr)) != size) {
    perror("sendto");
    fprintf(stderr, "error sending REQUEST VOTE");
  }
}

void Server::sendHeartBeatResponse(std::string_view request, const SenderInfo& sender_info) {

  std::println("[server@{}]: sendHeartBeatResponse term={}",
    port_,
    praftservice_->state().current_term_);

  const void* pbuf = static_cast<const void*>(request.data());  
  const AppendEntriesRPC* preq = GetAppendEntriesRPC(pbuf);

  if (!preq) {
    std::cerr << "request not of type sendHeartBeatResponse\n";
    return;
  }

  bool success{true};
  int curr_term = praftservice_->state().current_term_;
  if (preq->term() < curr_term) { 
    success = false;
  }

  flatbuffers::FlatBufferBuilder fbb{1024};
  auto o = CreateAppendEntriesRPCReply(fbb, curr_term, success);
  fbb.Finish(o);
  
  uint8_t* reply_buf = fbb.GetBufferPointer();
  int size = fbb.GetSize();
  
  int ret = ::sendto(sockfd_, reply_buf, size, 0,
    (struct sockaddr*)&sender_info.peer_addr, sender_info.peer_addrlen);
  if (ret != size) {
    std::cerr << "error sending heartbeat response\n";
  }
}

void termination_handler(int signal) {
  std::exit(EXIT_FAILURE);
}

int main(int argc, char** argv) {

  if (signal(SIGINT, termination_handler) == SIG_ERR) {
    perror("signal");
    std::exit(EXIT_FAILURE);
  }
  if (signal(SIGTERM, termination_handler) == SIG_ERR) {
    perror("signal");
    std::exit(EXIT_FAILURE);
  }

  int num_servers{std::stoi(std::string{argv[1]})};

  int idx{std::stoi(argv[2])};

  std::string serverinfo{argv[3]};


  Server s{num_servers, idx, serverinfo};
}