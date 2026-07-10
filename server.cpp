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
#include <cstddef>
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
  
  praftservice_->state().id_ = server_idx;

  servers_ = ExtractServerInfo(server_info);
  port_ = servers_[praftservice_->state().id_].port_;

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
    std::cerr << "exit getaddrinfo\n";
    std::exit(EXIT_FAILURE);
  }

  sockfd_ = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
  if (sockfd_ == -1) {
    perror("socket");
    std::cerr << "exit socket\n";
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

  startCommandListener();

  std::println("[server@{}]: start. listen at sock={}", port, sockfd_);
  start();
}

Server::~Server() {
  ::close(sockfd_);
}

void Server::startCommandListener() {

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
      praftservice_->state().state_ = doCandidateLoop();
      std::println("[server@{}]: main loop state={} ----", port_,
                   StateToString(praftservice_->state().state_));
    } else if (praftservice_->state().state_ == State::Leader) {
      praftservice_->state().state_ = doLeaderLoop();
      std::println("[server@{}]: main loop state={} ----", port_,
                   StateToString(praftservice_->state().state_));
    }
  }
}

/* ================== FOLLOWER ============== */
State Server::doFollowerLoop() {
  std::println("[server@{}]: --- enter doFollowerLoop. term={} ----", port_,
               praftservice_->state().current_term_);

  praftservice_->state().voted_in_current_term_ = false;

  std::deque<Message> message_q;

  std::mutex messages_mutex;
  std::condition_variable_any messages_cv;

  int n;

  std::jthread message_receiver_thr{[&](std::stop_token stop_token) {
    std::println("[server@{}]: ---- start follower message_receiver thr",
                 port_);
    struct pollfd *pfds;
    pfds = static_cast<struct pollfd *>(calloc(1, sizeof(struct pollfd)));
    pfds[0].fd = sockfd_;
    pfds[0].events = POLL_IN;

    SenderInfo recv_si{};
    recv_si.peer_addrlen = sizeof(struct sockaddr_storage);
    int ready;
    while (!stop_token.stop_requested()) {
      ready = poll(pfds, 1, 100);
      if (ready == -1) {
        perror("poll");
        std::cerr << "exit poll\n";
        std::exit(EXIT_FAILURE);
      }
      if (pfds[0].revents != 0) {
        char buf[BUFSIZE];
        if (pfds[0].revents & POLLIN) {
          n = ::recvfrom(sockfd_, buf, BUFSIZE, 0,
                         (struct sockaddr *)&recv_si.peer_addr,
                         &recv_si.peer_addrlen);
          if (n > 0) {
            {
              std::lock_guard lck{messages_mutex};
              message_q.push_back({std::string{std::string_view{buf, n}},
                                   {recv_si.peer_addr, recv_si.peer_addrlen}});
            }
            messages_cv.notify_one();
          }
        }
      }
    }
  }};

  bool has_response{false};
  std::jthread message_processing_thr{[&](std::stop_token stop_token) {
    std::println("[server@{}]: start follower message_processing_thr", port_);
    while (!stop_token.stop_requested()) {

      std::unique_lock lck{messages_mutex};
      auto pred = [&]() { return !message_q.empty(); };
      if (!messages_cv.wait(lck, stop_token, pred)) {
        return;
      }
      auto recv_data = message_q.front();
      message_q.pop_front();
      lck.unlock();

      std::string_view buf{recv_data.msg};
      if (praftservice_->hasHeartBeatInRequest(buf)) {
        sendHeartBeatResponse(buf, recv_data.si);
        std::println("[server@{}]: send heartbeat", port_);
        {
          std::lock_guard lck{timer_mutex_};
          has_response = true;
        }
      } else if (praftservice_->hasRequestVoteRequest(buf)) {
        bool sent_resp = sendRequestVoteResponse(buf, recv_data.si);
        if (!sent_resp) {
          continue;
        }
        std::println("[server@{}]: send requestVoteReply", port_);
        {
          std::lock_guard lck{timer_mutex_};
          has_response = true;
        }
      }
    }
  }};

  std::condition_variable_any timer_cv;
  std::chrono::milliseconds timeout_duration{GetRandomDuration()};
  std::println("[server@{}]: doFollowerLoopWait: timeout in={}", port_,
               timeout_duration);
  while (true) {
    auto cv_status = std::cv_status::no_timeout;
    {
      std::unique_lock lck{timer_mutex_};
      cv_status = timer_cv.wait_for(lck, timeout_duration);
    }
    if (!has_response) {
      praftservice_->state().state_ = State::Candidate;
      messages_cv.notify_all();
      break;
    }
  }
  std::println("[server@{}]: ---- finish doFollower. term={}, state={} ----",
    port_,
    praftservice_->state().current_term_,
    StateToString(praftservice_->state().state_));


  return State::Candidate;
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
  praftservice_->state().state_ = State::Candidate;

  std::println("[server@{}]: ---- enter doCandidateLoop. term={} ----",
    port_,
    praftservice_->state().current_term_);

  std::deque<Message> message_q;

  std::mutex messages_mutex;
  std::condition_variable_any messages_cv;

  int n;
  std::jthread message_receiver_thr{[&](std::stop_token stop_token) {
    std::println("[server@{}]: ---- start candidate message_receiver thr", port_); 
      struct pollfd* pfds;
      pfds = static_cast<struct pollfd*>(calloc(1, sizeof(struct pollfd)));
      pfds[0].fd = sockfd_;
      pfds[0].events = POLL_IN;

      char buf[BUFSIZE];
      SenderInfo recv_si{};
      recv_si.peer_addrlen = sizeof(struct sockaddr_storage);
      int ready;
      while (!stop_token.stop_requested()) {

        ready = poll(pfds, 1, 100);
        if (ready == -1) {
          perror("poll");
          std::cerr << "exit poll\n";
          std::exit(EXIT_FAILURE);
        }
        if (pfds[0].revents != 0) {

          if (pfds[0].revents & POLLIN) {
            n = ::recvfrom(sockfd_, buf, BUFSIZE, 0,
                           (struct sockaddr *)&recv_si.peer_addr,
                           &recv_si.peer_addrlen);
            if (n > 0) {
              std::lock_guard lck{messages_mutex};
              message_q.push_back({std::string{std::string_view{buf, n}},
                             {recv_si.peer_addr, recv_si.peer_addrlen}});
              messages_cv.notify_one();
            }
          }
        }
      }
  }};

  const size_t required_votes = numservers_/ 2 + 1;
  int num_votes{1};
  std::jthread message_processing_thr{[&](std::stop_token stop_token) {
    std::println("[server@{}]: start message_processing_thr", port_);
    while (!stop_token.stop_requested()) {

      std::unique_lock lck{messages_mutex};
      auto pred = [&]() { return !message_q.empty(); };
      if (!messages_cv.wait(lck, stop_token, pred)) {
        return;
      }
      auto recv_data = message_q.front();
      message_q.pop_front();
      lck.unlock();

      std::string_view buf{recv_data.msg};

      if (praftservice_->hasHigherTerm(buf).first) {
        {
          const int higher_term = praftservice_->hasHigherTerm(buf).second;
          std::println("[server@{}]: CandidateLoop complete. found higher "
                       "term. curr_term={}, higher term={}",
                       port_, praftservice_->state().current_term_,
                       higher_term);
          std::lock_guard lck{candidate_loop_mutex_};
          praftservice_->state().state_ = State::Follower;
          praftservice_->state().current_term_ = higher_term; 
        }
        candidate_loop_cv_.notify_one();
        break;
      } else if (praftservice_->hasRequestVoteReply(buf) &&
                 praftservice_->hasVoteInRequestVoteResponse(buf)) {
        ++num_votes;
        std::println("[server@{}]: message_processing_thr num_votes={}, "
                     "required_votes={}",
                     port_, num_votes, required_votes);
        if (num_votes >= required_votes) {
          std::println("[server@{}]: CandidateLoop complete. change state to "
                       "Leader. term={}",
                       port_, praftservice_->state().current_term_);
          {
            std::lock_guard lck{candidate_loop_mutex_};
            praftservice_->state().state_ = State::Leader;
          }
          candidate_loop_cv_.notify_one();
          break;
        }
      } else if (praftservice_->hasRequestVoteRequest(buf)) {
        std::println("[server@{}]: respond to requestVote", port_);
        sendRequestVoteResponse(buf, recv_data.si);
      } else if (praftservice_->hasHeartBeatInRequest(buf)) {
           std::println("[server@{}]: got HeartBeat. CandidateLoop complete. change state to "
                       "Follower. term={}",
                       port_, praftservice_->state().current_term_);
        sendHeartBeatResponse(buf, recv_data.si);
        {
          std::lock_guard lck{candidate_loop_mutex_};
          praftservice_->state().state_ = State::Follower;
        }
        candidate_loop_cv_.notify_one();
        break;
      }
    }
  }};
   
  std::cv_status status{std::cv_status::no_timeout};
  while (true) {
    for (int i{}; i < numservers_; ++i) {
      if (i == praftservice_->state().id_) {
        continue;
      }
      sendRequestVote(servers_[i]);
    }

    std::chrono::milliseconds timeout_duration{GetRandomDuration()}; 
    std::unique_lock lck{candidate_loop_mutex_};
    status = timer_cv_.wait_for(lck, timeout_duration);

    if (praftservice_->state().state_ == State::Follower ||
        praftservice_->state().state_ == State::Leader) {
      message_receiver_thr.request_stop();
      message_processing_thr.request_stop();
      break;
    } else {
      {
        std::lock_guard lck{messages_mutex};
        message_q.clear();
        num_votes = 1;
        praftservice_->state().voted_in_current_term_ = false;
      }
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
State Server::doLeaderLoop() {
  std::println("[server@{}]: ---- enter doLeaderLoop. term={} ----",
    port_,
    praftservice_->state().current_term_);

  auto start = std::chrono::steady_clock::now();
  std::chrono::duration<double> total{}; 

  State curr_state{State::Leader};

  std::deque<Message> message_q;

  std::mutex messages_mutex;
  std::condition_variable_any messages_cv;

  int n;
  std::jthread message_receiver_thr{[&](std::stop_token stop_token) {
    std::println("[server@{}]: ---- start leader messsage_receiver_thr", port_); 
      struct pollfd* pfds;
      pfds = static_cast<struct pollfd*>(calloc(1, sizeof(struct pollfd)));
      pfds[0].fd = sockfd_;
      pfds[0].events = POLL_IN;

      char buf[BUFSIZE];
      SenderInfo recv_si{};
      recv_si.peer_addrlen = sizeof(struct sockaddr_storage);
      int ready;
      while (!stop_token.stop_requested()) {

        ready = poll(pfds, 1, 100);
        if (ready == -1) {
          perror("poll");
          std::exit(EXIT_FAILURE);
        }
        if (pfds[0].revents != 0) {

          if (pfds[0].revents & POLLIN) {
            n = ::recvfrom(sockfd_, buf, BUFSIZE, 0,
                           (struct sockaddr *)&recv_si.peer_addr,
                           &recv_si.peer_addrlen);
            if (n > 0) {
              std::lock_guard lck{messages_mutex};
              message_q.push_back({std::string{std::string_view{buf, n}},
                             {recv_si.peer_addr, recv_si.peer_addrlen}});
              messages_cv.notify_one();
            }
          }
        }
      }
  }};

  const size_t required_votes = (numservers_ - 1) / 2 + 1;
  size_t num_responses{0};
  std::jthread message_processing_thr{[&](std::stop_token stop_token) {
    while (!stop_token.stop_requested()) {

      std::unique_lock lck{messages_mutex};
      auto pred = [&]() { return !message_q.empty(); };
      if (!messages_cv.wait(lck, stop_token, pred)) {
        return;
      }
      auto recv_data = message_q.front();
      message_q.pop_front();
      lck.unlock();

      std::string_view buf{recv_data.msg};

      if (praftservice_->hasHigherTerm(buf).first) {
        std::println("[server@{}]: RequestVotes complete. found higher term. "
                     "curr_term={}",
                     port_, praftservice_->state().current_term_);
        {
          std::lock_guard lck{leader_loop_mutex_};
          praftservice_->state().state_ = State::Follower;
          praftservice_->state().current_term_ = praftservice_->hasHigherTerm(buf).second;
        }
        timer_cv_.notify_one();
        break;
      }
      if (praftservice_->hasHeartBeatInResponse(buf)) {

        std::println("[server@{}]: Leader got heartBeatResponse. "
                     "curr_term={}",
                     port_, praftservice_->state().current_term_);

        std::lock_guard lck{leader_loop_mutex_};
        ++num_responses;
      }
      if (num_responses >= required_votes) {
        const auto end = std::chrono::steady_clock::now();
        const std::chrono::duration<double> diff = end - start;
        total += diff;
        start = end;
        std::println("[server@{}]: ---- Leader: QUORUM established. "
                     "term={} Leader since {} diff={}----",
                     port_, praftservice_->state().current_term_, total, diff);
      }
    }
  }};

  bool has_heartbeats{false};

  int num_attempts{0};
  while (true) { 
    for (int i{}; i < numservers_; ++i) {
      if (i == praftservice_->state().id_) {
        continue;
      }
      sendHeartBeat(i);
    }

    std::chrono::milliseconds timeout_duration{GetRandomDuration()};
    std::unique_lock lck{leader_loop_mutex_};
    has_heartbeats = timer_cv_.wait_for(lck, timeout_duration, [&]() {
      return num_responses >= required_votes;
    });

    lck.unlock();
    if (!has_heartbeats) {
      ++num_attempts;
      if (num_attempts >= 3) {
        std::println("[server@{}]: ---- Leader: !!LOST QUORUM!!. Change to "
                     "Follower term={} ----",
                     port_, praftservice_->state().current_term_);
        break;
      }
    } else {
      num_responses = 0;
      num_attempts = 0;
    }
  }
  std::println("[server@{}]: Leader state for={}", port_, total); 

  return State::Follower;
}
/* ================ END LEADER ================= */

// ================= SENDERS ================= */
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
bool Server::sendRequestVoteResponse(std::string_view request, const SenderInfo& sender_info) {
  std::println("[server@{}]: sendRequestVoteResponse term={}",
    port_,
    praftservice_->state().current_term_);
  if (praftservice_->state().voted_in_current_term_) {
    std::println("[server@{}]: sendRequestVoteResponse term={} -- not voting in current term. already voted", port_,
                 praftservice_->state().current_term_);
    return false;
  }

  const RequestVoteRPC* preq = GetRequestVoteRPC(static_cast<const void*>(request.data()));
  if (!preq) {
    std::cerr << "sendRequestVoteResponse preq null\n";
    return false;
  }

  const int term = preq->term();
  const int curr_term = praftservice_->state().current_term_;

  auto last_voted_for = praftservice_->state().voted_for_;

  bool vote_granted{false};

  if (term >= curr_term && (preq->last_log_index() >= praftservice_->state().commit_index_)) {
    praftservice_->state().voted_for_ = preq->candidate_id();
    vote_granted = true;
    praftservice_->state().voted_in_current_term_ = true;
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

  std::println("[server@{}]: vote_granted={}, to={} for term={}",
              port_,
              vote_granted,
              preq->candidate_id(),
              term);

  return vote_granted;
}

void Server::sendHeartBeat(size_t server_idx) {
  auto server_info = servers_[server_idx];
 
  /*
  std::println("[server@{}]: sendHeartBeat to={}. term={}",
    port_,
    server_info.port_,
    praftservice_->state().current_term_);
  */
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
  /*
  std::println("[server@{}]: sendHeartBeatResponse term={}",
    port_,
    praftservice_->state().current_term_);
   */
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

  praftservice_->state().current_term_ = curr_term;

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

int proc_numeric_id{};

void termination_handler(int signal) {
  std::cerr << "terminate " << proc_numeric_id << std::endl;
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
  proc_numeric_id = idx;

  std::string serverinfo{argv[3]};


  Server s{num_servers, idx, serverinfo};
}
