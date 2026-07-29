#include "server.h"

#include "command.h"
#include "raft_service.h"
#include "raft_state.h"
#include "server_info.h"

#include "generated/AppendEntriesReply_generated.h"
#include "generated/AppendEntries_generated.h"
#include "generated/CommandPut_generated.h"
#include "generated/CommandResponse_generated.h"
#include "generated/RequestVoteReply_generated.h"
#include "generated/RequestVote_generated.h"

#include <cerrno>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>

#include <chrono>
#include <condition_variable>
#include <ctime>
#include <deque>
#include <ios>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <print>
#include <queue>
#include <random>
#include <sstream>
#include <stop_token>
#include <string>
#include <string_view>
#include <thread>

#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <poll.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

using namespace std::chrono_literals;

namespace {
constexpr size_t BUFSIZE = 1024;
std::uniform_int_distribution<> dist(150, 300);
std::random_device rd{};
std::mt19937 gen(rd());
int GetRandomDuration() { return dist(gen); }

std::vector<ServerInfo> ExtractServerInfo(const std::string &si) {
  std::istringstream istr{si};
  std::vector<ServerInfo> infos;
  for (std::string line; std::getline(istr, line, ',');) {
    auto delim_pos = line.find(':');
    std::string ip = line.substr(0, delim_pos);
    uint16_t port = std::stoi(line.substr(delim_pos + 1));
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
    : numservers_{cluster_size}, praftservice_{std::make_unique<RaftService>()},
      pcommand_{std::make_unique<Command>()} {

  praftservice_->state().id_ = server_idx;

  praftservice_->state().commit_log_.push_back({0, {}});

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

  // set so_reuseaddr
  int optval{1};
  int ret =
      setsockopt(sockfd_, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval));
  if (ret == -1) {
    perror("setsocketopt");
    std::exit(EXIT_FAILURE);
  }

  if (bind(sockfd_, res->ai_addr, res->ai_addrlen) == -1) {
    perror("bind");
    std::exit(EXIT_FAILURE);
  }

  freeaddrinfo(res);

  std::println("[server@{}]: start id={}. listen at sock={}", port, server_idx, sockfd_);
  start();
}

Server::~Server() { ::close(sockfd_); }

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
    std::cout << std::flush;
  }
}

void Server::startFollowerLogConsistencyThread(const SenderInfo &sender_info) {}

/* ================== FOLLOWER ============== */
State Server::doFollowerLoop() {
  std::println("[server@{}]: --- enter doFollowerLoop. term={} ----", port_,
               praftservice_->state().current_term_);

  std::deque<Message> message_q;

  std::mutex messages_mutex;
  std::condition_variable_any messages_cv;

  int n;
  std::jthread message_receiver_thr_{[&](std::stop_token stop_token) {
    std::println("[server@{}]: start follower message_receiver_thr", port_);

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
    free(pfds);
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

      if (pcommand_->hasPut(buf)) {
        sendLeaderRedirect(recv_data.si);
      } else if (praftservice_->hasAppendEntriesRequest(buf)) {
        sendAppendEntriesResponse(buf, recv_data.si);
        {
          std::lock_guard lck{timer_mutex_};
          has_response = true;
        }
      } else if (praftservice_->hasHeartBeatInRequest(buf)) {
        sendHeartBeatResponse(buf, recv_data.si);
        {
          std::lock_guard lck{timer_mutex_};
          has_response = true;
        }
      } else if (praftservice_->hasRequestVoteRequest(buf)) {
        [[maybe_unused]] bool sent_resp =
            sendRequestVoteResponse(buf, recv_data.si);
        {
          std::lock_guard lck{timer_mutex_};
          has_response = true;
        }
      }
      //std::cout << std::endl;
    }
  }};

  std::condition_variable_any timer_cv;
  std::chrono::milliseconds timeout_duration{GetRandomDuration()};
  std::println("[server@{}]: doFollowerLoopWait: timeout in={}", port_,
               timeout_duration);
  while (true) {
    [[maybe_unused]] auto cv_status = std::cv_status::no_timeout;
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
               port_, praftservice_->state().current_term_,
               StateToString(praftservice_->state().state_));
  std::cout << std::flush;

  return State::Candidate;
}
/* ================ END FOLLOWER ============== */

/* ================ START CANDIDATE ============== */
State Server::doCandidateLoop() {
  ++praftservice_->state().current_term_;
  praftservice_->state().state_ = State::Candidate;

  //praftservice_->state().voted_for_.reset(); // TODO: remove

  std::println("[server@{}]: ---- enter doCandidateLoop. term={} ----", port_,
               praftservice_->state().current_term_);

  std::deque<Message> message_q;

  std::mutex messages_mutex;
  std::condition_variable_any messages_cv;

  int n;

  std::jthread message_receiver_thr_{[&](std::stop_token stop_token) {
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
            std::lock_guard lck{messages_mutex};
            message_q.push_back({std::string{std::string_view{buf, n}},
                                 {recv_si.peer_addr, recv_si.peer_addrlen}});
            messages_cv.notify_one();
          }
        }
      }
    }
    free(pfds);
  }};

  const size_t required_votes = numservers_ / 2 + 1;
  size_t num_votes{1};
  std::jthread message_processing_thr{[&](std::stop_token stop_token) {
    std::println("[server@{}]: start candidate message_processing_thr", port_);
    while (!stop_token.stop_requested()) {

      Message recv_data;
      std::unique_lock lck{messages_mutex};
      auto pred = [&]() { return !message_q.empty(); };
      if (!messages_cv.wait(lck, stop_token, pred)) {
        return;
      }

      if (!message_q.empty()) {
        recv_data = message_q.front();
        message_q.pop_front();
      }
      lck.unlock();

      if (message_q.empty()) {
        continue;
      }

      std::string_view buf{recv_data.msg};
      if (buf.empty()) {
        continue;
      }

      if (praftservice_->hasHigherTerm(buf).first) {
        const int higher_term = praftservice_->hasHigherTerm(buf).second;
        std::println("[server@{}]: CandidateLoop complete. found higher term",
                     port_);
        {
          std::lock_guard lck{timer_mutex_};
          praftservice_->state().state_ = State::Follower;
          praftservice_->state().current_term_ = higher_term;
        }
        timer_cv_.notify_one();
        break;
      } else if (praftservice_->hasRequestVoteReply(buf) &&
                 praftservice_->hasVoteInRequestVoteResponse(buf)) {
        std::println("[server@{}]: doCandidateLoop got vote in RequestVoteReply num_votes={}", port_, num_votes+1);
        std::lock_guard lck{timer_mutex_};
        ++num_votes;
        if (num_votes >= required_votes) {
          praftservice_->state().state_ = State::Leader;
          break;
        }
      } else if (praftservice_->hasRequestVoteRequest(buf)) {
        std::println("[server@{}]: doCandidateLoop do sendRequestVoteResponse", port_); 
        bool vote_granted = sendRequestVoteResponse(buf, recv_data.si);
        std::cerr << port_ << " granted vote " << vote_granted << std::endl;
      } else if (praftservice_->hasHeartBeatInRequest(buf)) {
        std::println("[server@{}]: got HeartBeat. CandidateLoop complete. "
                     "change state to "
                     "Follower. term={}",
                     port_, praftservice_->state().current_term_);
        sendHeartBeatResponse(buf, recv_data.si);
        {
          std::lock_guard lck{timer_mutex_};
          praftservice_->state().state_ = State::Follower;
        }
        timer_cv_.notify_one();
        break;
      }
    }
  }};

  [[maybe_unused]] std::cv_status status{std::cv_status::no_timeout};
  while (true) {
    // vote for self
    praftservice_->state().voted_for_ = praftservice_->state().id_;
    for (int i{}; i < numservers_; ++i) {
      if (i == praftservice_->state().id_) {
        continue;
      }
      sendRequestVote(servers_[i]);
    }

    std::chrono::milliseconds timeout_duration{GetRandomDuration()};
    std::unique_lock lck{timer_mutex_};
    status = timer_cv_.wait_for(lck, timeout_duration);
    if (praftservice_->state().state_ == State::Follower ||
        praftservice_->state().state_ == State::Leader) {
      message_receiver_thr_.request_stop();
      message_processing_thr.request_stop();
      break;
    }
    // reset votedFor
    praftservice_->state().voted_for_.reset(); 
    num_votes = 1;
    lck.unlock();
    {
      std::lock_guard lck{messages_mutex};
      message_q.clear();
    }
    messages_cv.notify_all();
  }

  std::println(
      "[server@{}]: ---- finish doCandidateLoop. term={}, state={} ----", port_,
      praftservice_->state().current_term_,
      StateToString(praftservice_->state().state_));

  std::cout << std::flush; 
  return praftservice_->state().state_;
}
/* ================ END CANDIDATE ============== */

/* ================ START LEADER ============== */
State Server::doLeaderLoop() {
  std::println("[server@{}]: ---- enter doLeaderLoop. term={} ----", port_,
               praftservice_->state().current_term_);

  std::cout << std::flush;
  praftservice_->state().leader_state_.next_index_.resize(numservers_);
  praftservice_->state().leader_state_.match_index_.resize(numservers_);

  auto start = std::chrono::steady_clock::now();
  std::chrono::duration<double> total{};

  std::deque<Message> message_q;
  std::deque<Message> appendentriesreply_q;
  std::deque<CommandMessage> command_q;

  std::mutex messages_mutex;
  std::condition_variable_any messages_cv;

  std::mutex commands_mutex;
  std::condition_variable_any commands_cv;

  std::jthread message_receiver_thr_{[&](std::stop_token stop_token) {
    struct pollfd *pfds;
    pfds = static_cast<struct pollfd *>(calloc(1, sizeof(struct pollfd)));
    pfds[0].fd = sockfd_;
    pfds[0].events = POLL_IN;
    int n;
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
            const bool has_put_msg = pcommand_->hasPut({buf, n});
            const auto ae_reply =
                praftservice_->hasAppendEntriesReply({buf, n});
            const bool has_ae_reply = ae_reply != std::nullopt;

            if (has_put_msg || has_ae_reply) {
              {
                std::lock_guard lck{commands_mutex};
                if (has_put_msg) {
                  // add the command locally in the commit log
                  praftservice_->state().commit_log_.push_back(
                      Entry{praftservice_->state().current_term_,
                            std::string{std::string_view{buf, n}}});
                  command_q.emplace_back(
                      Message{std::string{std::string_view{buf, n}},
                       {recv_si.peer_addr, recv_si.peer_addrlen}},
                      numservers_);
                } else {
                  appendentriesreply_q.push_back(
                      {std::string{buf, n},
                       {recv_si.peer_addr, recv_si.peer_addrlen}});
                }
              }
              commands_cv.notify_one();
            } else {
              std::lock_guard lck{messages_mutex};
              message_q.push_back({std::string{buf, n},
                                   {recv_si.peer_addr, recv_si.peer_addrlen}});
              messages_cv.notify_one();
            }
          }
        }
      }
    }
  }};

  const size_t required_votes = (numservers_ - 1) / 2 + 1;

  std::jthread command_processing_thr{[&](std::stop_token stop_token) {
    while (!stop_token.stop_requested()) {
      std::unique_lock lck{commands_mutex};
      auto pred = [&]() { return !appendentriesreply_q.empty(); };
      if (!commands_cv.wait(lck, stop_token, pred)) {
        return;
      }
      auto recv_data = appendentriesreply_q.front();
      appendentriesreply_q.pop_front();
      lck.unlock();

      std::string_view buf{recv_data.msg};

      std::cerr << port_ << " " << "leaderLoop process appendentriesq\n"; 
      if (auto resp = praftservice_->hasAppendEntriesReply(buf)) {
        if (std::get<0>(*resp)) {
          int id = std::get<1>(*resp);
          std::cerr << port_ << " " << "leaderLoop updateCommandQState with id " << id << '\n'; 
          {
            std::lock_guard lck{commands_mutex};
            if (!command_q.empty()) {
              updateCommandQState(command_q, id, required_votes);
            }
          }
          std::println("[server@{}]: entry safely replicated. commit "
                       "log size={}",
                       port_, praftservice_->state().commit_log_.size());
          std::cout << std::flush; 
        }
      }
    }
  }};

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
        std::println("[server@{}]: leaderLoop. found higher term. "
                     "curr_term={}",
                     port_, praftservice_->state().current_term_);
        praftservice_->state().state_ = State::Follower;
        praftservice_->state().current_term_ =
            praftservice_->hasHigherTerm(buf).second;

        timer_cv_.notify_one();
        break;
      } else if (praftservice_->hasHeartBeatInResponse(buf)) {
        /*
        std::println("[server@{}]: Leader got heartBeatResponse. "
                     "curr_term={}",
                     port_, praftservice_->state().current_term_);
        */
        std::lock_guard lck{messages_mutex};
        ++num_responses;
      }

      if (num_responses >= required_votes) {
        timer_cv_.notify_one();
        const auto end = std::chrono::steady_clock::now();
        const std::chrono::duration<double> diff = end - start;
        total += diff;
        start = end;
        /*
        std::println("[server@{}]: ---- Leader: QUORUM established. "
                     "term={} Leader since {} diff={}----",
                     port_, praftservice_->state().current_term_, total, diff);
        */
      }
    }
  }};

  bool has_heartbeats{false};
  int num_attempts{0};

  while (true) {
    if (!command_q.empty() &&
        command_q.front().num_replicated < required_votes) {
      std::cerr << port_ << " leaderloop sendAppendEntries\n";
      for (int i{}; i < numservers_; ++i) {
        if (i == praftservice_->state().id_) {
          continue;
        }
        if (!command_q.front().processed_entries[i]) {
          sendAppendEntries(command_q.front(), i);
        }
      }

      std::chrono::milliseconds timeout_duration{GetRandomDuration()};
      std::unique_lock lck{commands_mutex};
      [[maybe_unused]] auto cv_status =
          timer_cv_.wait_for(lck, timeout_duration);    
    } else {
      for (int i{}; i < numservers_; ++i) {
        if (i == praftservice_->state().id_) {
          continue;
        }
        sendHeartBeat(i);
      }
      std::chrono::milliseconds timeout_duration{GetRandomDuration()};
      std::unique_lock lck{messages_mutex};
      has_heartbeats = timer_cv_.wait_for(lck, timeout_duration, [&]() {
        return num_responses >= required_votes;
      });
      message_q.clear();
      if (has_heartbeats) {
        num_responses = 0;
        num_attempts = 0;
        lck.unlock();
        continue;
      }
      lck.unlock();

      if (!has_heartbeats) {
        ++num_attempts;
        if (num_attempts >= 3) {
          std::println("[server@{}]: ---- Leader: !!LOST QUORUM!!. Change to "
                       "Follower term={} ----",
                       port_, praftservice_->state().current_term_);
          break;
        }
      }
    }
    std::println("[server@{}]: Leader state for={}", port_, total);
  }
  return State::Follower;
}

void Server::updateCommandQState(
  std::deque<CommandMessage>& command_q,
  int id,
  int required_votes) {
  command_q.front().processed_entries[id] = true;
  ++command_q.front().num_replicated;
  if (command_q.front().num_replicated >= required_votes) {
    // respond to client
    command_q.pop_front();
    // command safely replicated on majority. update the commit
    // index.
    praftservice_->state().commit_index_ =
        praftservice_->state().commit_log_.size() - 1;
  }
}
/* ================ END LEADER ================= */

// ================= SENDERS ================= */
void Server::sendRequestVote(const ServerInfo &server_info) {
  struct sockaddr_in their_addr; // connector's address info
  memset(&their_addr, 0, sizeof(their_addr));
  their_addr.sin_family = AF_INET;                // host byte order
  their_addr.sin_port = htons(server_info.port_); // network byte order
  their_addr.sin_addr.s_addr = htonl(INADDR_ANY);

  const auto vote_term = praftservice_->state().current_term_;
  const int candidate_id = praftservice_->state().id_;

  flatbuffers::FlatBufferBuilder fbb(1024);
  const int last_log_index = praftservice_->state().commit_log_.size() - 1;
  const int last_log_term =
      praftservice_->state().commit_log_[last_log_index].term;
  auto o = CreateRequestVoteRPC(fbb, 55, vote_term, candidate_id, last_log_index,
                                last_log_term);                        
  fbb.Finish(o);

  uint8_t *buf = fbb.GetBufferPointer();
  int size = fbb.GetSize();

  if (sendto(sockfd_, buf, size, 0, (struct sockaddr *)&their_addr,
             sizeof(their_addr)) != size) {
    perror("sendto");
    fprintf(stderr, "error sending REQUEST VOTE");
  }
}

// reply false if term < currentTerm
// If votedFor is null or candidateId, and candidate’s log is at
//    least as up-to-date as receiver’s log, grant vote
// at most one vote in a given term
bool Server::sendRequestVoteResponse(std::string_view request,
                                     const SenderInfo &sender_info) {
  const RequestVoteRPC *preq =
      GetRequestVoteRPC(static_cast<const void *>(request.data()));
  if (!preq) {
    std::cerr << "sendRequestVoteResponse preq null\n";
    return false;
  }

  std::println("[server@{}]: sendRequestVoteResponse to={} term={}", port_,
               preq->candidate_id(),
               praftservice_->state().current_term_);
      
  const int term = preq->term();
  const int curr_term = praftservice_->state().current_term_;

  auto &voted_for_opt = praftservice_->state().voted_for_;
  int voted_for = -1;
  if (voted_for_opt) {
    voted_for = voted_for_opt.value();
  }

  bool vote_granted{false};

  if (term >= curr_term &&
      ((!voted_for_opt || voted_for_opt.value() == preq->candidate_id()) &&
       preq->last_log_index() >= praftservice_->state().commit_index_)) {
    voted_for_opt = preq->candidate_id();
    std::cerr << port_ <<  " " << " voted for " << preq->candidate_id() << std::flush;
    vote_granted = true;
  }

  flatbuffers::FlatBufferBuilder fbb{1024};
  auto o = CreateRequestVoteRPCReply(fbb, 66, curr_term, vote_granted);
  fbb.Finish(o);

  uint8_t *reply_buf = fbb.GetBufferPointer();
  int size = fbb.GetSize();

  int ret = ::sendto(sockfd_, reply_buf, size, 0,
                     (struct sockaddr *)&sender_info.peer_addr,
                     sender_info.peer_addrlen);
  if (ret != size) {
    perror("sendto");
    std::cerr << "error sending requestvote response\n";
  }

  std::println("[server@{}]: vote_granted={}, to={} voted_for={}, for term={}",
               port_, vote_granted, preq->candidate_id(), voted_for, term);

  return vote_granted;
}

void Server::sendAppendEntries(const CommandMessage &message,
                               size_t server_idx) {
  std::println("[server@{}]: sendAppendEntries term={}", port_,
               praftservice_->state().current_term_);

  // update next index
  praftservice_->state().leader_state_.next_index_[server_idx] =
      praftservice_->state().commit_log_.size();

  int term = praftservice_->state().current_term_;
  int leader_id = praftservice_->state().id_;

  int leader_commit = praftservice_->state().commit_log_.size() - 1;

  int prev_log_index = leader_commit - 1;
  int prev_log_term = praftservice_->state().commit_log_[prev_log_index].term;

  flatbuffers::FlatBufferBuilder builder;
  auto fb_str = builder.CreateString(message.msg.c_str());
  auto log_entry = CreateLogEntry(builder, 44, term, fb_str);

  std::vector<::flatbuffers::Offset<LogEntry>> entries_v;
  entries_v.push_back(log_entry);

  auto entries = builder.CreateVector(entries_v);
  auto off = CreateAppendEntriesRPC(builder, term, leader_id, prev_log_index,
                                    prev_log_term, entries, leader_commit, 11);
  builder.Finish(off);

  uint8_t *buf = builder.GetBufferPointer();
  int size = builder.GetSize();

  const auto server_info = servers_[server_idx];
  struct sockaddr_in their_addr; // connector's address info
  memset(&their_addr, 0, sizeof(their_addr));
  their_addr.sin_family = AF_INET;                // host byte order
  their_addr.sin_port = htons(server_info.port_); // network byte order
  their_addr.sin_addr.s_addr = htonl(INADDR_ANY); // network byte order

  if (sendto(sockfd_, buf, size, 0, (struct sockaddr *)&their_addr,
             sizeof(their_addr)) != size) {
    perror("sendto");
    fprintf(stderr, "error sending REQUEST VOTE");
  }
}

/*
1. Reply false if term < currentTerm (§5.1)
2. Reply false if log doesn’t contain an entry at prevLogIndex
whose term matches prevLogTerm (§5.3)
3. If an existing entry conflicts with a new one (same index
but different terms), delete the existing entry and all that
follow it (§5.3)
4. Append any new entries not already in the log
5. If leaderCommit > commitIndex, set commitIndex =
min(leaderCommit, index of last new entry)
*/
void Server::sendAppendEntriesResponse(std::string_view request,
                                       const SenderInfo &sender_info) {

  const void *pbuf = static_cast<const void *>(request.data());
  const AppendEntriesRPC *preq = GetAppendEntriesRPC(pbuf);

  if (!preq) {
    std::cerr << "sendAppendEntriesResponse bad request\n";
  }

  const int term = preq->term();
  const int prev_log_index = preq->prev_log_index();
  const int prev_log_term = preq->prev_log_term();
  const auto entries = preq->entries();
  std::string cmd{entries->Get(0)->command()->str()};
  int cmd_term = entries->Get(0)->term();
  const int leader_commit = preq->leader_commit();
  const int leader_id = preq->leader_id();

  std::println("[server@{}]: sendAppendEntriesResponse input "
               "term = {}, prev_log_index={}, prev_log_term={} cmd={}",
               port_, term, prev_log_index, prev_log_term, cmd);

  bool success{true};
  // 1. Reply false if term < currentTerm (§5.1)
  if (term < praftservice_->state().current_term_) {
    std::println("[server@{}]: sendAppendEntriesResponse term < current_term_", port_); 
    success = false;
  } 
  auto& log = praftservice_->state().commit_log_;

  // 2. Reply false if log doesn’t contain an entry at prevLogIndex
  // whose term matches prevLogTerm (§5.3)
  if (log.size() > prev_log_index &&
      log[prev_log_index].term != prev_log_term) {
    std::println("[server@{}]: sendAppendEntriesResponse mismatch log", port_);
    success = false;
  }

  if (success) {
    const int current_index = prev_log_index + 1;
    // 3. If an existing entry conflicts with a new one (same index
    // but different terms), delete the existing entry and all that
    // follow it (§5.3)
    if ((log.size() > current_index) && log[current_index].term != term) {
      log.erase(log.begin() + current_index, log.end());
      log.push_back({cmd_term, cmd});
    } else {
      log.push_back({cmd_term, cmd});
    }

    if (leader_commit > praftservice_->state().commit_index_) {
      praftservice_->state().commit_index_ =
          std::min(leader_commit, static_cast<int>(log.size() - 1));

      praftservice_->state().leader_id_ = preq->leader_id();
      praftservice_->state().current_term_ = term;
    }
  }

  flatbuffers::FlatBufferBuilder fbb{1024};
  auto o =
      CreateAppendEntriesRPCReply(fbb, 22, praftservice_->state().current_term_,
                                  success, praftservice_->state().id_);
  fbb.Finish(o);

  uint8_t *reply_buf = fbb.GetBufferPointer();
  int size = fbb.GetSize();

  int ret = ::sendto(sockfd_, reply_buf, size, 0,
                     (struct sockaddr *)&sender_info.peer_addr,
                     sender_info.peer_addrlen);
  if (ret != size) {
    std::cerr << "error sending heartbeat response\n";
  }

  std::cerr << port_ << " sendAppendEntriesResponse to " << leader_id << " success " << success << std::endl; 
  std::println("[server@{}]: sendAppendEntriesResponse to={}. term={} "
               "success={} log size={}",
               port_, leader_id, praftservice_->state().current_term_, success,
               praftservice_->state().commit_log_.size());
}

void Server::sendHeartBeat(size_t server_idx) {
  /*
  std::println("[server@{}]: sendHeartBeat to={}. term={}",
    port_,
    server_idx,
    praftservice_->state().current_term_);
  */
  flatbuffers::FlatBufferBuilder fbb(1024);
  auto off = CreateAppendEntriesRPC(fbb, praftservice_->state().current_term_,
                                    praftservice_->state().id_, 11);
  fbb.Finish(off);

  uint8_t *buf = fbb.GetBufferPointer();
  int size = fbb.GetSize();

  const auto server_info = servers_[server_idx];
  struct sockaddr_in their_addr; // connector's address info
  memset(&their_addr, 0, sizeof(their_addr));
  their_addr.sin_family = AF_INET;                // host byte order
  their_addr.sin_port = htons(server_info.port_); // network byte order
  their_addr.sin_addr.s_addr = htonl(INADDR_ANY); // network byte order

  if (sendto(sockfd_, buf, size, 0, (struct sockaddr *)&their_addr,
             sizeof(their_addr)) != size) {
    perror("sendto");
    fprintf(stderr, "error sending REQUEST VOTE");
  }
}

void Server::sendHeartBeatResponse(std::string_view request,
                                   const SenderInfo &sender_info) {
  const void *pbuf = static_cast<const void *>(request.data());
  const AppendEntriesRPC *preq = GetAppendEntriesRPC(pbuf);

  if (!preq) {
    std::cerr << "request not of type sendHeartBeatResponse\n";
    return;
  }

  bool success{true};
  int curr_term = praftservice_->state().current_term_;
  if (preq->term() < curr_term) {
    success = false;
  }

  praftservice_->state().leader_id_ = preq->leader_id();
  praftservice_->state().current_term_ = preq->term();

  /*
  std::println("[server@{}]: sendHeartBeatReply to={} term={}",
    port_,
    preq->leader_id(),
    praftservice_->state().current_term_);
  */
  flatbuffers::FlatBufferBuilder fbb{1024};
  auto o = CreateAppendEntriesRPCReply(fbb, 22, curr_term, success); // TODO
  fbb.Finish(o);

  uint8_t *reply_buf = fbb.GetBufferPointer();
  int size = fbb.GetSize();

  int ret = ::sendto(sockfd_, reply_buf, size, 0,
                     (struct sockaddr *)&sender_info.peer_addr,
                     sender_info.peer_addrlen);
  if (ret != size) {
    std::cerr << "error sending heartbeat response\n";
  }
}

void Server::sendLeaderRedirect(const SenderInfo &sender_info) {
  std::println("[server@{}]: redirect to leader_id={}", port_,
               praftservice_->state().leader_id_);

  flatbuffers::FlatBufferBuilder fbb{1024};
  auto o = CreateCommandResponse(
      fbb, false, servers_[praftservice_->state().leader_id_].port_);
  fbb.Finish(o);

  uint8_t *reply_buf = fbb.GetBufferPointer();
  int size{fbb.GetSize()};

  int ret = ::sendto(sockfd_, reply_buf, size, 0,
                     (struct sockaddr *)&sender_info.peer_addr,
                     sender_info.peer_addrlen);
  if (ret != size) {
    perror("sendto");
    std::cerr << "error sending requestvote response\n";
  }
}

int proc_numeric_id{};

void termination_handler(int signal) {
  std::cerr << "terminate " << proc_numeric_id << std::endl;
  std::exit(EXIT_FAILURE);
}

int main(int argc, char **argv) {

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

  // TODO: read from the log

  Server s{num_servers, idx, serverinfo};
}
