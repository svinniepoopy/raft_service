#include "server.h"
#include "raft_state.h"

#include "generated/AppendEntries_generated.h"
#include "generated/AppendEntriesReply_generated.h"
#include "generated/RequestVote_generated.h"
#include "generated/RequestVoteReply_generated.h"

#include 
#include <chrono>
#include <cstdio>
#include <cstdint>
#include <cstdlib
#include <future>
#include <string.h>
#include <string>
#include <memory>
#include <random>
#include <thread>

#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>

namespace {
size_t BUFSIZE = 1024;
std::uniform_int_distribution<> dist(150, 300);
std::random_device rd{};
std::mt19937 gen(rd());
std::chrono::milliseconds GetRandomDuration() {
  return std::chrono::milliseconds{dist(gen)};  
}
}

Server::Server(
    std::string host,
    std::string port,
    std::string server_info):
  praftservice_{std::make_unique<RaftService>()} {
  
  // TODO extract host/port for other servers in the cluster
  // from server_info
  
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
    if (raftstate_.state_ == State::Follower) {
      raftstate_.state_ = doFollowerLoop();
    } else if (raftstate_.state_ == State::Candidate) {
      raftstate_.state_ = doCandidateLoop();
    } else if (raftstate_.state_ == State::Leader) {
      raftstate_.state_ = doLeaderLoop();
    }
  }
}

void Server::startTimer() {
  // TODO: make sure the timer thread will stop for certain events
  // TODO: use stop_token
  auto timer_func = [&]() {
    while (true) {
      std::chrono::milliseconds timeout_duration = GetRandomDuration();
      std::unique_lock lck{timer_mutex_};
      auto st = timer_cv_.wait_for(lck, timeout_duration, []() {
	  return false;
	  });

      if (st == std::cv_status::timeout) {
	// TODO: this and GetNextState can update a shared variable
	// the shared variable will denote the next state
	// doTimeoutAction();
	

	// set timer expired and break
	// TODO: timer_expired_ can be atomic
	timer_expired_ = true;
	break;
      }
    }
  };
  std::jthread thr{timer_func};
}

// follower loop
// if timer expired then become candidate
// else if message(s) received within timer
// process messages from:
//   - other candidates
//   - leaders
// messages may arrive concurrently
State Server::doFollowerLoop() {
  startTimer();
  // receive operations should be non-blocking
  // use select, poll or epoll?
  // this should always be ready to receive messages
  char buf[BUFSIZE];
  while (true) {
    // TODO: make atomic
    if (timer_expired_) {
      return State::Candidate;
    }
    struct sockaddr_storage peer_addr;
    socklen_t peer_addrlen{sizeof(peer_addr)};

    char host[NI_MAXHOST], service[NI_MAXSERV];

    ssize_t n = ::recvfrom(
      sockfd_,
      buf,
      BUFSIZE,
      0,
      (struct sockaddr*) &peer_addr,
      &peer_addrlen); 

    if (n == -1) {
      continue;
    }
    
    int s = getnameinfo(
      (struct sockaddr*) &peer_addr,
      peer_addrlen,
      host,
      NI_MAXHOST,
      service,
      NI_MAXSERV,
      NI_NUMERICSERV);

    SenderInfo si{.peer_addr = &peer_addr, .peer_addrlen = peer_addrlen};

    auto fut = std::async(std::launch::async, [&]() {
      return getNextState(buf, n, si); 
    }); 
    auto next_state = fut.get(); 
    if (next_state != State::Follower) {
      return next_state;
    }
  }
}

State Server::doCandidateLoop() {

  // increment current term
  while (true) {

  }
  // pCluster->broadcast(/*requestForVote*/);
}

State Server::doLeaderLoop() {
 
}

State Server::getNextState(void* buf, size_t n, SenderInfo si) { 
  uint8_t* pbufu8 = static_cast<uint8_t*>(buf);

  const AppendEntriesRPC* p_app = GetAppendEntriesRPC(pbufu8);
  if (p_app) {
    auto [nextstate, reply] = praftservice_->GetNextStateAppendEntries(p_app, raftstate_);
    sendReply(reply, si);
    return nextstate.state_;
  } 
  const RequestVoteRPC* p_reqvote = GetRequestVoteRPC(pbufu8);
  auto [nextstate, reply] = praftservice_->GetNextStateRequestVote(p_reqvote, raftstate_); 
  sendReply(reply, si);
  return nextstate.state_;
}
