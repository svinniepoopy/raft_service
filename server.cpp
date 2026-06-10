#include "server.h"

Server::Server(
    std::string host,
    int port,
    std::shared_ptr<Cluster> pCluster):
  m_host{host},
  m_port{port},
  m_praftserver{std::make_unique<RaftService>()},
  m_pCluster{pCluster}{

  // install a signal handler from cluster
}

// in a infinite loop
//      listen to message on port
//      on receiving message
//         if received message is Request
void Server::start() {
  while (true) {
    if (state_ == ServerState::Follower) {
      state_ = doFollowerLoop();
    } else if (state_ = ServerState::Candidate) {
      state_ = doCandidateLoop();
    } else if (state_ == ServrState::Leader) {
      state_ = doLeaderLoop();
    }
  }
}

void Server::startTimer(State state) {
  // TODO: make sure the timer thread will stop for certain events
  auto timer_func = [&]() {
    while (true) {
      auto st = timer_cv.wait(timer_mutex, timeout_duration, []() {
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
  startTimer(State::Follower);
  // receive operations should be non-blocking
  // use select, poll or epoll?
  // this should always be ready to receive messages
  while (true) {
    // TODO: make atomic
    if (timer_expired_) {
      return State::Candidate;
    }
    ssize_t n = ::recv(sockfd_, buf, BUFBYTES); 

    // TODO: process with std::async
    message_t msg{buf, n};

    auto f = std::async(std::launch::async, Parser{}, msg);
    auto next_state = GetNextState(f.get());
    if (next_state != State::Follower) {
      return next_state;
    }
  }
}

void Server::doCandidateLoop() {

  // increment current term
  while (true) {

  }
  pCluster->broadcast(/*requestForVote*/);
}

void Server::doLeaderLoop() {
 auto pred = [&]() {
   // TODO leader specific recv
    int n = ::recv(sockfd, buf, BUFSIZE);

    return n > 0;
  };

  // increment current term
  pCluster->broadcast(/*AppendEntriesRPC*/);
  while (state_ == Leader) {

    auto status = cv_.wait_for(lock_, GetTimeout(), pred);
    if (status == std::cv_status::timeout) {
      return State::Follower;
    } else {
      // process received messages
      // - did it win the election?
      // - did another server get elected as leader?
      state_ = GetNextState();
    }
  }
}
