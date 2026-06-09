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

//
State Server::doFollowerLoop() {
  auto pred = [&]() {
    int n = ::recv(sockfd, buf, BUFSIZE);
    recv_bytes = n; // TODO 
    return n > 0;
  };

  while (state_ == Follower) {
    auto status = cv_.wait_for(lock_, GetTimeout(), pred);
    if (status == std::cv_status::timeout) {
      return State::Candidate;
    } else {
      // process received message
      state_ = GetNextState();
    }
  }
}

void Server::doCandidateLoop() {
  auto pred = [&]() {
    int n = ::recv(sockfd, buf, BUFSIZE);
    recv_bytes = n; // TODO 
    return n > 0;
  };

  // increment current term
  while (state_ == Candidate) {
    pCluster->broadcast(/*requestForVote*/);
    auto status = cv_.wait_for(lock_, GetTimeout(), pred);
    if (status == std::cv_status::timeout) {
      state_ = Candidate;
    } else {
      // TODO process received messages
      // - did it win the election?
      // - did another server get elected as leader?
      state_ = GetNextState();
    }
  }
}

void Server::doLeaderLoop() {
 auto pred = [&]() {
   // TODO leader specific recv
    int n = ::recv(sockfd, buf, BUFSIZE);
    recv_bytes = n; // TODO 
    return n > 0;
  };

  // increment current term
  while (state_ == Leader) {
    pCluster->broadcast(/*AppendEntriesRPC*/);
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
