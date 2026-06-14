#ifndef SERVER_H
#define SERVER_H

#include "raft_state.h"

class RaftService;
class Cluster;

#include <condition_variable>
#include <memory>

class Server {
  public:
    Server(int host, int port, std::shared_ptr<Cluster> pcluster);

  private:
    // server main loop
    void start();

    // expiration timer
    void startTimer();
    
    // loops for each of the states
    State doFollowerLoop();
    State doCandidateLoop();
    State doLeaderLoop();


    State getNextState(void* /*buf*/, size_t /*n*/, SenderInfo);

    bool timer_expired_{false};
    std::condition_variable timer_cv_;
    std::mutex timer_mutex_;

    int sockfd_;
    RaftState raftstate_{State::Follower};
    std::unique_ptr<RaftService> praftservice_;
    std::shared_ptr<Cluster> pcluster_;
};

#endif // SERVER_H
