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
    
    // loops for each of the states
    State doFollowerLoop();
    State doCandidateLoop();
    State doLeaderLoop();

    void doCandidateRequestVotes();
    void doCandidateListen();

    void sendRequestVote(size_t /*server_idx*/)
    void sendHeartBeat(size_t /*server_idx*/);

    State getNextState(void* /*buf*/, size_t /*n*/, SenderInfo);

    // timer data members
    bool timer_expired_{false};
    std::condition_variable_any timer_cv_;
    std::mutex timer_mutex_;

    // used in the candidate loop
    std::condition_variable candidate_loop_cv_;
    std::mutex candidate_loop_mutex_; 

    // used in the leader loop
    std::condition_variable leader_loop_cv_;
    std::mutex leader_loop_mutex_; 
    // true if a message was received before timer expiry
    bool has_msg_{false};


    int sockfd_;
    std::unique_ptr<RaftService> praftservice_;
    std::shared_ptr<Cluster> pcluster_;

    size_t numservers_;
    std::vector<ServerInfo> servers_;
};

#endif // SERVER_H
