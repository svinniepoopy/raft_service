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
    ~Server();
  private:
    // server main loop
    void start();
    
    // loops for each of the states
    State doFollowerLoop();
    State doCandidateLoop();
    State doLeaderLoop();

    // loop internal methods
    void doCandidateRequestVotes();
    void doCandidateListen();

    // senders
    void sendRequestVote(size_t /*server_idx*/);
    void sendRequestVoteResponse(SenderInfo);

    void sendHeartBeat(size_t /*server_idx*/);
    void sendHeartBeatResponse(SenderInfo);

    void sendRequestVoteReply(size_t /*server_idx*/);
    void sendAppendEntriesReply(size_t /*server_idx*/);

    std::condition_variable_any timer_cv_;
    std::mutex timer_mutex_;

    // used in the candidate loop
    std::condition_variable candidate_loop_cv_;
    std::mutex candidate_loop_mutex_; 

    // used in the leader loop
    std::condition_variable leader_loop_cv_;
    std::mutex leader_loop_mutex_; 

    int sockfd_;
    std::unique_ptr<RaftService> praftservice_;

    int numservers_;
    std::vector<ServerInfo> servers_;
};

#endif // SERVER_H
