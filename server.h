#ifndef SERVER_H
#define SERVER_H

#include "raft_state.h"
#include "server_info.h"

class RaftService;

#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

class Server {
  public:
    Server(int cluster_size, int server_idx, std::string servers); 
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
    void sendRequestVote(const ServerInfo&);
    void sendRequestVoteResponse(std::string_view request, const SenderInfo&);

    void sendHeartBeat(size_t /*server_idx*/);
    void sendHeartBeatResponse(std::string_view request, const SenderInfo&);

    int ReceiveHelper(int sockfd, char* buf, size_t size, SenderInfo& si);

    std::condition_variable_any timer_cv_;
    std::mutex timer_mutex_;

    // used in the candidate loop
    std::condition_variable candidate_loop_cv_;
    std::mutex candidate_loop_mutex_; 

    // used in the leader loop
    std::condition_variable leader_loop_cv_;
    std::mutex leader_loop_mutex_; 

    int port_;
    int sockfd_;
    std::unique_ptr<RaftService> praftservice_;

    int id_;
    int numservers_;
    std::vector<ServerInfo> servers_;
};

#endif // SERVER_H
