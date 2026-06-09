#ifndef SERVER_H
#define SERVER_H

#include "state_data.h"

class RaftService;
class Cluster;

using CandidateId = int;

enum class State : uint8_t {
  Follower,
  Candidate,
  Leader
};

class Server {
  public:
    Server(int host, int port, std::shared_ptr<Cluster> pcluster);

    // server main loop
    //
    // cluster calls each server's start method that enters server's main loop
    void start();

  private:
    void handleMessage(uint8_t* pbuf);

    bool doAppendEntries();
    bool doRequestVote();
    
    bool handleRequestVoteRPC();

    State state_{Follower};
    StateData data_;
    std::unique_ptr<RaftService> praftservice_;
    std::shared_ptr<Cluster> pcluster_;
};

#endif // SERVER_H
