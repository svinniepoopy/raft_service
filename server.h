#ifndef SERVER_H
#define SERVER_H

class RaftService;
class Cluster;

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

    State m_state;
    std::unique_ptr<RaftService> m_praftservice;
    std::shared_ptr<Cluster> m_pcluster;
};

#endif // SERVER_H
