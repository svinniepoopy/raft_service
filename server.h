#ifndef SERVER_H
#define SERVER_H

class RaftService;
class Cluster;

class Server {
  public:
    Server(int host, int port, std::shared_ptr<Cluster> pcluster);

    bool doAppendEntries();
    bool doRequestVote();

  private:
    State m_state;
    std::unique_ptr<RaftService> m_praftservice;
    std::shared_ptr<Cluster> m_pcluster;
};

#endif // SERVER_H
