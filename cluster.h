#ifndef CLUSTER_H
#define CLUSTER_H

class Cluster {
  public:
    void sendRequestVote();

    void sendAppendEntries(bool is_heartbeat = true);

private:
    std::vector<ServerInfo> m_serverinfos;
};

#endif // CLUSTER_H
