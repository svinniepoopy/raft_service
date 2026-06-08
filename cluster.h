#ifndef CLUSTER_H
#define CLUSTER_H

class Cluster {
  public:
    Cluster(std::vector<ServerInfo>&& serverInfos);

    void sendRequestVote();

    void sendAppendEntries(bool is_heartbeat = true);

    // 
    void start();

private:
    std::vector<ServerInfo> m_serverinfos;
};

#endif // CLUSTER_H
