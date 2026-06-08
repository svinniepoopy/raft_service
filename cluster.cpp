#include "cluster.h"

Cluster::Cluster(std::vector<ServerInfo>&& serverInfos):
  m_serverinfos{std::move(serverInfos)}
{}

void Cluster::sendRequestVote() {

}

void Cluster::sendAppendEntries(bool is_heartbeat) {

}

void Cluster::start() {

}
