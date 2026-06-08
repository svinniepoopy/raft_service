#include "server.h"


Server::Server(
    std::string host,
    int port,
    std::shared_ptr<Cluster> pCluster):
  m_host{host},
  m_port{port},
  m_praftserver{std::make_unique<RaftService>()},
  m_pCluster{pCluster}{

}

void Server::start() {
  while (true) {

  }
}

