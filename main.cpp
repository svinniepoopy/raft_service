#include "cluster.h"
#include "serverinfo.h"

#include <memory>
#include <vector>

int main(int argc, char** argv) {
    // extract server info from args
    std::vector<ServerInfo> serverInfos;
    std::unique_ptr<Cluster> pCluster = std::make_unique<Cluster>(std::move(serverInfos));

    pCluster->start();

    return 0;
}
