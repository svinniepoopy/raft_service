
#include <cstdint>
#include <string>

#include <sys/socket.h>
#include <sys/types.h>

struct ServerInfo {
    std::string ip_;
    uint16_t port_;
};

struct SenderInfo {
  struct sockaddr_storage* peer_addr;
  socklen_t peer_addrlen;
};
