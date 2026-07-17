#include "generated/CommandPut_generated.h"
#include "generated/CommandResponse_generated.h"

#include "command.h"

#include <iostream>
#include <string>
#include <string_view>

#include <cstdint>
#include <cstring>
#include <cstdlib>

#include <netdb.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>

size_t BUFSIZE = 1024;

int main(int argc, char* argv[]) {
  const char usage[] = "Usage: client <port> <key> <value>\n";
  if (argc != 4) {
    std::cerr << usage;
    return 1;
  }

  flatbuffers::FlatBufferBuilder builder;

  std::string key{argv[2]};
  std::string val{argv[3]};
  auto fb_key = builder.CreateString(key.c_str());
  auto fb_val = builder.CreateString(val.c_str());

  auto off = CreateCommandPut(builder, 99, fb_key, fb_val);
  builder.Finish(off);

  uint8_t* buf = builder.GetBufferPointer();
  int size = builder.GetSize();

  struct addrinfo hints, *res;
  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_DGRAM;
  hints.ai_flags = AI_PASSIVE;
  hints.ai_protocol = 0; // any protocol
  hints.ai_canonname = nullptr;
  hints.ai_addr = nullptr;
  hints.ai_next = nullptr;

  std::string portstr{argv[1]};
  // if ai_passive is specified in hints.ai_flags and node is NULL then
  // then returned socket will bind to INADDR_ANY
  int s = ::getaddrinfo(nullptr, portstr.c_str(), &hints, &res);
  if (s != 0) {
    perror("getaddrinfo");
    std::cerr << "exit getaddrinfo\n";
    std::exit(EXIT_FAILURE);
  }

  int sockfd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
  if (sockfd == -1) {
    perror("socket");
    std::cerr << "exit socket\n";
    std::exit(EXIT_FAILURE);
  }

  struct sockaddr_in their_addr; // connector's address info
  memset(&their_addr, 0, sizeof(their_addr));
  their_addr.sin_family = AF_INET;     // host byte order
  their_addr.sin_port = htons(std::stoi(argv[1])); // network byte order
  their_addr.sin_addr.s_addr = htonl(INADDR_ANY); 
  
  if (sendto(sockfd, buf, size, 0, (struct sockaddr*)&their_addr, sizeof(their_addr)) != size) {
    perror("sendto");
    fprintf(stderr, "error sending command\n");
  }

  /*
  socklen_t their_addr_len = sizeof(their_addr);
  char recvbuf[BUFSIZE];

  int n = ::recvfrom(sockfd, recvbuf, BUFSIZE, 0, (struct sockaddr *)&their_addr,
                 &their_addr_len);

  if (n > 0) {
    std::string_view vbuf{recvbuf, n};
    const CommandResponse *pcmd =
        GetCommandResponse(static_cast<const void *>(vbuf.data()));
    if (pcmd && pcmd->success()) {
      std::cerr << "ok\n";
    }
  }
  */  
  freeaddrinfo(res);

  return 0;
}