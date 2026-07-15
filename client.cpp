#include "generated/CommandPut_generated.h"
#include "generated/CommandResponse_generated.h"

#include <iostream>
#include <string>

#include <cstring>
#include <cstdlib>

#include <netdb.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>

int main(int argc, char* argv[]) {

  flatbuffers::FlatBufferBuilder builder;

  std::string key{argv[2]};
  std::string val{argv[3]};
  auto fb_key = builder.CreateString(key.c_str());
  auto fb_val = builder.CreateString(val.c_str());

  auto off = CreateCommandPut(builder, fb_key, fb_val);
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

  if (connect(sockfd, res->ai_addr, res->ai_addrlen) == -1) {
    perror("connect");
    std::cerr << "exit connect\n";
    std::exit(EXIT_FAILURE);   
  }

  struct sockaddr_in their_addr; // connector's address info
  memset(&their_addr, 0, sizeof(their_addr));
  their_addr.sin_family = AF_INET;     // host byte order
  their_addr.sin_port = htons(std::stoi(argv[1])); // network byte order
  their_addr.sin_addr.s_addr = htonl(INADDR_ANY); 
  
  if (sendto(sockfd_, buf, size, 0, (struct sockaddr*)&their_addr, sizeof(their_addr)) != size) {
    perror("sendto");
    fprintf(stderr, "error sending REQUEST VOTE");
  }

  freeaddrinfo(res); 
}