#include "command.h"

#include "generated/CommandPut_generated.h"
#include "generated/CommandResponse_generated.h"

#include <string_view>
#include <print>
#include <iostream>
#include <cstdint>

bool Command::hasPut(std::string_view buf) {
  if (buf.empty()) {
    return false;
  }
  /*
  const uint8_t* buffer = static_cast<const uint8_t*>(vbuf);
  int size = buf.size();
  flatbuffers::Verifier verifier(buffer, size); 

  if (!VerifyCommandPutBuffer(verifier)) {
    return false;
  }
  */
  const CommandPut* pcmd = GetCommandPut(static_cast<const void *>(buf.data()));
  if (pcmd != nullptr && pcmd->version() == 99) {
    std::cerr << "hasPut version 1" << std::endl;
    return true;
  }
  return false;
}

bool Command::hasPutResponse(std::string_view buf) {
  if (buf.empty()) {
    return false;
  }
  const CommandResponse* pcmd = GetCommandResponse(static_cast<const void *>(buf.data()));
  return pcmd != nullptr;
}