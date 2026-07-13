#include "command.h"

#include "generated/CommandPut_generated.h"
#include "generated/CommandResponse_generated.h"

#include <string_view>

bool Command::hasPut(std::string_view buf) {
    const CommandPut* pcmd = GetCommandPut(static_cast<const void*>(buf.data()));
    return pcmd != nullptr;
}

bool Command::hasPutResponse(std::string_view buf) {
    const CommandPut* pcmd = GetCommandPut(static_cast<const void*>(buf.data()));
    return pcmd != nullptr;
}