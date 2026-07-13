#ifndef COMMAND_H
#define COMMAND_H

#include <string_view>

class Command {
    public:
    bool hasPut(std::string_view);
    bool hasPutResponse(std::string_view);
};

#endif // COMMAND_H