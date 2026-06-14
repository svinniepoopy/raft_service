#ifndef RAFT_SERVICE_H
#define RAFT_SERVICE_H

#include "generated/AppendEntries_generated.h"
#include "generated/RequestVote_generated.h"
#include "raft_state.h"

#include <utility>

class AppendEntriesRPC;
class RequestVoteRPC;

class RaftService {
public:  
  std::pair<RaftState, RaftReply> GetNextStateAppendEntries(AppendEntriesRPC*, RaftState);
  std::pair<RaftState, RaftReply> GetNextStateRequestVote(RequestVoteRPC*, RaftState);
};

#endif // RAFT_SERVICE_H
