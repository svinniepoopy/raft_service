#ifndef RAFT_SERVICE_H
#define RAFT_SERVICE_H

#include "raft_state.h"

#include <string_view>

class RaftService {
public:
  // request vote
  bool hasRequestVoteRequest(std::string_view);
  bool hasRequestVoteReply(std::string_view);
  
  bool hasHeartBeatInRequest(std::string_view);
  bool hasNewLeaderInResponse(std::string_view);

  // append entries
  bool hasAppendEntriesRequest(std::string_view);
  bool hasAppendEntriesReply(std::string_view);

  const RaftState &state() const { return raftstate_; }

  RaftState &state() { return raftstate_; }

private:
  RaftState raftstate_{State::Follower};
};

#endif // RAFT_SERVICE_H
