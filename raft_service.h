#ifndef RAFT_SERVICE_H
#define RAFT_SERVICE_H

#include "raft_state.h"

#include <optional>
#include <string_view>
#include <utility>
#include <tuple>

class RaftService {
public:

  std::pair<bool, int> hasHigherTerm(std::string_view);

  // request vote
  bool hasRequestVoteRequest(std::string_view);
  bool hasRequestVoteReply(std::string_view);
  
  bool hasHeartBeatInRequest(std::string_view);
  bool hasHeartBeatInResponse(std::string_view);
  
  bool hasNewLeaderInResponse(std::string_view);
  bool hasVoteInRequestVoteResponse(std::string_view);

  // append entries
  bool hasAppendEntriesRequest(std::string_view);
  std::optional<std::tuple<bool, int, int>> hasAppendEntriesReply(std::string_view);
  bool entryReplicated(std::string_view);

  const RaftState &state() const { return raftstate_; }

  RaftState &state() { return raftstate_; }

private:
  RaftState raftstate_{}; // default follower
};

#endif // RAFT_SERVICE_H
