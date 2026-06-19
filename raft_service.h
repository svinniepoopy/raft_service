#ifndef RAFT_SERVICE_H
#define RAFT_SERVICE_H

#include "raft_state.h"

#include <utility>

class AppendEntriesRPC;
class RequestVoteRPC;

class RaftService {
public:
  std::pair<RaftState, RaftReply> GetNextStateAppendEntries(AppendEntriesRPC*,
                                                            RaftState);
  std::pair<RaftState, RaftReply> GetNextStateRequestVote(RequestVoteRPC*,
                                                          RaftState);

  bool hasVoteInRequestVoteReply(std::string_view buf);
  bool hasHeartBeatResponse(std::string_view buf);

  const RaftState& state() const {
    return raftstate_;
  }

  RaftState& state() {
    return raftstate_;
  }

  RaftState raftstate_{State::Follower};

private:
    RaftState raftstate_{State::Follower};
};

#endif // RAFT_SERVICE_H
