#ifndef STATEDATA_H
#define STATEDATA_H

#include "generated/AppendEntriesReply_generated.h"
#include "generated/RequestVoteReply_generated.h"
#include <cstdint>
#include <optional>
#include <sys/socket.h>

using CandidateId = int;

enum class State : uint8_t { Follower, Candidate, Leader };

struct RaftState {

  int id_;

  State state_{State::Follower};

  /* === persistent state ===
   * updated on stable storage before responding to RPCs
   */
  // latest term server has seen
  int current_term_{};

  // 
  std::optional<CandidateId> voted_for_;

  // std::vector<Entry> entries;

  /* end perisistent state */

  // index of highest log entry known to be committed
  int commit_index_{};

  int last_applied_{};
};

struct RaftReply {
};

struct RequestVoteRPCReply_T : RaftReply {
  RequestVoteRPCReply reply;
};

struct AppendEntriesRPCReply_T : RaftReply {
  AppendEntriesRPCReply reply;
}


#endif // STATEDATA_H
