#ifndef RAFT_STATE_H 
#define RAFT_STATE_H 

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

  // TODO: uncomment
  // std::vector<Entry> entries;

  /* end perisistent state */

  // index of highest log entry known to be committed
  int commit_index_{};

  int last_applied_{};
};


#endif // RAFT_STATE_H 