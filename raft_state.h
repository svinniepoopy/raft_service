#ifndef RAFT_STATE_H 
#define RAFT_STATE_H 

#include <string>
#include <vector>

#include <cstdint>
#include <optional>

using CandidateId = int;

enum class State : uint8_t { Follower, Candidate, Leader };

struct Entry {
  int term;
  std::string command;
};

struct RaftState {

  bool voted_in_current_term_{false};

  int id_;

  State state_{State::Follower};

  // index of highest log entry known to be committed
  int commit_index_{};

  // index of highest log entry applied to state mahchine
  int last_applied_{};

  /* === persistent state ===
   * updated on stable storage before responding to RPCs
   */
  // latest term server has seen
  int current_term_{};

  std::optional<CandidateId> voted_for_;

  std::vector<Entry> commit_log_;

  /* end persistent state */
};

// volatile state on leaders
struct LeaderState {
  std::vector<int> next_index;
  std::vector<int> match_index;
};


#endif // RAFT_STATE_H 