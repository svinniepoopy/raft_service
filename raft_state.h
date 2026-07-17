#ifndef RAFT_STATE_H 
#define RAFT_STATE_H 

#include <optional>
#include <string>
#include <vector>

#include <cstdint>


using CandidateId = int;

enum class State : uint8_t { Follower, Candidate, Leader };

struct Entry {
  int term;
  std::string command;
};

struct LeaderState {
  // 
  std::vector<int> next_index_;
  std::vector<int> match_index_;
};

struct RaftState {
  int id_;

  int leader_id_;

  State state_{State::Follower};

  // index of highest log entry known to be committed
  // initialized to zero. 1-indexed.
  int commit_index_{};

  // index of highest log entry applied to state mahchine
  // initialized to zero. 1-indexed.
  int last_applied_{};

  // volatile state on leaders
  LeaderState leader_state_;

  /* === persistent state ===
   * updated on stable storage before responding to RPCs
   */
  // latest term server has seen
  int current_term_{};

  std::optional<CandidateId> voted_for_;

  std::vector<Entry> commit_log_;

  /* end persistent state */
};


#endif // RAFT_STATE_H 