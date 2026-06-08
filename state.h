#ifndef STATE_H
#define STATE_H

#include <optional>

namespace State {

using CandidateId = int;

struct ServerState {

  /* === persistent state ===
   * updated on stable storage before responding to RPCs
   */
  // latest term server has seen
  int m_current_term{};

  // 
  std::optional<CandidateId> m_voted_for{};

  // std::vector<Entry> entries;

  /* end perisistent state */

  // index of highest log entry known to be committed
  int m_commit_index{};

  int m_last_applied{};

};

} // namespace state

#endif // STATE_H
