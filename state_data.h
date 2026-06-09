#ifndef STATEDATA_H
#define STATEDATA_H

#include <cstdint>
#include <optional>

struct StateData {
  std::string host_;
  uint16_t port_;

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

#endif // STATEDATA_H
