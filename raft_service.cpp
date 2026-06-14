#include "raft_server.h"

std::pair<State, StateData> RaftService::GetNextStateAppendEntries(State state, const StateData& data) {

}

std::pair<State, StateData> RaftService::GetNextStateRequestVote(
    RequestVoteRPC* prequest,
    State state,
    const StateData& data) {

  const int term = prequest->term();
  const int candidate_id = prequest->candidate_id();
  const int last_log_index = prequest->last_log_index();
  
  const int last_log_term = prequest->last_log_term();

  // no change
  if (term < current_term_) {
    return {State, data};
  }

  if ((!voted_for_ || *voted_for == candidate_id) &&
      last_log_index >= commit_index_) {
    // send grant vote reply to RequestVote's sender

  }

}
