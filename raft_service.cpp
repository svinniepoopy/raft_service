#include "raft_service.h"

#include "raft_state.h"

#include "generated/AppendEntries_generated.h"
#include "generated/AppendEntriesReply_generated.h"
#include "generated/RequestVote_generated.h"
#include "generated/RequestVoteReply_generated.h"

#include <string_view>

bool RaftService::hasRequestVoteRequest(std::string_view buf) {
  const void* pbuf = static_cast<const void*>(buf.data());
  const RequestVoteRPC* reply = GetRequestVoteRPC(pbuf);
  return reply != nullptr;  
}

bool RaftService::hasRequestVoteReply(std::string_view buf) {
  const void* pbuf = static_cast<const void*>(buf.data());
  const RequestVoteRPCReply* reply = GetRequestVoteRPCReply(pbuf);
  return reply && reply->vote_granted();
}

bool RaftService::hasHeartBeatInRequest(std::string_view buf) {
  const void* pbuf = static_cast<void*>(buf.data());
  const AppendEntriesRPCReply* reply = GetAppendEntriesRPCReply(pbuf); 
  return reply ? reply->success() : false;
}

bool RaftService::hasNewLeaderInResponse(std::string_view buf) {
  const void* pbuf = static_cast<const void*>(buf.data());
  const AppendEntriesRPCReply* reply = GetAppendEntriesRPCReply(pbuf);

  if (!reply) {
    return false;
  }

  const int leaders_term = reply->term();
  return leaders_term >= raftstate_->current_term_;
}

// append entries
bool RaftService::hasAppendEntriesRequest(std::string_view buf) {
  const void* pbuf = static_cast<const void*>(buf.data());
  const AppendEntriesRPCReply* reply = GetAppendEntriesRPC(pbuf);
  return reply != nullptr;
}

bool RaftService::hasAppendEntriesReply(std::string_view buf) {
  const void* pbuf = static_cast<const void*>(buf.data());
  const AppendEntriesRPCReply* reply = GetAppendEntriesRPCReply(pbuf);
  return reply != nullptr;
}

/*
std::pair<State, StateData> RaftService::GetNextStateRequestVote(
    RequestVoteRPC* prequest,
    const RaftState& raftstate) {

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
*/