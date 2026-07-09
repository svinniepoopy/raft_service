#include "raft_service.h"

#include "raft_state.h"

#include "generated/AppendEntries_generated.h"
#include "generated/AppendEntriesReply_generated.h"
#include "generated/RequestVote_generated.h"
#include "generated/RequestVoteReply_generated.h"

#include <string_view>
#include <utility>

std::pair<bool, int> RaftService::hasHigherTerm(std::string_view buf) {
  const void* pbuf = static_cast<const void*>(buf.data());
  if (const RequestVoteRPC *rpc = GetRequestVoteRPC(pbuf);
      rpc && rpc->term() >= raftstate_.current_term_) {
    return {true, rpc->term()};
  }
  if (const RequestVoteRPCReply *rpc = GetRequestVoteRPCReply(pbuf);
      rpc && rpc->term() >= raftstate_.current_term_) {
    return {true, rpc->term()};
  }
  if (const AppendEntriesRPC* rpc = GetAppendEntriesRPC(pbuf);
      rpc && rpc->term() >= raftstate_.current_term_) {
    return {true, rpc->term()};
  }
  if (const AppendEntriesRPCReply* rpc = GetAppendEntriesRPCReply(pbuf);
      rpc && rpc->term() >= raftstate_.current_term_) {
    return {true, rpc->term()};
  }
  return {false, raftstate_.current_term_};
}

bool RaftService::hasRequestVoteRequest(std::string_view buf) {
  const void* pbuf = static_cast<const void*>(buf.data());
  const RequestVoteRPC* reply = GetRequestVoteRPC(pbuf);
  return reply != nullptr;  
}

bool RaftService::hasRequestVoteReply(std::string_view buf) {
  const void* pbuf = static_cast<const void*>(buf.data());
  const RequestVoteRPCReply* reply = GetRequestVoteRPCReply(pbuf);
  return reply != nullptr; 
}

bool RaftService::hasHeartBeatInRequest(std::string_view buf) {
  const void* pbuf = static_cast<const void*>(buf.data());
  const AppendEntriesRPC* rpc = GetAppendEntriesRPC(pbuf);
  if (!rpc) {
    return false; 
  } 
  return true; 
}

bool RaftService::hasHeartBeatInResponse(std::string_view buf) {
  const void* pbuf = static_cast<const void*>(buf.data());
  const AppendEntriesRPCReply* reply = GetAppendEntriesRPCReply(pbuf); 
  return reply ? reply->success() : false;
}

bool RaftService::hasVoteInRequestVoteResponse(std::string_view buf) {
  const void* pbuf = static_cast<const void*>(buf.data());
  const RequestVoteRPCReply* reply = GetRequestVoteRPCReply(pbuf);

  if (!reply) {
    return false;
  }
  return reply->vote_granted();
}

// append entries
std::pair<bool, int> RaftService::hasAppendEntriesRequest(std::string_view buf) {
  const void* pbuf = static_cast<const void*>(buf.data());
  const AppendEntriesRPC* reply = GetAppendEntriesRPC(pbuf);
  if (reply) {
    return {true, reply->term()};
  }
  return {false, raftstate_.current_term_}; 
}

bool RaftService::hasAppendEntriesReply(std::string_view buf) {
  const void* pbuf = static_cast<const void*>(buf.data());
  const AppendEntriesRPCReply* reply = GetAppendEntriesRPCReply(pbuf);
  return reply != nullptr;
}