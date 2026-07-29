#include "raft_service.h"

#include "raft_state.h"

#include "generated/AppendEntries_generated.h"
#include "generated/AppendEntriesReply_generated.h"
#include "generated/RequestVote_generated.h"
#include "generated/RequestVoteReply_generated.h"

#include <optional>
#include <string_view>
#include <tuple>
#include <utility>

std::pair<bool, int> RaftService::hasHigherTerm(std::string_view buf) {
  const void* pbuf = static_cast<const void*>(buf.data());
  if (const RequestVoteRPC *rpc = GetRequestVoteRPC(pbuf);
      rpc && rpc->term() > raftstate_.current_term_) {
    return {true, rpc->term()};
  }
  if (const RequestVoteRPCReply *rpc = GetRequestVoteRPCReply(pbuf);
      rpc && rpc->term() > raftstate_.current_term_) {
    return {true, rpc->term()};
  }
  if (const AppendEntriesRPC* rpc = GetAppendEntriesRPC(pbuf);
      rpc && rpc->term() > raftstate_.current_term_) {
    return {true, rpc->term()};
  }
  if (const AppendEntriesRPCReply* rpc = GetAppendEntriesRPCReply(pbuf);
      rpc && rpc->term() > raftstate_.current_term_) {
    return {true, rpc->term()};
  }
  return {false, raftstate_.current_term_};
}

bool RaftService::hasRequestVoteRequest(std::string_view buf) {
  const void* pbuf = static_cast<const void*>(buf.data());
  const RequestVoteRPC* reply = GetRequestVoteRPC(pbuf);
  if (!reply) {
    return false;
  }
  return reply && reply->version() == 105;
}

bool RaftService::hasRequestVoteReply(std::string_view buf) {
  const void* pbuf = static_cast<const void*>(buf.data());
  const RequestVoteRPCReply* reply = GetRequestVoteRPCReply(pbuf);
  return reply != nullptr; 
}

bool RaftService::hasHeartBeatInRequest(std::string_view buf) {
  const void* pbuf = static_cast<const void*>(buf.data());
  const AppendEntriesRPC* rpc = GetAppendEntriesRPC(pbuf);
  return rpc != nullptr;
}

bool RaftService::hasHeartBeatInResponse(std::string_view buf) {
  const void* pbuf = static_cast<const void*>(buf.data());
  const AppendEntriesRPCReply* reply = GetAppendEntriesRPCReply(pbuf);
  if (!reply) {
    return false;
  }
  return reply->success(); 
}

bool RaftService::hasVoteInRequestVoteResponse(std::string_view buf) {
  const void* vbuf = static_cast<const void*>(buf.data());
  /*
  const uint8_t* buffer = static_cast<const uint8_t*>(vbuf);
  int size = buf.size();
  flatbuffers::Verifier verifier(buffer, size); 
 
  if (!VerifyRequestVoteRPCReplyBuffer<>(verifier)) {
    return false;
  }
  */
  const RequestVoteRPCReply* reply = GetRequestVoteRPCReply(vbuf);

  if (!reply) {
    return false;
  }
  if (reply && reply->version() != 104) {
    return false;
  }
  return reply && reply->vote_granted();
}

// append entries
bool RaftService::hasAppendEntriesRequest(std::string_view buf) {
  const void* pbuf = static_cast<const void*>(buf.data());
  const AppendEntriesRPC* reply = GetAppendEntriesRPC(pbuf);
  if (reply == nullptr) {
    return false;
  }
  if (reply->version() != 11) {
    return false;
  }
  const auto entries = reply->entries();
  if (!entries) {
    return false;
  }
  return entries->size() > 0;
}

std::optional<std::tuple<bool, int, int>> RaftService::hasAppendEntriesReply(std::string_view buf) {
  const void* pbuf = static_cast<const void*>(buf.data());
  const AppendEntriesRPCReply* reply = GetAppendEntriesRPCReply(pbuf);
  if (!reply) {
    return {};
  }
  if (reply && reply->version() != 103) {
    return {};
  }
  return std::optional{std::make_tuple(reply->success(), reply->id(), reply->term())};
}