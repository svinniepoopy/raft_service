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

  if (hasRequestVoteRequest(buf)) {
    if (const RequestVoteRPC *rpc = GetRequestVoteRPC(pbuf);
        rpc && rpc->term() > raftstate_.current_term_) {
      return {true, rpc->term()};
    }
  }

  if (hasRequestVoteReply(buf)) {
    if (const RequestVoteRPCReply *rpc = GetRequestVoteRPCReply(pbuf);
        rpc && rpc->term() > raftstate_.current_term_) {
      return {true, rpc->term()};
    }
  }

  if (hasAppendEntriesRequest(buf)) {
    if (const AppendEntriesRPC *rpc = GetAppendEntriesRPC(pbuf);
        rpc && rpc->term() > raftstate_.current_term_) {
      return {true, rpc->term()};
    }
  }

  auto rep = hasAppendEntriesReply(buf);
  if (rep) {
    auto term = std::get<2>(*rep);
    return {true, term};
  }

  return {false, raftstate_.current_term_};
}

bool RaftService::hasRequestVoteRequest(std::string_view buf) {
  const void* pbuf = static_cast<const void*>(buf.data());
  const RequestVoteRPC* reply = GetRequestVoteRPC(pbuf);
  if (!reply) {
    return false;
  }
  return reply && reply->version() == 55;
}

bool RaftService::hasRequestVoteReply(std::string_view buf) {
  const void* pbuf = static_cast<const void*>(buf.data());
  const RequestVoteRPCReply* reply = GetRequestVoteRPCReply(pbuf);

  return reply && reply->version() == 66;
}

bool RaftService::hasHeartBeatInRequest(std::string_view buf) {
  const void* pbuf = static_cast<const void*>(buf.data());
  const AppendEntriesRPC* rpc = GetAppendEntriesRPC(pbuf);
  if (!rpc || rpc->version() != 11) {
    return nullptr;
  }
  const auto entries = rpc->entries();
  return entries == nullptr;
}

bool RaftService::hasHeartBeatInResponse(std::string_view buf) {
  const void* pbuf = static_cast<const void*>(buf.data());
  const AppendEntriesRPCReply* reply = GetAppendEntriesRPCReply(pbuf);
  if (!reply) {
    return false;
  }
  return reply && reply->version() == 22 && reply->success(); 
}

bool RaftService::hasVoteInRequestVoteResponse(std::string_view buf) {
  const void* vbuf = static_cast<const void*>(buf.data());
  const RequestVoteRPCReply* reply = GetRequestVoteRPCReply(vbuf);

  if (!reply) {
    return false;
  }
  if (reply && reply->version() != 66) {
    return false;
  }
  return reply && reply->vote_granted() == true;
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
  if (reply && reply->version() != 22) {
    return {};
  }
  return std::optional{std::make_tuple(reply->success(), reply->id(), reply->term())};
}