#ifndef SERVER_H
#define SERVER_H

#include "raft_state.h"
#include "server_info.h"

class RaftService;
class Command;

#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <vector>
#include <stop_token>
#include <thread>

struct Message {
  std::string msg;
  SenderInfo si;
};

struct CommandMessage : Message {
  CommandMessage(const Message& base, int numservers)
  : Message{base} {
      processed_entries.resize(numservers, false);
    }
  size_t num_replicated{1};
  std::vector<bool> processed_entries;
};

class Server {
public:
  Server(int /*cluster_size*/, int /*server_idx*/, std::string /*servers*/);
  ~Server();

private:
  // server main loop
  void start();

  // loops for each of the states
  State doFollowerLoop();
  State doCandidateLoop();
  State doLeaderLoop();

  void updateCommandQState(std::deque<CommandMessage> & /*command_q*/,
                           int /*id*/, int /*required_votes*/);

  // senders
  void sendRequestVote(const ServerInfo&);
  bool sendRequestVoteResponse(std::string_view /*request*/,
                               const SenderInfo&);

  void sendHeartBeat(size_t /*server_idx*/);
  void sendHeartBeatResponse(std::string_view /*request*/, const SenderInfo&);

  void sendAppendEntries(const CommandMessage&, size_t /*server_idx*/);
  void sendAppendEntriesResponse(std::string_view /*request*/,
                                 const SenderInfo&);

  void sendLeaderRedirect(const SenderInfo&);
  void startFollowerLogConsistencyThread(const SenderInfo&);

  std::condition_variable_any timer_cv_;
  std::mutex timer_mutex_;

  int port_;
  int sockfd_;

  int numservers_;
  std::vector<ServerInfo> servers_;

  std::unique_ptr<RaftService> praftservice_;
  std::unique_ptr<Command> pcommand_;
};

#endif // SERVER_H
