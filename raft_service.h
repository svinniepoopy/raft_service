#ifndef RAFT_SERVICE_H
#define RAFT_SERVICE_H

class RaftService {
  public:
    bool requestVote();
    bool appendEntries();
};

#endif // RAFT_SERVICE_H
