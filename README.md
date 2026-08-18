Implementation of the Raft Consensus Protocol: https://raft.github.io/raft.pdf

It's mostly in the proof-of-concept stage, however. 

TODO:
 - section 7: Log Compaction and InstallSnapshot RPC
 - code hardening, unit tests
 - some states could use a weaker memory model
 - remove some of the bare loops with STL algorithms
 - emulate InstallSnapshot to lagging followers 
 - bunch of -fsanitize={thread} warnings
 - replace makefile with autotools/CMake 
 - better per node logging mechanism, right now logs are to stdout/err