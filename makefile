CXX = g++
CXXFLAGS = -std=c++26 -Wall -Wextra -Wunreachable-code
INCLUDES = -I./flatbuffers/include 

raft-runner : main.cpp server.o raft_service.o
			$(CXX) $(CXXFLAGS) $(INCLUDES) main.cpp server.o raft_service.o -o raft-runner

raft_service.o : raft_service.cpp raft_state.h
			$(CXX) $(CXXFLAGS) $(INCLUDES) -c raft_service.cpp 

server.o : server.cpp raft_service.o server.h raft_state.h server_info.h
			$(CXX) $(CXXFLAGS) $(INCLUDES) -c server.cpp

clean :
	rm -rf raft-runner *.o
