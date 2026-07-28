CXX = g++
CXXFLAGS = -ggdb -std=c++26 -Wall -Wextra -Wunreachable-code -fsanitize=address
INCLUDES = -I./flatbuffers/include 

raft-runner : main.cpp
			$(CXX) $(CXXFLAGS) $(INCLUDES) main.cpp -o raft-runner

raft_service.o : raft_service.cpp raft_state.h
			$(CXX) $(CXXFLAGS) $(INCLUDES) -c raft_service.cpp 


command.o  : command.cpp command.h
			$(CXX) $(CXXFLAGS) $(INCLUDES) -c command.cpp 

server.o : server.cpp raft_service.o command.o server.h raft_state.h server_info.h
			$(CXX) $(CXXFLAGS) $(INCLUDES) -c server.cpp

server : server.cpp raft_service.o command.o server.h raft_state.h server_info.h
			$(CXX) $(CXXFLAGS) $(INCLUDES) server.cpp raft_service.o command.o -o server

client : client.cpp
			$(CXX) $(CXXFLAGS) $(INCLUDES) client.cpp command.o -o client
clean :
	rm -rf raft-runner server client *.o 
