#include <cerrno>
#include <csignal>
#include <cstdlib>
#include <cstdio>

#include <fstream>
#include <iostream>
#include <print>
#include <string>
#include <string_view>
#include <vector>

#include <unistd.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/wait.h>

constexpr int MAXCHILDREN = 12;

int total_children{};

pid_t children[MAXCHILDREN];

std::string ParseConfigFile(const char* file_name) {
    std::ifstream ifs{file_name};
    if (!ifs) {
        std::cerr << "bad fname\n";
        std::exit(EXIT_FAILURE);
    }

    std::string hosts;
    if (!(ifs >> hosts)) {
        std::cerr << "fname: can't read\n";
        std::exit(EXIT_FAILURE);
    }
    
    return hosts;
}

void termination_handler(int signal) {
    std::println("[main:termination_handler] send SIGINT to children");
    for (int i{};i<total_children;++i) {
        kill(children[i], SIGINT);
    }
    std::exit(EXIT_FAILURE);
}

int main(int argc, char** argv) {

    if (argc != 3) {
        std::cerr << "Usage:\n";
        std::cerr << "raft-runner <num-servers> <config-file-name>\n";
        return 1;    
    }

    const int num_servers{std::atoi(argv[1])};
    std::println("[main]: num_server={}", num_servers);

    std::string hosts{ParseConfigFile(argv[2])};

    static char* newargv[] = {
        "server", 
        argv[1],
        nullptr,
        hosts.data(),
        nullptr
    };
    static char* newenviron[] = {nullptr};

    std::vector<pid_t> childpids;
    for (int i{}; i<num_servers; ++i) {
        pid_t pid = fork();

        if (pid == -1) {
            perror("fork");
            std::exit(EXIT_FAILURE);
        }

        std::string sidx{std::to_string(i)};
        newargv[2] = sidx.data(); 
        if (pid == 0) {
            int ret = execve(newargv[0], newargv, newenviron);
            if (ret == -1) {
                perror("execve");
                std::exit(EXIT_FAILURE);
            }
        } else {
            std::println("[main] created process={}", pid);
            childpids.push_back(pid);
            children[total_children++] = pid;
        }
    }

    if (signal(SIGINT, termination_handler) == SIG_ERR) { 
      perror("signal");
      std::exit(EXIT_FAILURE);
    }
    if (signal(SIGTERM, termination_handler) == SIG_ERR) {
      perror("signal");
      std::exit(EXIT_FAILURE);
    }

    pid_t pid;
    int status;
    for (;;) {
      pid = waitpid (WAIT_ANY, &status, WNOHANG);
      if (pid == -1) {
        perror("waitpid)");
        std::exit(EXIT_FAILURE);
      }
    }    

    std::println("[main]: exit");
    return 0;
}
