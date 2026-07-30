#include "rand_util.h"

#include <random>

namespace {
std::uniform_int_distribution<> dist(150, 300);
std::random_device rd{};
std::mt19937 gen(rd());
}

int GetRandomDuration() { return dist(gen); }