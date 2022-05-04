#ifndef NEZHA_REPLICA_H
#define NEZHA_REPLICA_H

#include <stdio.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <ev.h>
#include <strings.h>
#include <chrono>
#include <unistd.h>
#include <arpa/inet.h>
#include <iostream>
#include <thread>
#include <vector>
#include "nezha/nezha-proto.pb.h"
#include "lib/json.hpp"

namespace nezha {

    class Replica
    {
    private:
        /* data */
        std::vector<std::thread> threadPool_;
    public:
        Replica(/* args */);
        ~Replica();
        void RequestReceive(int id);
        void RequestProcess(int id);
        void RequestReply(int id);
    };


}

#endif