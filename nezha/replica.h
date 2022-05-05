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
#include <fstream>
#include <glog/logging.h>
#include <junction/ConcurrentMap_Leapfrog.h>
#include "nezha/nezha-proto.pb.h"
#include "lib/utils.hpp"
#include "lib/json.hpp"
#include "lib/concurrentqueue.hpp"

namespace nezha {
    using namespace nezha::proto;
    using json = nlohmann::json;
    template<typename T1> using ConcurrentQueue = moodycamel::ConcurrentQueue<T1>;
    template<typename T1, typename T2> using ConcurrentMap = junction::ConcurrentMap_Leapfrog<T1, T2>;
    enum REPLICA_STATUS {
        NORMAL = 1,
        VIEWCHANGE = 2,
        RECOVERING = 3
    };
    const uint32_t BUFFER_SIZE = 65535;

    class Replica
    {
    private:
        json replicaConfig_;
        std::atomic<uint32_t> viewNum_;
        std::atomic<uint32_t> replicaId_;
        std::atomic<uint32_t> replicaNum_;
        std::atomic<uint32_t> status_; // for workers to check whether they should stop (for view change)
        // use <threadName> as the key (for search), <threadPtr, threadId> as the value
        std::map<std::string, std::thread*> threadPool_;
        std::map<std::string, struct ev_loop*> evLoops_;
        std::atomic<uint64_t> lastHeartBeatTime_; // for master to check whether it should issue view change
        std::atomic<uint32_t> workerCounter_; // for master to check whether everybody has stopped
        struct ev_loop* masterLoop_;
        int masterSocketFd_;
        struct ev_io masterIO_;
        struct ev_timer masterTimer_;
        char masterBuffer[BUFFER_SIZE];

        ConcurrentMap<uint64_t, void*> requestMap_;
        ConcurrentMap<uint64_t, void*> replyMap_;
        void CreateContext();
        void LaunchThreads();
        void StartViewChange();
        bool AmLeader();
    public:
        Replica(const std::string& configFile = std::string("../configs/nezha-replica.config.json"));
        ~Replica();
        void RequestReceive(int id);
        void RequestProcess(int id);
        void RequestReply(int id);
        void Master();
        void MasterReceive();
    };


}

#endif