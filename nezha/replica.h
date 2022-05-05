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

        std::set<std::pair<uint64_t, uint64_t>> earlyBuffer_; // <deadline, reqKey>
        std::vector<std::pair<uint64_t, uint64_t>> lastReleasedEntryByKeys_;


        // use <threadName> as the key (for search), <threadPtr, threadId> as the value
        std::map<std::string, std::thread*> threadPool_;

        // Context
        std::map<std::string, struct ev_loop*> evLoops_;
        std::map<std::string, struct ev_io*> evIOs_;
        std::map<std::string, struct ev_timer*> evTimers_;
        std::map<std::string, int> socketFds_;
        std::map<std::string, char*> receiverBuffers_;
        std::vector<char*> requestBuffers_;

        std::atomic<uint64_t> lastHeartBeatTime_; // for master to check whether it should issue view change
        std::atomic<uint32_t> workerCounter_; // for master to check whether everybody has stopped


        ConcurrentMap<uint64_t, Request*> requestMap_;
        ConcurrentMap<uint64_t, Reply*> replyMap_;
        ConcurrentQueue<uint64_t> processQu_;
        ConcurrentQueue<uint64_t> replyQus_[16];

        void CreateMasterContext();
        void CreateReceiverContext();
        void CreateContext();
        void LaunchThreads();
        void StartViewChange();
        bool AmLeader();
    public:
        Replica(const std::string& configFile = std::string("../configs/nezha-replica.config.json"));
        ~Replica();

        void ReceiveTd(int id = -1);
        void ProcessTd(int id = -1);
        void ReplyTd(int id = -1);
        void RequestReceive(int id = -1, int fd = -1);

        void Master();
        void MasterReceive(int fd = -1);
    };


}

#endif