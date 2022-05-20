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
#include <yaml-cpp/yaml.h>
#include "nezha/nezha-proto.pb.h"
#include "lib/utils.hpp"
#include "lib/concurrentqueue.hpp"
#include "lib/udp_socket_endpoint.h"

namespace nezha {
    using namespace nezha::proto;
    template<typename T1> using ConcurrentQueue = moodycamel::ConcurrentQueue<T1>;
    template<typename T1, typename T2> using ConcurrentMap = junction::ConcurrentMap_Leapfrog<T1, T2>;

    class Replica
    {
    private:
        YAML::Node replicaConfig_;

        std::atomic<uint32_t> viewNum_;
        std::atomic<uint32_t> replicaId_;
        std::atomic<uint32_t> replicaNum_;
        std::atomic<uint32_t> status_; // Worker threads check status to decide whether they should stop (for view change)

        std::map<std::pair<uint64_t, uint64_t>, Request*> earlyBuffer_; // The key pair is <deadline, reqKey>
        std::vector<std::pair<uint64_t, uint64_t>> lastReleasedEntryByKeys_;

        ConcurrentMap<uint64_t, Request*> syncedRequestMap_; // <reqKey, request>
        ConcurrentMap<uint64_t, Request*> unsyncedRequestMap_; // <reqKey, request>
        ConcurrentMap<uint32_t, LogEntry*> syncedEntries_; // log-id as the key [accumulated hashes]
        ConcurrentMap<uint32_t, LogEntry*> unsyncedEntries_; // log-id as the key [accumulated hashes]
        ConcurrentMap<uint64_t, uint32_t> syncedReq2LogId_; // <reqKey, logId> (inverse index)
        ConcurrentMap<uint64_t, uint32_t> unsyncedReq2LogId_; // <reqKey, logId> (inverse index)

        // These two (maxSyncedLogId_ and minUnSyncedLogId_) combine to work as sync-point, and 
        // provide convenience for garbage-collection
        std::atomic<uint32_t> maxSyncedLogId_;
        std::atomic<uint32_t> minUnSyncedLogId_;
        std::atomic<uint32_t> maxUnSyncedLogId_;
        // syncedLogIdByKey_ and unsyncedLogIdByKey_ are fine-grained version of maxSyncedLogId_ and minUnSyncedLogId_, to support commutativity optimization
        std::vector<uint32_t> syncedLogIdByKey_;
        std::vector<uint32_t> unsyncedLogIdByKey_;

        // use <threadName> as the key (for search), <threadPtr, threadId> as the value
        std::map<std::string, std::thread*> threadPool_;

        // Context
        std::vector<UDPSocketEndpoint*> endPoints_;
        std::vector<std::pair<Replica*, int>> receiverEPContexts_;
        std::vector<void*> endPointContexts_; // Free them in deconstructor to avoid memory leak
        int masterEPIndex_;
        int reqReceiverEPIndex_;
        int indexSyncEPIndex_;
        int askIndexSyncEPIndex_;
        int askMissedReqEPIndex_;
        int indexSenderEPIndex_;
        int ackMissedReqEPIndex_;
        int ackMissedIdxEPIndex_;

        std::vector<in_addr_t> proxyIPs_;

        std::atomic<uint64_t> lastHeartBeatTime_; // for master to check whether it should issue view change
        std::atomic<uint32_t> workerCounter_; // for master to check whether everybody has stopped


        ConcurrentMap<uint64_t, Request*> requestMap_;
        ConcurrentMap<uint64_t, Reply*> fastReplyMap_;
        ConcurrentMap<uint64_t, Reply*> slowReplyMap_;
        ConcurrentQueue<Request*> processQu_;
        ConcurrentQueue<LogEntry*> fastReplyQus_[4];
        ConcurrentQueue<LogEntry*> slowReplyQus_[4];

        void CreateContext();
        void CreateMasterContext();
        void CreateReceiverContext();
        void CreateSenderContext();
        void CreateContext();
        void LaunchThreads();
        void StartViewChange();
        std::string ApplicationExecute(Request* req);
        bool AmLeader();
        bool CheckViewAndCV();
    public:
        Replica(const std::string& configFile = std::string("../configs/nezha-replica.config.json"));
        ~Replica();

        void ReceiveClientRequest(int id = -1, char* msgBuffer, int msgLen);
        void ReceiveIndexSyncMessage(char* msgBuffer, int msgLen);
        void ReceiveAskMissedReq(char* msgBuffer, int msgLen);
        void ReceiveAskMissedIdx(char* msgBuffer, int msgLen);

        void ReceiveTd(int id = -1);
        void ProcessTd(int id = -1);
        void FastReplyTd(int id = -1);
        void SlowReplyTd(int id = -1);
        void IndexSyncTd(int id = -1);
        void MissedIndexAckTd();
        void MissedReqAckTd();

        void FollowerIndexSyncReceive(int id = -1, int fd = -1);
        void LeaderIndexSyncReceive(int id = -1, int fd = -1);
        bool ProcessIndexSync(const IndexSync& idxSyncMsg);
        void RequestReceive(int id = -1, int fd = -1);

        void Master();
        void MasterReceive(int fd = -1);
    };


}

#endif