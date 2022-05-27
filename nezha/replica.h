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
#include "lib/utils.h"
#include "lib/concurrentqueue.hpp"
#include "lib/udp_socket_endpoint.h"

namespace nezha {
    using namespace nezha::proto;
    template<typename T1> using ConcurrentQueue = moodycamel::ConcurrentQueue<T1>;
    template<typename T1, typename T2> using ConcurrentMap = junction::ConcurrentMap_Leapfrog<T1, T2>;




    struct Context
    {
        UDPSocketEndpoint* endPoint_;
        MsgHandlerStruct* msgHandler_;
        TimerStruct* monitorTimer_;
        Context(UDPSocketEndpoint* ep = NULL, MsgHandlerStruct* m = NULL, TimerStruct* t = NULL) :endPoint_(ep), msgHandler_(m), monitorTimer_(t) {}
        void Register() {
            endPoint_->RegisterMsgHandler(msgHandler_);
            endPoint_->RegisterTimer(monitorTimer_);
        }
    };

    class Replica
    {
    private:
        YAML::Node replicaConfig_;

        std::atomic<uint32_t> viewId_;
        std::atomic<uint32_t> lastNormalView_;
        std::atomic<uint32_t> replicaId_;
        std::atomic<uint32_t> replicaNum_;
        std::atomic<int> status_; // Worker threads check status to decide whether they should stop (for view change)

        std::map<std::pair<uint64_t, uint64_t>, Request*> earlyBuffer_; // The key pair is <deadline, reqKey>
        std::vector<std::pair<uint64_t, uint64_t>> lastReleasedEntryByKeys_;

        ConcurrentMap<uint64_t, Request*> syncedRequestMap_; // <reqKey, request>
        ConcurrentMap<uint64_t, Request*> unsyncedRequestMap_; // <reqKey, request>
        ConcurrentMap<uint32_t, LogEntry*> syncedEntries_; // log-id as the key [accumulated hashes]
        ConcurrentMap<uint32_t, LogEntry*> unsyncedEntries_; // log-id as the key [accumulated hashes]
        ConcurrentMap<uint64_t, uint32_t> syncedReq2LogId_; // <reqKey, logId> (inverse index, used for duplicate check by leader)
        ConcurrentMap<uint64_t, uint32_t> unsyncedReq2LogId_; // <reqKey, logId> (inverse index, used for duplicate check by followers)

        // These two (maxSyncedLogId_ and minUnSyncedLogId_) combine to work as sync-point, and provide convenience for garbage-collection
        std::atomic<uint32_t> maxSyncedLogId_;
        std::atomic<uint32_t> minUnSyncedLogId_;
        std::atomic<uint32_t> maxUnSyncedLogId_;
        // syncedLogIdByKey_ and unsyncedLogIdByKey_ are fine-grained version of maxSyncedLogId_ and minUnSyncedLogId_, to support commutativity optimization
        std::atomic<uint32_t> committedLogId_;

        //  To support commutativity optimization
        std::vector<uint32_t> maxSyncedLogIdByKey_; // per-key based log-id
        std::vector<uint32_t> minUnSyncedLogIdByKey_; // per-key based log-id, starts with 2
        std::vector<uint32_t> maxUnSyncedLogIdByKey_; // per-key based log-id
        ConcurrentMap<uint64_t, LogEntry*> syncedEntriesByKey_; // <(Key|Id), entry>
        ConcurrentMap<uint64_t, LogEntry*> unsyncedEntriesByKey_; // <(Key|Id), entry>

        // Use <threadName> as the key (for search), <threadPtr, threadId> as the value
        std::map<std::string, std::thread*> threadPool_;

        // Context
        Context masterContext_;
        std::vector<Context> requestContext_;
        Context indexSyncContext_;
        Context missedIndexAckContext_;
        Context missedReqAckContext_;
        TimerStruct* indexAskTimer_;
        TimerStruct* requestAskTimer_;
        TimerStruct* viewChangeTimer_;
        TimerStruct* stateTransferTimer_;
        std::vector<UDPSocketEndpoint*> indexSender_;
        std::vector<UDPSocketEndpoint*> fastReplySender_;
        std::vector<UDPSocketEndpoint*> slowReplySender_;
        UDPSocketEndpoint* indexRequster_;
        UDPSocketEndpoint* reqRequester_;
        UDPSocketEndpoint* indexAcker_;
        UDPSocketEndpoint* reqAcker_;
        std::vector<Address*> indexReceiver_;
        std::vector<Address*> indexAskReceiver_;
        std::vector<Address*> requestAskReceiver_;
        std::vector<Address*> masterReceiver_;
        uint32_t roundRobinIndexAskIdx_;
        uint32_t roundRobinRequestAskIdx_;
        ConcurrentMap<uint32_t, CrashVectorStruct*> crashVector_; // Version-based CrashVector, used for garbage-collection
        std::atomic<CrashVectorStruct*>* crashVectorInUse_;
        uint32_t indexRecvCVIdx_;
        uint32_t indexAckCVIdx_;
        std::map<uint32_t, IndexSync> pendingIndexSync_;
        std::set<uint32_t> missedReqKeys_; // Missed Request during index synchronization
        std::pair<uint32_t, uint32_t> missedIndices_; // Missed Indices during index synchronization

        std::set < std::pair<uint32_t, uint32_t>> stateTransferIndices_;
        uint32_t stateTransferTargetReplica_;
        std::function<void(void)> stateTransferCallback_;
        // TODO: There needs to be some mechanism to jump out of the case when the stateTransferTarget replica fails

        std::map<uint32_t, ViewChange> viewChangeSet_;

        ConcurrentMap<uint64_t, Address*> proxyAddressMap_; // Inserted by receiver threads, and looked up by fast/slow reply threads

        std::atomic<uint64_t> lastHeartBeatTime_; // for master to check whether it should issue view change
        std::atomic<uint32_t> workerCounter_; // for master to check whether everybody has stopped


        ConcurrentMap<uint64_t, Request*> requestMap_;
        ConcurrentQueue<Request*> processQu_;
        std::vector<ConcurrentQueue<LogEntry*>> fastReplyQu_;
        std::vector<ConcurrentQueue<LogEntry*>> slowReplyQu_;

        void CreateContext();
        void LaunchThreads();
        void StartViewChange();
        void BroadcastViewChange();
        void SendViewChangeRequest(const int toReplicaId);
        void SendViewChange();
        void InitiateViewChange(const uint32_t view);
        void SendStartView(const int toReplicaId);
        void EnterNewView();

        bool ProcessIndexSync(const IndexSync& idxSyncMsg);
        void ProcessViewChangeReq(const ViewChangeRequest& viewChangeReq);
        void ProcessViewChange(const ViewChange& viewChange);
        void ProcessStateTransferReq(const StateTransferRequest& stateTransferReq);
        void ProcessStateTransferRep(const StateTransferReply& stateTransferRep);


        std::string ApplicationExecute(Request* req);
        bool AmLeader();
        MessageHeader* CheckMsgLength(const char* msgBuffer, const int msgLen);
        bool CheckViewAndCV(const uint32_t view, const google::protobuf::RepeatedField<uint32_t>& cv);
        bool CheckCV(const uint32_t senderId, const google::protobuf::RepeatedField<uint32_t>& cv);
        bool Aggregated(const google::protobuf::RepeatedField<uint32_t>& cv);
        void MergeLog();
        void MergeSyncedLog();
        void MergeUnSyncedLog();

    public:
        Replica(const std::string& configFile = std::string("../configs/nezha-replica.config.yaml"));
        ~Replica();

        void ReceiveClientRequest(char* msgBuffer, int msgLen, Address* sender, UDPSocketEndpoint* receiverEP);
        void ReceiveIndexSyncMessage(char* msgBuffer, int msgLen);
        void ReceiveAskMissedReq(char* msgBuffer, int msgLen);
        void ReceiveAskMissedIdx(char* msgBuffer, int msgLen);
        void ReceiveMasterMessage(char* msgBuffer, int msgLen, Address* sender, UDPSocketEndpoint* receiverEP);
        void AskMissedIndex();
        void AskMissedRequest();
        void CheckHeartBeat();

        void ReceiveTd(int id = -1);
        void ProcessTd(int id = -1);
        void FastReplyTd(int id = -1, int cvId = -1);
        void SlowReplyTd(int id = -1, int cvId = -1);
        void IndexSendTd(int id = -1, int cvId = -1);
        void IndexRecvTd();
        void MissedIndexAckTd();
        void MissedReqAckTd();
    };


}

#endif