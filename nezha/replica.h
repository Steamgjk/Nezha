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
#include <string>
#include <condition_variable>
#include <glog/logging.h>
#include <junction/ConcurrentMap_Leapfrog.h>
#include <yaml-cpp/yaml.h>
#include <boost/uuid/uuid.hpp>            
#include <boost/uuid/uuid_generators.hpp> 
#include <boost/uuid/uuid_io.hpp>         
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

        ConcurrentMap<uint32_t, Request*> lateBuffer_; // Only used by followers, <id, reqKey>, we can use reqKey to get the request from unsyncedRequestMap_
        ConcurrentMap<uint64_t, uint32_t> lateBufferReq2LogId_; // <reqKey, logId> Inverse Index, used (by follower) to look up request from lateBuffer. 
        std::atomic<uint32_t> maxLateBufferId_;
        std::atomic<uint32_t> minLateBufferId_; // Used by ProcessTd to signal garbage collection 
        std::atomic<uint32_t> validLateBufferId_; // used by IndexRecvTd to signal garbage collection

        std::vector<std::pair<uint64_t, uint64_t>> lastReleasedEntryByKeys_;

        ConcurrentMap<uint64_t, Request*> syncedRequestMap_; // <reqKey, request>
        ConcurrentMap<uint64_t, Request*> unsyncedRequestMap_; // <reqKey, request>
        ConcurrentMap<uint32_t, LogEntry*> syncedEntries_; // log-id as the key [accumulated hashes]
        ConcurrentMap<uint32_t, LogEntry*> unsyncedEntries_; // log-id as the key [accumulated hashes]
        ConcurrentMap<uint64_t, uint32_t> syncedReq2LogId_; // <reqKey, logId> (inverse index, used for duplicate check by leader)
        ConcurrentMap<uint64_t, uint32_t> unsyncedReq2LogId_; // <reqKey, logId> (inverse index, used for duplicate check by followers)
        std::atomic<uint32_t> validUnSyncedLogId_; //  used by IndexRecvTd to signal garbage collection // obsolete

        std::atomic<uint32_t>* unSyncedLogIdInUse_; // used by IndexRecvTd and FastReplyTd to signal garbage collection


        // These two (maxSyncedLogId_ and minUnSyncedLogId_) combine to work as sync-point, and provide convenience for garbage-collection
        std::atomic<uint32_t> maxSyncedLogId_;
        std::atomic<uint32_t> minUnSyncedLogId_;
        std::atomic<uint32_t> maxUnSyncedLogId_;
        // syncedLogIdByKey_ and unsyncedLogIdByKey_ are fine-grained version of maxSyncedLogId_ and minUnSyncedLogId_, to support commutativity optimization

        // sortedUnSyncedRequests_ will be used for state transfer during view change
        // std::map is not thread-safe, so it is managed by one single thread:
        // During normal processing, only GarbageCollectTd can update it
        // During view change, only masterTd will manage it
        std::map<std::pair<uint64_t, uint64_t>, Request*> sortedUnSyncedRequests_;
        std::vector<Request*> filteredUnSyncedRequests_;

        std::atomic<uint32_t> committedLogId_;

        //  To support commutativity optimization
        uint32_t keyNum_;
        std::vector<uint32_t> maxSyncedLogIdByKey_; // per-key based log-id
        std::vector<uint32_t> minUnSyncedLogIdByKey_; // per-key based log-id, together with  maxSyncedLogIdByKey_, they serve as the sync-points on followers
        std::vector<uint32_t> maxUnSyncedLogIdByKey_; // per-key based log-id


        ConcurrentMap<uint64_t, LogEntry*> syncedEntriesByKey_; // <(Key|Id(consecutive)), entry>
        ConcurrentMap<uint64_t, LogEntry*> unsyncedEntriesByKey_; // <(Key|Id), entry>

        // ConcurrentMap<uint64_t, uint32_t> unSyncedLogIdInUseByKey_; // used by follower's fast-reply thread, GarbageCollectTd will check it before deleting some entries  // obsolete

        // Use <threadName> as the key (for search), <threadPtr, threadId> as the value
        std::map<std::string, std::thread*> threadPool_;

        // Context
        Context masterContext_;
        std::vector<Context> requestContext_;
        Context indexSyncContext_;
        Context missedIndexAckContext_;
        Context missedReqAckContext_;
        TimerStruct* heartbeatCheckTimer_;
        TimerStruct* indexAskTimer_;
        TimerStruct* requestAskTimer_;
        TimerStruct* viewChangeTimer_;
        TimerStruct* stateTransferTimer_;
        TimerStruct* periodicSyncTimer_;
        TimerStruct* crashVectorRequestTimer_;
        TimerStruct* recoveryRequestTimer_;
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
        uint32_t roundRobinRequestProcessIdx_;
        uint32_t roundRobinIndexAskIdx_;
        uint32_t roundRobinRequestAskIdx_;
        ConcurrentMap<uint32_t, CrashVectorStruct*> crashVector_; // Version-based CrashVector, used for garbage-collection
        std::atomic<CrashVectorStruct*>* crashVectorInUse_;
        uint32_t crashVectorVecSize_;


        uint32_t indexRecvCVIdx_;
        uint32_t indexAckCVIdx_;
        std::map<std::pair<uint32_t, uint32_t>, IndexSync> pendingIndexSync_;
        std::set<uint32_t> missedReqKeys_; // Missed Request during index synchronization
        std::pair<uint32_t, uint32_t> missedIndices_; // Missed Indices during index synchronization

        uint32_t indexTransferBatch_;
        uint32_t requestKeyTransferBatch_;
        uint32_t requestTrasnferBatch_;

        uint64_t stateTransferTimeout_;
        std::uint64_t stateTransferTerminateTime_; // When it comes to thsi time, the state transfer is forced to be terminated
        bool transferSyncedEntry_;

        std::map<uint32_t, std::pair<uint32_t, uint32_t>> stateTransferIndices_; // <targetReplica, <logbegin, logend> >

        std::map<std::pair<uint64_t, uint64_t>, std::pair<Request*, uint32_t>> requestsToMerge_;


        std::function<void(void)> stateTransferCallback_;
        // There needs to be some mechanism to jump out of the case when the stateTransferTarget replica fails
        std::function<void(void)> stateTransferTerminateCallback_;

        std::string nonce_;
        std::map<uint32_t, CrashVectorReply> crashVectorReplySet_;
        std::map<uint32_t, RecoveryReply> recoveryReplySet_;
        std::map<uint32_t, ViewChange> viewChangeSet_;
        std::map<uint32_t, SyncStatusReport> syncStatusSet_;

        ConcurrentMap<uint64_t, Address*> proxyAddressMap_; // Inserted by receiver threads, and looked up by fast/slow reply threads

        std::atomic<uint64_t> lastHeartBeatTime_; // for master to check whether it should issue view change
        std::atomic<uint32_t> activeWorkerNum_; // for master to check whether everybody has stopped
        uint32_t totalWorkerNum_;
        std::condition_variable waitVar_; // for worker threads to block during view change
        std::mutex waitMutext_;

        ConcurrentQueue<Request*> processQu_;
        std::vector<ConcurrentQueue<LogEntry*>> fastReplyQu_;
        std::vector<ConcurrentQueue<LogEntry*>> slowReplyQu_;

        // OWD related variables
        uint32_t slidingWindowLen_;
        ConcurrentQueue<std::pair<uint64_t, uint32_t>> owdQu_; // <proxyId, owd>
        ConcurrentMap<uint64_t, uint32_t> owdMap_; // <proxyId, owd>

        std::map<uint64_t, std::vector<uint32_t>> slidingWindow_; // <proxyid, vec>
        std::map<uint64_t, uint64_t> owdSampleNum_;

        // Garbage-Collection related variables
        uint32_t cvVersionToClear_;
        uint32_t unsyncedLogIdToClear_;
        std::vector<uint32_t> unsyncedLogIdByKeyToClear_;

        void CreateContext();
        void LaunchThreads();
        void EnterNewView();
        void ResetContext();
        void InitiateViewChange(const uint32_t view);
        void BroadcastViewChange();
        void SendViewChangeRequest(const int toReplicaId);
        void SendViewChange();

        void InitiateRecovery();
        void BroadcastCrashVectorRequest();
        void BroadcastRecoveryRequest();
        void SendStartView(const int toReplicaId);
        void SendStateTransferRequest();
        void SendSyncStatusReport();
        void SendCommit();

        void RollbackToViewChange();
        void RollbackToRecovery();

        void SortUnSyncedRequests(uint32_t begin, uint32_t end);
        void ReclaimStaleLogs(uint32_t end);
        void ReclaimStaleCrashVector();

        void FilterUnSyncedRequests();
        void RewindSyncedLogTo(uint32_t rewindPoint);
        void BlockWhenStatusIsNot(char targetStatus);

        bool ProcessIndexSync(const IndexSync& idxSyncMsg);
        void ProcessViewChangeReq(const ViewChangeRequest& viewChangeReq);
        void ProcessViewChange(const ViewChange& viewChange);
        void ProcessStateTransferRequest(const StateTransferRequest& stateTransferReq);
        void ProcessStateTransferReply(const StateTransferReply& stateTransferRep);
        void ProcessStartView(const StartView& startView);
        void ProcessCrashVectorRequest(const CrashVectorRequest& request);
        void ProcessCrashVectorReply(const CrashVectorReply& reply);
        void ProcessRecoveryRequest(const RecoveryRequest& request);
        void ProcessRecoveryReply(const RecoveryReply& reply);
        void ProcessSyncStatusReport(const SyncStatusReport& report);
        void ProcessCommitInstruction(const CommitInstruction& commit);
        void ProcessRequest(const uint64_t deadline, const uint64_t reqKey, const Request& request, const bool isSyncedReq = true, const bool sendReply = true);



        std::string ApplicationExecute(const Request& request);
        bool AmLeader();
        MessageHeader* CheckMsgLength(const char* msgBuffer, const int msgLen);
        bool CheckViewAndCV(const uint32_t view, const google::protobuf::RepeatedField<uint32_t>& cv);
        bool CheckView(const uint32_t view);
        bool CheckCV(const uint32_t senderId, const google::protobuf::RepeatedField<uint32_t>& cv);
        bool Aggregated(const google::protobuf::RepeatedField<uint32_t>& cv);
        void TransferSyncedLog();
        void TransferUnSyncedLog();
        void MergeUnSyncedLog();
        void PrintConfig();

        // Threads
        void ReceiveTd(int id = -1);
        void ProcessTd(int id = -1);
        void FastReplyTd(int id = -1, int cvId = -1);
        void SlowReplyTd(int id = -1);
        void IndexSendTd(int id = -1, int cvId = -1);
        void IndexRecvTd();
        void MissedIndexAckTd();
        void MissedReqAckTd();
        void OWDCalcTd();
        void GarbageCollectTd();

    public:
        Replica(const std::string& configFile = std::string("../configs/nezha-replica-config.yaml"), bool isRecovering = false);
        ~Replica();

        void ReceiveClientRequest(char* msgBuffer, int msgLen, Address* sender, UDPSocketEndpoint* receiverEP);
        void ReceiveIndexSyncMessage(char* msgBuffer, int msgLen);
        void ReceiveAskMissedReq(char* msgBuffer, int msgLen);
        void ReceiveAskMissedIdx(char* msgBuffer, int msgLen);
        void ReceiveMasterMessage(char* msgBuffer, int msgLen, Address* sender, UDPSocketEndpoint* receiverEP);
        void AskMissedIndex();
        void AskMissedRequest();
        void CheckHeartBeat();

        void Run();
        void Terminate();
    };


}

#endif