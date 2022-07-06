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
#include <concurrentqueue.h>    
#include "proto/nezha-proto.pb.h"
#include "lib/utils.h"
#include "lib/udp_socket_endpoint.h"
#include "lib/tcp_socket_endpoint.h"

namespace nezha {
    using namespace nezha::proto;
    template<typename T1> using ConcurrentQueue = moodycamel::ConcurrentQueue<T1>;
    template<typename T1, typename T2> using ConcurrentMap = junction::ConcurrentMap_Leapfrog<T1, T2>;

    struct Context
    {
        Endpoint* endPoint_;
        uint32_t channeldId_; // The dest IP and port
        EndpointMsgHandler* msgHandler_;
        EndpointTimer* monitorTimer_;
        Context(Endpoint* ep = NULL, uint32_t cid = 0,
            EndpointMsgHandler* m = NULL, EndpointTimer* t = NULL)
            :endPoint_(ep), channeldId_(cid), msgHandler_(m), monitorTimer_(t) {}
        void Register(uint32_t endpointType = 1) {
            endPoint_->RegisterTimer(monitorTimer_);
            if (endpointType == 1) {
                // UDP Endpoint
                ((UDPSocketEndpoint*)endPoint_)->RegisterMsgHandler((UDPMsgHandler*)msgHandler_);
            }
            else if (endpointType == 2) {
                // TCP Endpoint
                ((TCPSocketEndpoint*)endPoint_)->RegisterMsgHandler(
                    channeldId_, (TCPMsgHandler*)msgHandler_);
            }
            else {
                LOG(ERROR) << "unknown endpoint type " << endpointType;
            }
        }
    };


    class Replica
    {
    private:
        YAML::Node replicaConfig_;
        uint32_t endPointType_; // 1 for UDP, 2 for TCP
        /** viewId_ starts from 0 */
        std::atomic<uint32_t> viewId_;
        std::atomic<uint32_t> lastNormalView_;
        /** replicaId_ starts from 0 */
        std::atomic<uint32_t> replicaId_;
        std::atomic<uint32_t> replicaNum_;

        /** Worker threads check status_ to decide whether they should be blocked (for view change) */
        std::atomic<int> status_;
        /** earlyBuffer_ uses the pair <deadline, reqKey> as key. std::map will sort them in ascending order by default */
        std::map<std::pair<uint64_t, uint64_t>, RequestBody*> earlyBuffer_;
        /** lastReleasedEntryByKeys_ is used to support communativity, we record the last relased entry for each key. When new requests come, it compares with the last released entry in the same key */
        std::vector<std::pair<uint64_t, uint64_t>> lastReleasedEntryByKeys_;
        /** lateBuffer_ is only used by followers, the key is logId */
        ConcurrentMap<uint32_t, RequestBody*> lateBuffer_;
        /** Inverse Index: used (by followers) to look up request from lateBuffer with a reqKey.
         * reqKey is concated by clientId and reqId
        */
        ConcurrentMap<uint64_t, uint32_t> lateBufferReq2LogId_;
        std::atomic<uint32_t> maxLateBufferId_;


        /** log-id as the key */
        ConcurrentMap<uint32_t, LogEntry*> syncedEntries_;
        ConcurrentMap<uint32_t, LogEntry*> unsyncedEntries_;
        /** Inverse Index: find logId with a reqKey */
        ConcurrentMap<uint64_t, uint32_t> syncedReq2LogId_;
        ConcurrentMap<uint64_t, uint32_t> unsyncedReq2LogId_;

        /** maxSyncedLogId_ and minUnSyncedLogId_ combine to work as sync-point, and provide convenience for garbage-collection */
        std::atomic<uint32_t> maxSyncedLogId_;
        std::atomic<uint32_t> maxUnSyncedLogId_;
        /** minUnSyncedLogId_ will be used to reduce state transfer amount */
        uint32_t minUnSyncedLogId_;

        /**  keyNum_ indicates the number of keys that requests will work on (to support commutativity optimization). We assume one request will only work on one key */
        uint32_t keyNum_;
        /** maxSyncedLogIdByKey_, minUnSyncedLogIdByKey_  and maxUnSyncedLogIdByKey_ are fine-grained version of maxSyncedLogId_, maxUnSyncedLogIdByKey_ and minUnSyncedLogId_, to support commutativity optimization */
        std::vector<uint32_t> maxSyncedLogIdByKey_;
        std::vector<uint32_t> maxUnSyncedLogIdByKey_;
        std::vector<uint32_t> minUnSyncedLogIdByKey_;


        /** Each thread is given a unique name (key) */
        std::map<std::string, std::thread*> threadPool_;

        /** SlowReplyTd(s) track the maxSyncedLogId to advance  slowRepliedLogId_ */
        std::vector<uint32_t> slowRepliedLogId_;
        /** committedLogId_ and toCommitLogId_ are used for peridical synchronization (to accelerate failure recovery) */
        std::atomic<uint32_t> committedLogId_;
        std::atomic<uint32_t> toCommitLogId_;
        uint32_t duplicateNum_; // For debug, will be deleted 
        uint32_t duplicateNum1_;
        uint32_t duplicateNum2_;

        /** Context (including a message handler and a monitor timer) */
        Context masterContext_;
        std::vector<Context> requestContext_;
        Context indexSyncContext_;
        Context missedIndexAckContext_;
        Context missedReqAckContext_;
        /** Timers */
        EndpointTimer* heartbeatCheckTimer_;
        EndpointTimer* indexAskTimer_;
        EndpointTimer* requestAskTimer_;
        EndpointTimer* viewChangeTimer_;
        EndpointTimer* stateTransferTimer_;
        EndpointTimer* periodicSyncTimer_;
        EndpointTimer* crashVectorRequestTimer_;
        EndpointTimer* recoveryRequestTimer_;
        /** Endpoints */
        std::vector<Endpoint*> indexSender_;
        std::vector<Endpoint*> fastReplySender_;
        std::vector<Endpoint*> slowReplySender_;
        Endpoint* indexRequster_;
        Endpoint* reqRequester_;
        Endpoint* indexAcker_;
        Endpoint* reqAcker_;
        /** Addresses */
        std::vector<Address*> indexReceiver_;
        std::vector<Address*> indexAskReceiver_;
        std::vector<Address*> requestAskReceiver_;
        std::vector<Address*> masterReceiver_;

        /* Round robin indices are used to achieve load balance among threads of the same functionality (e.g., multiple reply threads) */
        uint32_t roundRobinProcessIdx_;
        uint32_t roundRobinIndexAskIdx_;
        uint32_t roundRobinRequestAskIdx_;

        /** Version-based CrashVector (version number as the key), to facilitate garbage-collection */
        ConcurrentMap<uint32_t, CrashVectorStruct*> crashVector_;
        /** Each related thread (i.e. fast reply threads + index recv thread + index ack thread) will hold an atomic pointer */
        std::atomic<CrashVectorStruct*>* crashVectorInUse_;
        /** The number of threads using crash vectors (i.e., the length of crashVectorInUse_) */
        uint32_t crashVectorVecSize_;

        /** The sync messages (for index sync process) which have not been processed */
        std::map<std::pair<uint32_t, uint32_t>, IndexSync> pendingIndexSync_;
        /** Each key in missedReqKeys_ indicating a request is missing on this replica */
        std::set<uint64_t> missedReqKeys_;

        /** Each pair indicates a segment of indices is missing during index sync process */
        std::pair<uint32_t, uint32_t> missedIndices_;

        /** The max number of indices/reqKeys/requests that can be carried in one stateTransfer message */
        uint32_t indexTransferBatch_;
        uint32_t requestKeyTransferBatch_;
        uint32_t requestTrasnferBatch_;

        /* State-Transfer related variables **/
        uint64_t stateTransferTimeout_;
        bool transferSyncedEntry_;
        /** key: the target replica to ask for requests; value: the segment <begin, end> of requests that will be transferred */
        std::map<uint32_t, std::pair<uint32_t, uint32_t>> stateTransferIndices_;
        std::function<void(void)> stateTransferCallback_;
        /** The max amount of time that the state transfer can last */
        std::uint64_t stateTransferTerminateTime_;
        /** If the state transfer cannot be completed within  stateTransferTerminateTime_, execute the following callback and terminate the state transfer */
        std::function<void(void)> stateTransferTerminateCallback_;
        std::vector<uint32_t> filterUnSyncedLogIds_;



        /** Used for log merge to build new logs. Key: <deadline, reqKey>; Value: <request, the number of remaining replicas containing this request>  */
        std::map<std::pair<uint64_t, uint64_t>, std::pair<RequestBody*, uint32_t>> requestsToMerge_;

        // Recovery related variables
        std::string nonce_;
        std::map<uint32_t, CrashVectorReply> crashVectorReplySet_;
        std::map<uint32_t, RecoveryReply> recoveryReplySet_;
        std::map<uint32_t, ViewChange> viewChangeSet_;
        std::map<uint32_t, SyncStatusReport> syncStatusSet_;

        /** Inserted by ReceiveTd, and looked up by FastReplyTd/SlowReplyTd */
        ConcurrentMap<uint64_t, Address*> proxyAddressMap_;

        /** Followers used lastHeartBeatTime_ to check whether it should issue view change */
        std::atomic<uint64_t> lastHeartBeatTime_;
        /** Replicas use it to check whether every worker thread has stopped */
        std::atomic<uint32_t> activeWorkerNum_;
        uint32_t totalWorkerNum_;
        /** To implement blocking mechanism, see BlockWhenStatusIsNot function */
        std::condition_variable waitVar_;
        std::mutex waitMutext_;

        /** To communicate between ReceiveTd and ProcessTd */
        ConcurrentQueue<RequestBody*> processQu_;
        /** To communinicate between ProcessTd and FastReplyTd */
        std::vector<ConcurrentQueue<LogEntry*>> fastReplyQu_;
        /** To communinicate between ProcessTd and SlowReplyTd */
        std::vector<ConcurrentQueue<LogEntry*>> slowReplyQu_;
        /** To communicate between ReceiveTd and OWDCalcTd (Transmit <proxyId, owd>) */
        ConcurrentQueue<std::pair<uint64_t, uint32_t>> owdQu_;
        /** Record the one-way delay for each proxy. Updated by OWDCalcTd, read by FastReplyTd/SlowReplyTd */
        ConcurrentMap<uint64_t, uint32_t> owdMap_;
        /** To window size used to estimate one-way delay */
        uint32_t slidingWindowLen_;
        std::map<uint64_t, std::vector<uint32_t>> slidingWindow_; // <proxyid, vec>
        std::map<uint64_t, uint64_t> owdSampleNum_;

        /** Garbage-Collection related variables */
        uint32_t reclaimTimeout_;
        /** The old versions of crash vectors in crashVector_ that can be reclaimed */
        uint32_t cvVersionToClear_;
        /**  GarbageCollectTd use prepareToClearLateBufferLogId_ to tell IndexSyncTd that it intends to clear the requests before this point [included]  */
        std::atomic<uint32_t> prepareToClearLateBufferLogId_;
        /**  GarbageCollectTd use prepareToClearLateBufferLogId_ to tell IndexSyncTd and FastReplyTd that it intends to clear the log entries before this point [included]  */
        std::atomic<uint32_t> prepareToClearUnSyncedLogId_;
        /** IndexSyncTd use safeToClearLateBufferLogId_ to tell GarbageCollectTd that it can safely clear the requests in late buffer up to this point [included] */
        std::atomic<uint32_t> safeToClearLateBufferLogId_;
        /** FastReplyTd(s) and IndexSyncTd use these atomic variables to tell GarbageCollectTd, telling that it can safely clear unsynced log entries up to this point [included] */
        std::atomic<uint32_t>* safeToClearUnSyncedLogId_;


        void CreateContext();
        void LaunchThreads();
        void EnterNewView();

        /** View Change (recovery) related */
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
        void RollbackToViewChange();
        void RollbackToRecovery();
        void RewindSyncedLogTo(uint32_t rewindPoint);


        /** Periodic Sync related */
        void SendSyncStatusReport();
        void SendCommit();


        /** Garbage-Collect related */
        void ReclaimStaleLogs();
        void PrepareNextReclaim();
        void ReclaimStaleCrashVector();

        /** Message handler */
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
        void ProcessRequest(const RequestBody* rb, const bool isSyncedReq = true, const bool sendReply = true);


        /** The interfaces to bridge specific applications with Nezha */
        std::string ApplicationExecute(const RequestBody& request);

        /** Tools */
        bool AmLeader();
        void BlockWhenStatusIsNot(char targetStatus);
        MessageHeader* CheckMsgLength(const char* msgBuffer, const int msgLen);
        /** Master thread can initiate view change, non-master threads only switch status to ViewChange  */
        bool CheckView(const uint32_t view, const bool isMaster = true);
        bool CheckCV(const uint32_t senderId, const google::protobuf::RepeatedField<uint32_t>& cv);
        bool Aggregated(const google::protobuf::RepeatedField<uint32_t>& cv);
        void TransferSyncedLog();
        void TransferUnSyncedLog();
        void MergeUnSyncedLog();
        void PrintConfig();
        /** Convert our self-defined message to proto message */
        void RequestBodyToMessage(const RequestBody& rb, RequestBodyMsg* rbMsg);

        /** Threads */
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

        /** Registered event functions */
        void ReceiveClientRequest(MessageHeader* msgHdr, char* msgBuffer, Address* sender);
        void ReceiveIndexSyncMessage(MessageHeader* msgHdr, char* msgBuffer);
        void ReceiveAskMissedReq(MessageHeader* msgHdr, char* msgBuffer);
        void ReceiveAskMissedIdx(MessageHeader* msgHdr, char* msgBuffer);
        void ReceiveMasterMessage(MessageHeader* msgHdr, char* msgBuffer);
        void AskMissedIndex();
        void AskMissedRequest();
        void CheckHeartBeat();

        void Run();
        void Terminate();
    };


}

#endif