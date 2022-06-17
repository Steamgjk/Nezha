#include "nezha/replica.h"

namespace nezha {
#define GJK_DEBUG
#ifdef GJK_DEBUG
#define ASSERT(x) assert(x)
#else 
#define ASSERT(x) {}
#endif

    Replica::Replica(const std::string& configFile, bool isRecovering) :viewId_(0), lastNormalView_(0)
    {
        // Load Config
        replicaConfig_ = YAML::LoadFile(configFile);
        if (isRecovering) {
            status_ = ReplicaStatus::RECOVERING;
        }
        else {
            status_ = ReplicaStatus::NORMAL;
        }
        LOG(WARNING) << "Replica Status " << status_;
        PrintConfig();
        CreateContext();
        LOG(INFO) << "viewId_=" << viewId_
            << "\treplicaId=" << replicaId_
            << "\treplicaNum=" << replicaNum_
            << "\tkeyNum=" << keyNum_;
    }

    Replica::~Replica()
    {
        status_ = ReplicaStatus::TERMINATED;
        for (auto& kv : threadPool_) {
            delete kv.second;
            LOG(INFO) << "Deleted\t" << kv.first;
        }

        // TODO: A more elegant way is to reclaim or dump all logs before exit
        // For now, it is fine because all the memory is freed after the process is terminated
    }

    void Replica::Run() {
        // Master thread run
        masterContext_.Register();
        if (status_ == ReplicaStatus::RECOVERING) {
            masterContext_.endPoint_->RegisterTimer(crashVectorRequestTimer_);
        }
        else if (status_ == ReplicaStatus::NORMAL) {
            if (!AmLeader()) {
                masterContext_.endPoint_->RegisterTimer(heartbeatCheckTimer_);
            }
            // TODO: Register periodicSyncTimer_
        }
        // Launch worker threads (based on config)
        LaunchThreads();

        masterContext_.endPoint_->LoopRun();
        VLOG(2) << "Break LoopRun";

        // Wait until all threads return
        for (auto& kv : threadPool_) {
            VLOG(2) << "Joining " << kv.first;
            kv.second->join();
            VLOG(2) << "Join Complete \t" << kv.first;
        }
    }


    void Replica::PrintConfig() {
        if (replicaConfig_["print-config"].as<bool>()) {
            LOG(INFO) << "Replica IPs";
            for (uint32_t i = 0; i < replicaConfig_["replica-ips"].size(); i++) {
                LOG(INFO) << "\t" << replicaConfig_["replica-ips"][i].as<std::string>();
            }
            LOG(INFO) << "ReplicaID:" << replicaConfig_["replica-id"].as<int>();
            LOG(INFO) << "Request Receiver Shards:" << replicaConfig_["receiver-shards"].as<int>();
            LOG(INFO) << "Process Shards:" << replicaConfig_["process-shards"].as<int>();
            LOG(INFO) << "Reply Shards:" << replicaConfig_["reply-shards"].as<int>();
            LOG(INFO) << "Receiver Port:" << replicaConfig_["receiver-port"].as<int>();
            LOG(INFO) << "Index Sync Port:" << replicaConfig_["index-sync-port"].as<int>();
            LOG(INFO) << "(Missing) Request Ask Port:" << replicaConfig_["request-ask-port"].as<int>();
            LOG(INFO) << "(Missing) Index Ask Port:" << replicaConfig_["index-ask-port"].as<int>();
            LOG(INFO) << "Master Port:" << replicaConfig_["master-port"].as<int>();
            LOG(INFO) << "Monitor Period (ms):" << replicaConfig_["monitor-period-ms"].as<int>();
            LOG(INFO) << "Heartbeat Threshold (ms):" << replicaConfig_["heartbeat-threshold-ms"].as<int>();
            LOG(INFO) << "(Missing) Index Ask Period (ms):" << replicaConfig_["index-ask-period-ms"].as<int>();
            LOG(INFO) << "(Missing) Request Ask Period (ms):" << replicaConfig_["request-ask-period-ms"].as<int>();
            LOG(INFO) << "View Change Period (ms):" << replicaConfig_["view-change-period-ms"].as<int>();
            LOG(INFO) << "State Transfer Period (ms):" << replicaConfig_["state-transfer-period-ms"].as<int>();
            LOG(INFO) << "State Transfer Timeout (ms):" << replicaConfig_["state-transfer-timeout-ms"].as<int>();

            LOG(INFO) << "Index Transfer Max Batch:" << replicaConfig_["index-transfer-batch"].as<int>();
            LOG(INFO) << "(Missing) Request Key Ask Batch:" << replicaConfig_["request-key-transfer-batch"].as<int>();
            LOG(INFO) << "Request Transfer Max Batch:" << replicaConfig_["request-transfer-batch"].as<int>();

            LOG(INFO) << "Crash Vector Request Period (ms):" << replicaConfig_["crash-vector-request-period-ms"].as<int>();
            LOG(INFO) << "Recovery Request Period (ms):" << replicaConfig_["recovery-request-period-ms"].as<int>();
            LOG(INFO) << "Sync Report Period (ms):" << replicaConfig_["sync-report-period-ms"].as<int>();
            LOG(INFO) << "Key Num:" << replicaConfig_["key-num"].as<int>();
            LOG(INFO) << "Sliding Window (for owd estimation):" << replicaConfig_["owd-estimation-window"].as<uint32_t>();
            LOG(INFO) << "Reclaim Timeout (ms):" << replicaConfig_["reclaim-timeout-ms"].as<uint32_t>();
        }

    }

    void Replica::Terminate() {
        do {
            status_ = ReplicaStatus::TERMINATED;
            waitVar_.notify_all();
            // LOG(INFO) << "activeWorkerNum_=" << activeWorkerNum_;
        } while (activeWorkerNum_ > 0);
    }

    void Replica::CreateContext() {
        replicaId_ = replicaConfig_["replica-id"].as<int>();
        replicaNum_ = replicaConfig_["replica-ips"].size();
        keyNum_ = replicaConfig_["key-num"].as<uint32_t>();
        lastReleasedEntryByKeys_.resize(keyNum_, { 0ul,0ul });
        // Since ConcurrentMap reseres 0 and 1, we can only use log-id from 2
        maxSyncedLogId_ = CONCURRENT_MAP_START_INDEX - 1;
        minUnSyncedLogId_ = CONCURRENT_MAP_START_INDEX - 1;
        maxUnSyncedLogId_ = CONCURRENT_MAP_START_INDEX - 1;
        maxLateBufferId_ = CONCURRENT_MAP_START_INDEX - 1;
        maxSyncedLogIdByKey_.resize(keyNum_, CONCURRENT_MAP_START_INDEX - 1);
        minUnSyncedLogIdByKey_.resize(keyNum_, CONCURRENT_MAP_START_INDEX - 1); //
        maxUnSyncedLogIdByKey_.resize(keyNum_, CONCURRENT_MAP_START_INDEX - 1);
        unsyncedLogIdByKeyToClear_.resize(keyNum_, CONCURRENT_MAP_START_INDEX);
        committedLogId_ = CONCURRENT_MAP_START_INDEX - 1;


        // Create master endpoints and context
        std::string ip = replicaConfig_["replica-ips"][replicaId_.load()].as<std::string>();
        int port = replicaConfig_["master-port"].as<int>();
        int monitorPeriodMs = replicaConfig_["monitor-period-ms"].as<int>();

        masterContext_.endPoint_ = new UDPSocketEndpoint(ip, port, true);
        masterContext_.msgHandler_ = new MsgHandlerStruct([](char* msgBuffer, int bufferLen, Address* sender, void* ctx, UDPSocketEndpoint* receiverEP) {
            ((Replica*)ctx)->ReceiveMasterMessage(msgBuffer, bufferLen, sender, receiverEP);
            }, this);
        // Register a timer to monitor replica status
        masterContext_.monitorTimer_ = new TimerStruct([](void* ctx, UDPSocketEndpoint* receiverEP) {
            if (((Replica*)ctx)->status_ == ReplicaStatus::TERMINATED) {
                // Master thread will only break its loop when status comes to TERMINATED
                receiverEP->LoopBreak();
            }
            }, this, monitorPeriodMs);

        // Create request-receiver endpoints and context
        requestContext_.resize(replicaConfig_["receiver-shards"].as<int>());
        for (int i = 0; i < replicaConfig_["receiver-shards"].as<int>(); i++) {
            int port = replicaConfig_["receiver-port"].as<int>() + i;
            requestContext_[i].endPoint_ = new UDPSocketEndpoint(ip, port);
            // Register a request handler to this endpoint
            requestContext_[i].msgHandler_ = new MsgHandlerStruct([](char* msgBuffer, int bufferLen, Address* sender, void* ctx, UDPSocketEndpoint* receiverEP) {
                ((Replica*)ctx)->ReceiveClientRequest(msgBuffer, bufferLen, sender, receiverEP);
                }, this);
            // Register a timer to monitor replica status
            requestContext_[i].monitorTimer_ = new TimerStruct([](void* ctx, UDPSocketEndpoint* receiverEP) {
                if (((Replica*)ctx)->status_ != ReplicaStatus::NORMAL) {
                    receiverEP->LoopBreak();
                }
                }, this, monitorPeriodMs);
        }

        // (Leader) Use these endpoints to broadcast indices
        for (int i = 0; i < replicaConfig_["index-sync-shards"].as<int>(); i++) {
            indexSender_.push_back(new UDPSocketEndpoint());
        }
        indexAcker_ = new UDPSocketEndpoint();
        reqAcker_ = new UDPSocketEndpoint();
        indexRequster_ = new UDPSocketEndpoint();
        reqRequester_ = new UDPSocketEndpoint();
        for (uint32_t i = 0; i < replicaNum_; i++) {
            std::string ip = replicaConfig_["replica-ips"][i].as<std::string>();
            int indexPort = replicaConfig_["index-sync-port"].as<int>();
            indexReceiver_.push_back(new Address(ip, indexPort));
            int indexAskPort = replicaConfig_["index-ask-port"].as<int>();
            indexAskReceiver_.push_back(new Address(ip, indexAskPort));
            int requestAskPort = replicaConfig_["request-ask-port"].as<int>();
            requestAskReceiver_.push_back(new Address(ip, requestAskPort));
            int masterPort = replicaConfig_["master-port"].as<int>();
            masterReceiver_.push_back(new Address(ip, masterPort));
        }
        // (Followers:) Create index-sync endpoint to receive indices
        port = replicaConfig_["index-sync-port"].as<int>();
        indexSyncContext_.endPoint_ = new UDPSocketEndpoint(ip, port);
        // Register a msg handler to this endpoint
        indexSyncContext_.msgHandler_ = new MsgHandlerStruct([](char* msgBuffer, int bufferLen, Address* sender, void* ctx, UDPSocketEndpoint* receiverEP) {
            ((Replica*)ctx)->ReceiveIndexSyncMessage(msgBuffer, bufferLen);
            }, this);
        // Register a timer to monitor replica status
        indexSyncContext_.monitorTimer_ = new TimerStruct([](void* ctx, UDPSocketEndpoint* receiverEP) {
            if (((Replica*)ctx)->status_ != ReplicaStatus::NORMAL) {
                receiverEP->LoopBreak();
            }
            }, this, monitorPeriodMs);
        // Create an endpoint to handle others' requests for missed index
        port = replicaConfig_["index-ask-port"].as<int>();
        missedIndexAckContext_.endPoint_ = new UDPSocketEndpoint(ip, port);
        // Register message handler
        missedIndexAckContext_.msgHandler_ = new MsgHandlerStruct([](char* msgBuffer, int bufferLen, Address* sender, void* ctx, UDPSocketEndpoint* receiverEP) {
            ((Replica*)ctx)->ReceiveAskMissedIdx(msgBuffer, bufferLen);
            }, this);
        // Register a timer to monitor replica status
        missedIndexAckContext_.monitorTimer_ = new TimerStruct([](void* ctx, UDPSocketEndpoint* receiverEP) {
            if (((Replica*)ctx)->status_ != ReplicaStatus::NORMAL) {
                receiverEP->LoopBreak();
            }
            }, this, monitorPeriodMs);

        // Create an endpoint to handle others' requests for missed req
        port = replicaConfig_["request-ask-port"].as<int>();
        missedReqAckContext_.endPoint_ = new UDPSocketEndpoint(ip, port);
        // Register message handler
        missedReqAckContext_.msgHandler_ = new MsgHandlerStruct([](char* msgBuffer, int bufferLen, Address* sender, void* ctx, UDPSocketEndpoint* receiverEP) {
            ((Replica*)ctx)->ReceiveAskMissedReq(msgBuffer, bufferLen);
            }, this);
        // Register a timer to monitor replica status
        missedReqAckContext_.monitorTimer_ = new TimerStruct([](void* ctx, UDPSocketEndpoint* receiverEP) {
            if (((Replica*)ctx)->status_ != ReplicaStatus::NORMAL) {
                receiverEP->LoopBreak();
            }
            }, this, monitorPeriodMs);


        // Create reply endpoints
        int replyShardNum = replicaConfig_["reply-shards"].as<int>();
        for (int i = 0; i < replyShardNum;i++) {
            fastReplySender_.push_back(new UDPSocketEndpoint());
            slowReplySender_.push_back(new UDPSocketEndpoint());
        }
        // Create reply queues (one queue per fast/slow reply thread)
        fastReplyQu_.resize(replyShardNum);
        slowReplyQu_.resize(replyShardNum);

        // Create CrashVector Context
        std::vector<uint32_t> cvVec(replicaNum_, 0);
        CrashVectorStruct* cv = new CrashVectorStruct(cvVec, 2);
        crashVector_.assign(cv->version_, cv);
        crashVectorVecSize_ = 1 + replyShardNum + replicaConfig_["index-sync-shards"].as<int>() + 2;
        indexRecvCVIdx_ = crashVectorVecSize_ - 2;
        indexAckCVIdx_ = crashVectorVecSize_ - 1;
        crashVectorInUse_ = new std::atomic<CrashVectorStruct*>[crashVectorVecSize_];
        for (uint32_t i = 0; i < crashVectorVecSize_; i++) {
            crashVectorInUse_[i] = cv;
        }

        // Create other useful timers
        heartbeatCheckTimer_ = new TimerStruct([](void* ctx, UDPSocketEndpoint* receiverEP) {
            // Followers use this timer to check leader's heartbeat
            ((Replica*)ctx)->CheckHeartBeat();
            }, this, monitorPeriodMs);

        indexAskTimer_ = new TimerStruct([](void* ctx, UDPSocketEndpoint* receiverEP) {
            ((Replica*)ctx)->AskMissedIndex();
            }, this, replicaConfig_["index-ask-period-ms"].as<int>());
        roundRobinIndexAskIdx_ = 0;
        // Initially, no missed indices, so we make first > second
        missedIndices_ = { 1, 0 };

        requestAskTimer_ = new TimerStruct([](void* ctx, UDPSocketEndpoint* receiverEP) {
            ((Replica*)ctx)->AskMissedRequest();
            }, this, replicaConfig_["request-ask-period-ms"].as<int>());
        roundRobinRequestAskIdx_ = 0;
        missedReqKeys_.clear();

        viewChangeTimer_ = new TimerStruct([](void* ctx, UDPSocketEndpoint* receiverEP) {
            ((Replica*)ctx)->BroadcastViewChange();
            }, this, replicaConfig_["view-change-period-ms"].as<int>());
        roundRobinProcessIdx_ = 0;


        periodicSyncTimer_ = new TimerStruct([](void* ctx, UDPSocketEndpoint* receiverEP) {
            ((Replica*)ctx)->SendSyncStatusReport();
            }, this, replicaConfig_["sync-report-period-ms"].as<int>());


        requestTrasnferBatch_ = replicaConfig_["request-transfer-batch"].as<uint32_t>();
        indexTransferBatch_ = replicaConfig_["index-transfer-batch"].as<uint32_t>();
        requestKeyTransferBatch_ = replicaConfig_["request-key-transfer-batch"].as<uint32_t>();

        stateTransferTimer_ = new TimerStruct([](void* ctx, UDPSocketEndpoint* receiverEP) {
            ((Replica*)ctx)->SendStateTransferRequest();
            }, this, replicaConfig_["state-transfer-period-ms"].as<int>());

        stateTransferTimeout_ = replicaConfig_["state-transfer-timeout-ms"].as<uint64_t>();

        crashVectorRequestTimer_ = new TimerStruct([](void* ctx, UDPSocketEndpoint* receiverEP) {
            ((Replica*)ctx)->BroadcastCrashVectorRequest();
            }, this, replicaConfig_["crash-vector-request-period-ms"].as<int>());

        recoveryRequestTimer_ = new TimerStruct([](void* ctx, UDPSocketEndpoint* receiverEP) {
            ((Replica*)ctx)->BroadcastRecoveryRequest();
            }, this, replicaConfig_["recovery-request-period-ms"].as<int>());

        slidingWindowLen_ = replicaConfig_["owd-estimation-window"].as<uint32_t>();

        // Signal variable for garbage collection (of followers)
        reclaimTimeout_ = replicaConfig_["reclaim-timeout-ms"].as<uint32_t>();
        safeToClearUnSyncedLogId_ = new std::atomic<uint32_t>[replyShardNum + 1];
        safeToClearLateBufferLogId_ = CONCURRENT_MAP_START_INDEX - 1;
        for (int i = 0; i <= replyShardNum; i++) {
            safeToClearUnSyncedLogId_[i] = CONCURRENT_MAP_START_INDEX - 1;
        }
        prepareToClearLateBufferLogId_ = CONCURRENT_MAP_START_INDEX - 1;
        prepareToClearUnSyncedLogId_ = CONCURRENT_MAP_START_INDEX - 1;


    }

    void Replica::ResetContext() {
        // Clear queues 
        for (uint32_t i = 0; i < fastReplyQu_.size(); i++) {
            LogEntry* entry;
            while (fastReplyQu_[i].try_dequeue(entry)) {}
            while (slowReplyQu_[i].try_dequeue(entry)) {}
            // Don't worry about memory leakage, the memory pointed by these in-queue pointers have already been cleaned or will be cleaned according to their Conucrrent maps
        }
        Request* request;
        while (processQu_.try_dequeue(request)) {}

        // Clear UnSyncedLogs
        // Since ConcurrentMap reserves 0 and 1, we can only use log-id from 2
        minUnSyncedLogId_ = CONCURRENT_MAP_START_INDEX - 1;
        maxUnSyncedLogId_ = CONCURRENT_MAP_START_INDEX - 1;
        // Clear LateBuffer
        while (maxLateBufferId_ >= CONCURRENT_MAP_START_INDEX) {
            Request* request = lateBuffer_.get(maxLateBufferId_);
            if (request != NULL) {
                uint64_t reqKey = CONCAT_UINT32(request->clientid(), request->reqid());
                lateBufferReq2LogId_.erase(reqKey);
                if (syncedRequestMap_.get(reqKey) != request) {
                    delete request;
                }

                lateBuffer_.erase(maxLateBufferId_);
                maxLateBufferId_--;

            }
            else {
                break;
            }
        }

        maxLateBufferId_ = CONCURRENT_MAP_START_INDEX - 1;


        minUnSyncedLogIdByKey_.clear();
        maxUnSyncedLogIdByKey_.clear();
        unsyncedLogIdByKeyToClear_.clear();
        minUnSyncedLogIdByKey_.resize(keyNum_, CONCURRENT_MAP_START_INDEX - 1);
        maxUnSyncedLogIdByKey_.resize(keyNum_, CONCURRENT_MAP_START_INDEX - 1);
        unsyncedLogIdByKeyToClear_.resize(keyNum_, CONCURRENT_MAP_START_INDEX);
        committedLogId_.store(maxSyncedLogId_.load());

        // Reset lastReleasedEntryByKeys_, no need to care about UnSyncedLogs, because they are all cleared
        for (uint32_t key = 0; key < keyNum_; key++) {
            if (maxSyncedLogIdByKey_[key] < CONCURRENT_MAP_START_INDEX) {
                lastReleasedEntryByKeys_[key] = { 0ul, 0ul };
            }
            else {
                uint64_t opKeyAndId = key;
                opKeyAndId = ((opKeyAndId << 32u) | maxSyncedLogIdByKey_[key]);
                LogEntry* entry = syncedEntriesByKey_.get(opKeyAndId);
                ASSERT(entry != NULL);
                lastReleasedEntryByKeys_[key] = { entry->deadline, entry->reqKey };
            }
        }

        ConcurrentMap<uint32_t, LogEntry*>::Iterator iter(unsyncedEntries_);
        std::vector<uint64_t> keysToRemove;
        while (iter.isValid()) {
            keysToRemove.push_back(iter.getKey());
            iter.next();
        }
        for (auto& k : keysToRemove) {
            unsyncedEntries_.erase(k);
        }

        ConcurrentMap<uint64_t, LogEntry*>::Iterator iter2(unsyncedEntriesByKey_);
        keysToRemove.clear();
        while (iter2.isValid()) {
            keysToRemove.push_back(iter2.getKey());
            LogEntry* entry = iter2.getValue();
            delete entry;
            iter2.next();
        }
        for (auto& k : keysToRemove) {
            unsyncedEntries_.erase(k);
        }

        ConcurrentMap<uint64_t, uint32_t>::Iterator iter3(unsyncedReq2LogId_);
        keysToRemove.clear();
        while (iter3.isValid()) {
            keysToRemove.push_back(iter3.getKey());
            iter3.next();
        }
        for (auto& k : keysToRemove) {
            unsyncedReq2LogId_.erase(k);
        }

        ConcurrentMap<uint64_t, Request*>::Iterator iter4(unsyncedRequestMap_);
        keysToRemove.clear();
        while (iter4.isValid()) {
            keysToRemove.push_back(iter4.getKey());
            iter4.next();
            if (syncedRequestMap_.get(iter4.getKey()) == NULL) {
                // This request is useless, free it
                Request* request = iter4.getValue();
                delete request;
            }
        }
        for (auto& k : keysToRemove) {
            unsyncedRequestMap_.erase(k);
        }

        roundRobinIndexAskIdx_ = 0;
        // Initially, no missed indices, so we make first > second
        missedIndices_ = { 1, 0 };
        roundRobinRequestAskIdx_ = 0;
        missedReqKeys_.clear();
        roundRobinProcessIdx_ = 0;
        // Reset Index Sync related
        pendingIndexSync_.clear();

        stateTransferIndices_.clear();
        viewChangeSet_.clear();
        crashVectorReplySet_.clear();
        recoveryReplySet_.clear();
        syncStatusSet_.clear();

        // Reset OWD-Calc Related stuff
        slidingWindow_.clear();
        owdSampleNum_.clear();

        // Reset Master's timers 
        // No need to worry about other timers: worker thread will unregister their timers and msg handlers during LoopBreak
        masterContext_.endPoint_->UnRegisterAllTimers();
        masterContext_.endPoint_->RegisterTimer(masterContext_.monitorTimer_);
        if (!AmLeader()) {
            // Start checking leader's heartbeat from now on
            lastHeartBeatTime_ = GetMicrosecondTimestamp();
            masterContext_.endPoint_->RegisterTimer(heartbeatCheckTimer_);
        }
        // TODO: Register periodicSyncTimer_



        // Reset signal variable for garbage collection (of followers)
        safeToClearLateBufferLogId_ = CONCURRENT_MAP_START_INDEX - 1;
        for (uint32_t i = 0; i <= fastReplyQu_.size(); i++) {
            // The number of such counters is number of FastReplyTd_ + 1 (IndexRecv)
            safeToClearUnSyncedLogId_[i] = CONCURRENT_MAP_START_INDEX - 1;
        }
        prepareToClearLateBufferLogId_ = CONCURRENT_MAP_START_INDEX - 1;
        prepareToClearUnSyncedLogId_ = CONCURRENT_MAP_START_INDEX - 1;

    }

    void Replica::LaunchThreads() {
        activeWorkerNum_ = 0; // Dynamic variable, used as semphore
        totalWorkerNum_ = 0; // static variable to count number of workers
        // RequestReceive
        for (int i = 0; i < replicaConfig_["receiver-shards"].as<int>(); i++) {
            totalWorkerNum_++;
            std::thread* td = new std::thread(&Replica::ReceiveTd, this, i);
            std::string key("ReceiveTd-" + std::to_string(i));
            threadPool_[key] = td;
            LOG(INFO) << "Launched " << key << "\t" << td->native_handle();
        }

        // RequestProcess
        for (int i = 0; i < replicaConfig_["process-shards"].as<int>(); i++) {
            totalWorkerNum_++;
            std::thread* td = new std::thread(&Replica::ProcessTd, this, i);
            std::string key("ProcessTd-" + std::to_string(i));
            threadPool_[key] = td;
            LOG(INFO) << "Launched " << key << "\t" << td->native_handle();
        }

        // RequestReply
        int replyShardNum = replicaConfig_["reply-shards"].as<int>();
        for (int i = 0; i < replyShardNum; i++) {
            totalWorkerNum_++;
            std::thread* td = new std::thread(&Replica::FastReplyTd, this, i, i + 1);
            std::string key("FastReplyTd-" + std::to_string(i));
            threadPool_[key] = td;
            LOG(INFO) << "Launched " << key << "\t" << td->native_handle();
        }
        for (int i = 0; i < replyShardNum; i++) {
            totalWorkerNum_++;
            std::thread* td = new std::thread(&Replica::SlowReplyTd, this, i);
            std::string key("SlowReplyTd-" + std::to_string(i));
            threadPool_[key] = td;
            LOG(INFO) << "Launched " << key << "\t" << td->native_handle();
        }

        // IndexSync
        for (int i = 0; i < replicaConfig_["index-sync-shards"].as<int>(); i++) {
            totalWorkerNum_++;
            std::thread* td = new std::thread(&Replica::IndexSendTd, this, i, i + replyShardNum + 1);
            std::string key("IndexSendTd-" + std::to_string(i));
            threadPool_[key] = td;
            LOG(INFO) << "Launched " << key << "\t" << td->native_handle();
            if (!AmLeader()) {
                // follower only needs one sync thread
                break;
            }
        }

        totalWorkerNum_++;
        threadPool_["IndexRecvTd"] = new std::thread(&Replica::IndexRecvTd, this);
        LOG(INFO) << "Launched IndexRecvTd\t" << threadPool_["IndexRecvTd"]->native_handle();

        totalWorkerNum_++;
        threadPool_["MissedIndexAckTd"] = new std::thread(&Replica::MissedIndexAckTd, this);
        LOG(INFO) << "Launched MissedIndexAckTd\t" << threadPool_["MissedIndexAckTd"]->native_handle();

        totalWorkerNum_++;
        threadPool_["MissedReqAckTd"] = new std::thread(&Replica::MissedReqAckTd, this);
        LOG(INFO) << "Launched MissedReqAckTd\t" << threadPool_["MissedReqAckTd"]->native_handle();

        totalWorkerNum_++;
        threadPool_["GarbageCollectTd"] = new std::thread(&Replica::GarbageCollectTd, this);
        LOG(INFO) << "Launch  GarbageCollectTd " << threadPool_["GarbageCollectTd"]->native_handle();

        totalWorkerNum_++;
        threadPool_["OWDCalcTd"] = new std::thread(&Replica::OWDCalcTd, this);
        LOG(INFO) << "Launch  OWDCalcTd " << threadPool_["OWDCalcTd"]->native_handle();

        LOG(INFO) << "totalWorkerNum_=" << totalWorkerNum_;


    }

    void Replica::ReceiveClientRequest(char* msgBuffer, int msgLen, Address* sender, UDPSocketEndpoint* receiverEP) {
        if (!CheckMsgLength(msgBuffer, msgLen)) {
            return;
        }
        MessageHeader* msgHdr = (MessageHeader*)(void*)msgBuffer;
        if (msgHdr->msgType == MessageType::CLIENT_REQUEST) {
            Request* request = new Request();
            if (request->ParseFromArray(msgBuffer + sizeof(MessageHeader), msgHdr->msgLen)) {
                LOG(INFO) << "Request " << request->reqid() << " max " << maxSyncedLogId_;

                if (proxyAddressMap_.get(request->proxyid()) == 0) {
                    Address* addr = new Address(*sender);
                    // LOG(INFO) << "proxyId=" << request->proxyid() << "\taddr=" << addr->DecodeIP() << ":" << addr->DecodePort();
                    //  When one proxy sends the request, it needs to specify a proper *unique* proxyid related to one specific receiver thread on the replica, so that this replica's different receiver threads will not insert the same entry concurrently (otherwise, it may cause memory leakage) 
                    // In our proxy Implemention, each proxy machine has a unique id, with multiple shard. The machine-id concats shard-id becomes a unqiue proxy-id, modulo replica-shard-num and then send to the replica receiver 
                    proxyAddressMap_.assign(request->proxyid(), addr);
                }

                processQu_.enqueue(request);

                if (GetMicrosecondTimestamp() > request->sendtime()) {
                    owdQu_.enqueue(std::pair<uint32_t, uint32_t>(request->proxyid(), GetMicrosecondTimestamp() - request->sendtime()));
                }

            }
            else {
                LOG(WARNING) << "Parse request fail";
                delete request;
            }

        }
        else {
            LOG(WARNING) << "Invalid Message Type " << (uint32_t)(msgHdr->msgType);
        }

    }



    void Replica::BlockWhenStatusIsNot(char targetStatus) {
        if (status_ != targetStatus) {
            activeWorkerNum_.fetch_sub(1);
            std::unique_lock<std::mutex> lk(waitMutext_);
            waitVar_.wait(lk, [this, targetStatus] {
                if (status_ == ReplicaStatus::TERMINATED || status_ == targetStatus) {
                    // Unblock 
                    activeWorkerNum_.fetch_add(1);
                    return true;
                }
                else {
                    return false;
                }
                });
        }
    }


    void Replica::OWDCalcTd() {
        activeWorkerNum_.fetch_add(1);
        std::pair<uint32_t, uint64_t> owdSample;
        while (status_ != ReplicaStatus::TERMINATED) {
            BlockWhenStatusIsNot(ReplicaStatus::NORMAL);
            // if (viewId_ == 1) return;
            if (owdQu_.try_dequeue(owdSample)) {
                uint64_t proxyId = owdSample.first;
                uint32_t owd = owdSample.second;
                owdSampleNum_[proxyId]++;
                // LOG(INFO) << "proxyId=" << proxyId << "\t"
                //     << "owd=" << owd << "\t"
                //     << "sampleNum=" << owdSampleNum_[proxyId];

                if (slidingWindow_[proxyId].size() < slidingWindowLen_) {
                    slidingWindow_[proxyId].push_back(owd);
                }
                else {
                    slidingWindow_[proxyId][owdSampleNum_[proxyId] % slidingWindowLen_] = owd;
                }
                if (owdSampleNum_[proxyId] >= slidingWindowLen_) {
                    std::vector<uint32_t> tmpSamples(slidingWindow_[proxyId]);
                    sort(tmpSamples.begin(), tmpSamples.end());
                    uint32_t movingMedian = tmpSamples[slidingWindowLen_ / 2];
                    owdMap_.assign(proxyId, movingMedian);
                }
            }
        }
        uint32_t preVal = activeWorkerNum_.fetch_sub(1);
        VLOG(2) << "OWDCalcTd Terminated: " << preVal - 1 << " worker remaining";
    }

    void Replica::ReceiveTd(int id) {
        activeWorkerNum_.fetch_add(1);
        while (status_ != ReplicaStatus::TERMINATED) {
            BlockWhenStatusIsNot(ReplicaStatus::NORMAL);
            requestContext_[id].Register();
            requestContext_[id].endPoint_->LoopRun();
        }
        uint32_t preVal = activeWorkerNum_.fetch_sub(1);
        VLOG(2) << "ReceiveTd Terminated:" << preVal - 1 << " worker remaining";;
    }

    void Replica::ProcessTd(int id) {
        activeWorkerNum_.fetch_add(1);
        uint64_t reqKey = 0;
        Request* req = NULL;
        bool amLeader = AmLeader();
        while (status_ != ReplicaStatus::TERMINATED) {
            BlockWhenStatusIsNot(ReplicaStatus::NORMAL);
            // if (viewId_ == 1) return;
            if (processQu_.try_dequeue(req)) {
                reqKey = req->clientid();
                reqKey = ((reqKey << 32u) | (req->reqid()));
                // LOG(INFO) << "Deque " << req->reqid();
                if (amLeader) {
                    uint32_t duplicateLogIdx = syncedReq2LogId_.get(reqKey);
                    if (duplicateLogIdx == 0) {
                        // Not duplicate
                        uint64_t deadline = req->sendtime() + req->bound();
                        std::pair<uint64_t, uint64_t>myEntry(deadline, reqKey);
                        syncedRequestMap_.assign(reqKey, req);
                        if (myEntry > lastReleasedEntryByKeys_[req->key()]) {
                            earlyBuffer_[myEntry] = req;
                        }
                        else {
                            // req cannot enter early buffer
                            // Leader modifies its deadline
                            uint64_t newDeadline = lastReleasedEntryByKeys_[req->key()].first + 1;
                            std::pair<uint64_t, uint64_t>myEntry(newDeadline, reqKey);
                            earlyBuffer_[myEntry] = req;
                        }
                    }
                    else {
                        LOG(WARNING) << "duplicated " << duplicateLogIdx;
                        // at-most-once: duplicate requests are not executed twice
                        // We simply send the previous reply messages
                        LogEntry* entry = syncedEntries_.get(duplicateLogIdx);
                        // update proxy id in case the client has changed its proxy
                        entry->proxyId = req->proxyid();
                        fastReplyQu_[(roundRobinProcessIdx_++) % fastReplyQu_.size()].enqueue(entry);
                        // free this req
                        delete req;
                    }
                }
                else {
                    uint32_t duplicateLogIdx = syncedReq2LogId_.get(reqKey);
                    if (duplicateLogIdx > 0) {
                        // Duplicate: resend slow-reply for this request
                        LogEntry* entry = syncedEntries_.get(duplicateLogIdx);
                        entry->proxyId = req->proxyid();
                        slowReplyQu_[(roundRobinProcessIdx_++) % slowReplyQu_.size()].enqueue(entry);
                    }
                    else {
                        duplicateLogIdx = unsyncedReq2LogId_.get(reqKey);
                        if (duplicateLogIdx == 0) {
                            // Not duplicate
                            uint64_t deadline = req->sendtime() + req->bound();
                            std::pair<uint64_t, uint64_t>myEntry(deadline, reqKey);

                            if (myEntry > lastReleasedEntryByKeys_[req->key()]) {
                                unsyncedRequestMap_.assign(reqKey, req);
                                earlyBuffer_[myEntry] = req;
                            }
                            else {
                                // ELse, followers leave it in late buffer
                                // Also check to avoid duplication (memory leak)
                                if (lateBufferReq2LogId_.get(reqKey) >= CONCURRENT_MAP_START_INDEX) {
                                    LOG(INFO) << "here delete";
                                    delete req;
                                }
                                else {
                                    uint32_t logId = maxLateBufferId_ + 1;
                                    lateBuffer_.assign(logId, req);
                                    // Set the highest bit as 1, to differentiate from those unsynced requests which are accepted by the follower (i.e. go to the earlyBuffer)
                                    lateBufferReq2LogId_.assign(reqKey, logId);
                                    maxLateBufferId_++;
                                }
                            }
                        }
                        else {
                            LOG(WARNING) << "duplicated " << duplicateLogIdx;
                            // As a very rare case, if the request is duplicate but also unsynced, follower does not handle it, for fear of data race with other threads (e.g. GarbageCollectTd)
                            // But this will not harm liveness, because eventually the request will become synced request, so the former if-branch will handle it and resend a slow reply
                            LOG(INFO) << "here delete";
                            delete req;
                        }
                    }
                }

            }

            // Polling early-buffer
            uint64_t nowTime = GetMicrosecondTimestamp();

            while ((!earlyBuffer_.empty()) && nowTime <= earlyBuffer_.begin()->first.first) {
                uint64_t deadline = earlyBuffer_.begin()->first.first;
                uint64_t reqKey = earlyBuffer_.begin()->first.second;
                Request* req = earlyBuffer_.begin()->second;
                lastReleasedEntryByKeys_[req->key()] = { deadline, reqKey };
                // LOG(INFO) << "Processing " << req->reqid();
                ProcessRequest(deadline, reqKey, *req, AmLeader(), true);
                earlyBuffer_.erase(earlyBuffer_.begin());
            }

        }
        uint32_t preVal = activeWorkerNum_.fetch_sub(1);
        VLOG(2) << "ProcessTd Terminated: " << preVal - 1 << " worker remaining";;
    }

    void Replica::ProcessRequest(const uint64_t deadline, const uint64_t reqKey, const Request& request, const bool isSyncedReq, const bool sendReply) {
        SHA_HASH myHash = CalculateHash(deadline, reqKey);
        SHA_HASH hash = myHash;
        uint64_t opKeyAndId = 0ul;
        LogEntry* prev = NULL;
        LogEntry* entry = NULL;
        if (isSyncedReq) {
            // The log id of the previous non-commutative entry in the synced logs
            uint32_t prevLogId = maxSyncedLogIdByKey_[request.key()];
            if (prevLogId >= CONCURRENT_MAP_START_INDEX) {
                // There are previous (non-commutative) requests appended
                opKeyAndId = CONCAT_UINT32(request.key(), prevLogId);
                prev = syncedEntriesByKey_.get(opKeyAndId);
                ASSERT(prev != NULL);
                hash.XOR(prev->hash);
            }

            std::string result = ApplicationExecute(request);
            entry = new LogEntry(deadline, reqKey, myHash, hash, request.key(), prevLogId, UINT32_MAX, result, request.proxyid());
            uint32_t logId = maxSyncedLogId_ + 1;
            syncedEntries_.assign(logId, entry);
            syncedReq2LogId_.assign(reqKey, logId);
            // LOG(INFO) << "request.key=" << request.key() << "\t" << "size=" << maxSyncedLogIdByKey_.size();
            opKeyAndId = CONCAT_UINT32(request.key(), logId);
            syncedEntriesByKey_.assign(opKeyAndId, entry);
            if (prev != NULL) {
                prev->nextLogId = logId; // This is very important, to establish the connection based on key, making them look like a skiplist
            }

            maxSyncedLogIdByKey_[request.key()] = logId;
            maxSyncedLogId_++;
        }
        else {
            // The log id of the previous non-commutative entry in the unsynced logs
            uint32_t prevLogId = maxSyncedLogIdByKey_[request.key()];
            if (prevLogId >= CONCURRENT_MAP_START_INDEX) {
                // There are previous (non-commutative) requests appended
                opKeyAndId = CONCAT_UINT32(request.key(), prevLogId);
                LogEntry* prev = unsyncedEntriesByKey_.get(opKeyAndId);
                ASSERT(prev != NULL);
                hash.XOR(prev->hash);
            }
            entry = new LogEntry(deadline, reqKey, myHash, hash, request.key(), prevLogId, UINT32_MAX, "", request.proxyid());
            uint32_t logId = maxUnSyncedLogId_ + 1;
            unsyncedEntries_.assign(logId, entry);
            unsyncedReq2LogId_.assign(reqKey, logId);
            opKeyAndId = CONCAT_UINT32(request.key(), logId);
            unsyncedEntriesByKey_.assign(opKeyAndId, entry);
            if (prev != NULL) {
                prev->nextLogId = logId; // This is very important, see above
            }
            if (minUnSyncedLogIdByKey_[request.key()] < CONCURRENT_MAP_START_INDEX) {
                // ProcessTd only make once assignement for minUnSyncedLogIdByKey_
                // After switching it from the dummy value (CONCURRENT_MAP_START_INDEX-1) to a valid logId, the following update will all be charged by IndexRecvTd
                minUnSyncedLogIdByKey_[request.key()] = logId;
            }
            maxUnSyncedLogIdByKey_[request.key()] = logId;
            maxUnSyncedLogId_++;

        }
        if (sendReply) {
            fastReplyQu_[(roundRobinProcessIdx_++) % fastReplyQu_.size()].enqueue(entry);
        }
    }

    void Replica::FastReplyTd(int id, int cvId) {
        activeWorkerNum_.fetch_add(1);
        LogEntry* entry = NULL;
        Reply reply;
        reply.set_view(viewId_);
        reply.set_replytype(MessageType::FAST_REPLY);
        reply.set_replicaid(replicaId_);
        bool amLeader = AmLeader();
        CrashVectorStruct* cv = crashVectorInUse_[cvId];
        while (status_ != ReplicaStatus::TERMINATED) {
            BlockWhenStatusIsNot(ReplicaStatus::NORMAL);
            // if (viewId_ == 1) return;
            safeToClearUnSyncedLogId_[id].store(prepareToClearUnSyncedLogId_.load());
            // Before encoding crashVector into hash, check whether the crashVector (cv) is the freshest one
            CrashVectorStruct* masterCV = crashVectorInUse_[0].load();
            if (cv->version_ < masterCV->version_) {
                // My cv is stale, update it
                crashVectorInUse_[cvId] = masterCV;
                cv = masterCV;
            }
            if (fastReplyQu_[id].try_dequeue(entry)) {
                Address* addr = proxyAddressMap_.get(entry->proxyId);
                if (addr == NULL) {
                    // The replica cannot find the address to send reply
                    // This can happen in very trivial edge cases, e.g.,
                    // Step 1: This replica misses the entry 
                    // Step 2: The other replica gives it the missing entry
                    // Step 3: This replica has not received any entries from that proxy, so it does not have any addr info
                    // Step 4: This replica wants to send reply for this entry
                    LOG(ERROR) << "Cannot find the address of the proxy " << entry->proxyId;
                    continue;
                }

                reply.set_clientid((entry->reqKey) >> 32);
                reply.set_reqid((uint32_t)(entry->reqKey));
                reply.set_result(entry->result);
                // If the owdMap_ does not have the proxyId (i.e. the owd for this proxyId has not been estimated), it will return 0 (0 happens to be the dummy value of protobuf, and the proxy will not consider it as an estimated owd)
                reply.set_owd(owdMap_.get(entry->proxyId));
                SHA_HASH hash(entry->hash);
                hash.XOR(cv->cvHash_);
                if (amLeader) {
                    // Leader's logic is very easy: After XORing the crashVector and the log entry hash together, it can directly reply
                    reply.set_hash(hash.hash, SHA_DIGEST_LENGTH);
                    fastReplySender_[id]->SendMsgTo(*addr, reply, MessageType::FAST_REPLY);
                    // LOG(INFO) << "Leader reply=" << reply.reqid() << "\t"
                    //     << "opKey=" << entry->opKey << "\t"
                    //     << "hash=" << hash.toString();
                }
                else {
                    // But follower's hash is a bit complicated, because it needs to consider both synced entries and unsynced entries, i.e.
                    // We need to (1) eliminate the part to the left of sync-point
                    // and (2) use the remaining part (to the right of sync-point) to XOR the part that has already been synced 

                    // Let's first get the boundary, i.e. minUnSyncedLogId_ and maxSyncedLogId_
                    // maxSynced is always updated earlier than minUnSynced, so we first get minUnSynced, and then get maxSynced, this ensures minUnSynced is no fresher than maxSynced
                    // By contrast, if we get the two variables in the reverse order, then we cannot be sure which variable is fresher, that can lead to the missing of some entries during hash calculation
                    uint32_t unsyncedLogId = minUnSyncedLogIdByKey_[entry->opKey];// per-key based monotonously increasing
                    uint32_t syncedLogId = maxSyncedLogIdByKey_[entry->opKey];

                    if (syncedLogId < CONCURRENT_MAP_START_INDEX || unsyncedLogId < CONCURRENT_MAP_START_INDEX || unsyncedLogId <= safeToClearUnSyncedLogId_[id]) {
                        // The index sync process may have not been started, or may have not catch up;
                        // Or the unsynced logs have been reclaimed by GarbageCollectionTd (we have advanced safeToClearUnSyncedLogId_)
                        // We cannot decide the sync-point,
                        // so we directly reply with the XORed hash (similar to the leader)
                        reply.set_hash(hash.hash, SHA_DIGEST_LENGTH);
                        fastReplySender_[id]->SendMsgTo(*addr, reply, MessageType::FAST_REPLY);
                    }
                    else {
                        // The follower already gets some synced non-commutative logs (via index sync process)
                        LogEntry* syncedEntry = syncedEntries_.get(syncedLogId); // Get sync-point
                        ASSERT(syncedEntry != NULL);

                        // Log entries up to syncedEntry are all synced
                        // syncedEntry->hash represents them
                        if (entry->LessOrEqual(*syncedEntry)) {
                            // No need to send fast replies, because this entry has already been covered by index sync process, a slow reply has already been sent
                            continue;
                        }
                        // Beyond syncedEntry, we need to find the boundary in the unsynced logs
                        // We are sure that  unsyncedLogId >= CONCURRENT_MAP_START_INDEX
                        LogEntry* unsyncedEntry = unsyncedEntries_.get(unsyncedLogId);
                        ASSERT(unsyncedEntry != NULL);
                        // Since unsyncedLogId is no fresher (maybe stale) than syncedLogId, then unsyncedEntry may have already been surpasssed by syncedEntry, we need to remove the (potential) overlap
                        while (unsyncedEntry->LessOrEqual(*syncedEntry)) {
                            if (unsyncedEntry->nextLogId < UINT32_MAX) {
                                unsyncedEntry = unsyncedEntries_.get(unsyncedEntry->nextLogId);
                            }
                            else {
                                break;
                            }
                        }
                        // hash encodes all the (unsynced) entries up to entry
                        hash.XOR(unsyncedEntry->hash);
                        hash.XOR(unsyncedEntry->myhash);
                        // Now hash only encodes [unsyncedEntry, entry]
                        // Let's add the synced part
                        hash.XOR(syncedEntry->hash);
                        reply.set_hash(hash.hash, SHA_DIGEST_LENGTH);
                        fastReplySender_[id]->SendMsgTo(*addr, reply, MessageType::FAST_REPLY);
                    }
                }
            }
        }
        uint32_t preVal = activeWorkerNum_.fetch_sub(1);
        VLOG(2) << "Fast Reply Terminated " << preVal - 1 << " worker remaining";;
    }

    void Replica::SlowReplyTd(int id) {
        activeWorkerNum_.fetch_add(1);
        LogEntry* entry = NULL;
        Reply reply;
        reply.set_view(viewId_);
        reply.set_replytype(MessageType::SLOW_REPLY);
        reply.set_replicaid(replicaId_);
        reply.set_hash("");
        reply.set_result("");
        uint32_t repliedLogId = maxSyncedLogId_;
        while (status_ != ReplicaStatus::TERMINATED) {
            // LOG(INFO)<<"AmLeader? "<<AmLeader()<<" status="<<
            BlockWhenStatusIsNot(ReplicaStatus::NORMAL);
            if (viewId_ == 1) return;
            // LOG(INFO) << "AmLeader? " << AmLeader();
            if (AmLeader()) {
                // Leader does not send slow replies
                // LOG(INFO) << "Become Leader ";
                usleep(1000);
                continue;
            }
            entry = NULL;
            // SlowReplyTd get entries from two sources 
            // (1) (Mostly) IndexSync process will keep advancing maxSyncedLogId_, so that SlowReplyTd should send slow-replies for these entries prior to maxSyncedLogId_
            // (2) Some clients retry some requests, and the corresponding entries have already been synced with the leader, so SlowReplyTd also resend slow-replies for these entries

            // TODO: SlowReplyTd can be merged with FastReplyTd, just needs to piggyback the most recent synced-log-id on fast-reply, and that is enough for proxies to do quorum check, even without slow-reply messages 
            if (repliedLogId <= maxSyncedLogId_) {
                entry = unsyncedEntries_.get(repliedLogId);
                repliedLogId++;
            }
            else {
                slowReplyQu_[id].try_dequeue(entry);
            }
            if (entry) {
                reply.set_clientid((entry->reqKey) >> 32);
                reply.set_reqid((uint32_t)(entry->reqKey));
                // Optimize: SLOW_REPLY => COMMIT_REPLY
                if (syncedReq2LogId_.get(entry->reqKey) <= committedLogId_) {
                    reply.set_replytype(MessageType::COMMIT_REPLY);
                }
                reply.set_owd(owdMap_.get(entry->proxyId));

                Address* addr = proxyAddressMap_.get(entry->proxyId);
                if (addr != NULL) {
                    slowReplySender_[id]->SendMsgTo(*addr, reply, MessageType::SLOW_REPLY);
                    // LOG(INFO) << "Slow Reply " << reply.reqid() << "\t";
                }
            }
        }
        uint32_t preVal = activeWorkerNum_.fetch_sub(1);
        VLOG(2) << "SlowReplyTd Terminated " << preVal - 1 << " worker remaining";;
    }

    void Replica::IndexSendTd(int id, int cvId) {
        activeWorkerNum_.fetch_add(1);
        uint32_t lastSyncedLogId = maxSyncedLogId_;
        IndexSync indexSyncMsg;
        CrashVectorStruct* cv = crashVectorInUse_[cvId].load();
        while (status_ != ReplicaStatus::TERMINATED) {
            BlockWhenStatusIsNot(ReplicaStatus::NORMAL);
            if (viewId_ == 1) return;
            if (!AmLeader()) {
                // Although this replica is not leader currently
                // We still keep this thread. When it becomes the leader
                // we can immediately use the thread instead of launching (slow)
                usleep(1000);
                continue;
            }
            uint32_t logEnd = maxSyncedLogId_;
            if (lastSyncedLogId < logEnd
                || indexSyncMsg.sendtime() + 10000 < GetMicrosecondTimestamp()) {
                // (1) Leader has some indices to sync
                // (2) There is noting to send, but we still send an indexSync msg every 10ms (to serve as leader's heartbeat)
                indexSyncMsg.set_view(this->viewId_);
                indexSyncMsg.set_logidbegin(lastSyncedLogId + 1);
                logEnd = std::min(lastSyncedLogId + indexTransferBatch_, logEnd);
                indexSyncMsg.set_logidend(logEnd);
                indexSyncMsg.clear_deadlines();
                indexSyncMsg.clear_reqkeys();
                for (uint32_t i = indexSyncMsg.logidbegin(); i <= indexSyncMsg.logidend(); i++) {
                    LogEntry* entry = syncedEntries_.get(i);
                    ASSERT(entry != NULL);
                    indexSyncMsg.add_deadlines(entry->deadline);
                    indexSyncMsg.add_reqkeys(entry->reqKey);
                }

                CrashVectorStruct* masterCV = crashVectorInUse_[0].load();
                if (cv->version_ < masterCV->version_) {
                    crashVectorInUse_[cvId] = masterCV;
                    cv = masterCV;
                }

                // Add cv to broadcast
                indexSyncMsg.clear_cv();
                indexSyncMsg.mutable_cv()->Add(cv->cv_.begin(), cv->cv_.end());
                indexSyncMsg.set_sendtime(GetMicrosecondTimestamp());
                // Send
                LOG(INFO) << "Send " << indexSyncMsg.logidbegin() << "\t" << indexSyncMsg.logidend();
                for (uint32_t r = 0; r < replicaNum_; r++) {
                    if (r != replicaId_) {
                        indexSender_[id]->SendMsgTo(*(indexReceiver_[r]), indexSyncMsg, MessageType::SYNC_INDEX);
                    }
                }
                lastSyncedLogId = logEnd;
            }
            else {
                usleep(20);
            }

        }
        uint32_t preVal = activeWorkerNum_.fetch_sub(1);
        VLOG(2) << "IndexSendTd Terminated " << preVal - 1 << " worker remaining";;
    }

    void Replica::IndexRecvTd() {
        activeWorkerNum_.fetch_add(1);
        while (status_ != ReplicaStatus::TERMINATED) {
            BlockWhenStatusIsNot(ReplicaStatus::NORMAL);
            if (viewId_ == 1) return;
            indexSyncContext_.Register();
            indexSyncContext_.endPoint_->LoopRun();
        }
        uint32_t preVal = activeWorkerNum_.fetch_sub(1);
        VLOG(2) << "IndexRecvTd Terminated " << preVal - 1 << " worker remaining";
    }

    void Replica::ReceiveIndexSyncMessage(char* msgBuffer, int msgLen) {
        // Promise to the GarbageCollectTd, that I will not use the data before safeToClearLateBufferLogId_ and safeToClearUnSyncedLogId_, so that GarbageCollectTd can safely reclaim them
        safeToClearLateBufferLogId_.store(prepareToClearLateBufferLogId_.load());
        safeToClearUnSyncedLogId_[fastReplyQu_.size()].store(prepareToClearUnSyncedLogId_.load());

        if (!CheckMsgLength(msgBuffer, msgLen)) {
            return;
        }
        MessageHeader* msgHdr = (MessageHeader*)(void*)msgBuffer;
        char* buffer = msgBuffer + sizeof(MessageHeader);

        if (msgHdr->msgType == MessageType::SYNC_INDEX) {

            IndexSync idxSyncMsg;
            if (idxSyncMsg.ParseFromArray(buffer, msgHdr->msgLen)) {
                if (!CheckViewAndCV(idxSyncMsg.view(), idxSyncMsg.cv())) {
                    return;
                }
                LOG(INFO) << "begin end " << idxSyncMsg.logidbegin() << "\t" << idxSyncMsg.logidend();
                if (idxSyncMsg.logidend() <= maxSyncedLogId_) {
                    // useless message
                    return;
                }
                std::pair<uint32_t, uint32_t> key(idxSyncMsg.logidbegin(), idxSyncMsg.logidend());
                pendingIndexSync_[key] = idxSyncMsg;
                // Process pendingIndexSync, if any
                while (!pendingIndexSync_.empty()) {
                    if (ProcessIndexSync(pendingIndexSync_.begin()->second)) {
                        pendingIndexSync_.erase(pendingIndexSync_.begin());
                    }
                    else {
                        break;
                    }
                }
            }
        }
        else if (msgHdr->msgType == MessageType::MISSED_REQ) {
            MissedReq missedReqMsg;
            if (missedReqMsg.ParseFromArray(buffer, msgHdr->msgLen)) {
                for (int i = 0; i < missedReqMsg.reqs().size(); i++) {
                    uint64_t reqKey = missedReqMsg.reqs(i).clientid();
                    reqKey = ((reqKey << 32u) | missedReqMsg.reqs(i).reqid());
                    if (missedReqKeys_.find(reqKey) != missedReqKeys_.end()) {
                        Request* req = new Request(missedReqMsg.reqs(i));
                        // We must handle it to process td, to avoid data race (and further memroy leakage), although it is a trivial possibility
                        processQu_.enqueue(req);
                        missedReqKeys_.erase(reqKey);
                        if (missedReqKeys_.empty()) {
                            // can stop the timer now
                            indexSyncContext_.endPoint_->UnregisterTimer(requestAskTimer_);
                        }
                    }
                }
            }
        }
        else {
            LOG(WARNING) << "Unexpected msg type " << msgHdr->msgType;
        }

    }


    bool Replica::ProcessIndexSync(const IndexSync& idxSyncMsg) {
        LOG(INFO) << "Begin index begin " << idxSyncMsg.logidbegin() << "\t" << idxSyncMsg.logidend() << "\t maxSyncedLogId_=" << maxSyncedLogId_;
        if (idxSyncMsg.logidend() <= maxSyncedLogId_) {
            // This idxSyncMsg is useless 
            return true;
        }
        if (idxSyncMsg.logidbegin() > maxSyncedLogId_ + 1) {
            // Missing some indices
            missedIndices_ = { maxSyncedLogId_ + 1, idxSyncMsg.logidbegin() - 1 };
            if (indexSyncContext_.endPoint_->isRegistered(indexAskTimer_) == false) {
                // We are missing some idxSyncMsgs
               //  And We haven't launched the timer to ask indices
                indexSyncContext_.endPoint_->RegisterTimer(indexAskTimer_);
            }
            return false;
        }


        // idxSyncMsg.logidbegin() <= maxSyncedLogId + 1
        if (missedIndices_.second <= idxSyncMsg.logidend()) {
            // already fixed missing idices [Maybe]
            missedIndices_ = { 1, 0 };
            if (indexSyncContext_.endPoint_->isRegistered(indexAskTimer_)) {
                indexSyncContext_.endPoint_->UnregisterTimer(indexAskTimer_);
            }
        }
        else {
            missedIndices_.first = idxSyncMsg.logidend() + 1;
            if (indexSyncContext_.endPoint_->isRegistered(indexAskTimer_) == false) {
                indexSyncContext_.endPoint_->RegisterTimer(indexAskTimer_);
            }
        }
        LOG(INFO) << "index begin " << idxSyncMsg.logidbegin() << "\t" << idxSyncMsg.logidend() << "\t maxSyncedLogId_=" << maxSyncedLogId_;
        uint64_t opKeyAndId = 0ul;
        for (uint32_t logId = maxSyncedLogId_ + 1; logId <= idxSyncMsg.logidend(); logId++) {
            uint32_t offset = logId - idxSyncMsg.logidbegin();
            uint64_t reqKey = idxSyncMsg.reqkeys(offset);
            uint64_t deadline = idxSyncMsg.deadlines(offset);

            Request* req = NULL;
            uint32_t reqLogId = unsyncedReq2LogId_.get(reqKey);
            if (reqLogId >= CONCURRENT_MAP_START_INDEX
                && reqLogId > safeToClearUnSyncedLogId_[fastReplyQu_.size()]) {
                // We must ensure this logid is no smaller than unSyncedLogIdInUse_[fastReplyQu_.size()], otherwisxe, it may have been reclaimed
                req = unsyncedRequestMap_.get(reqKey);
                ASSERT(req != NULL);
            }

            if (req == NULL) {
                // cannot find the request in unsynced requests (early-buffer), then try to look up it in the late buffer
                uint32_t reqLogId = lateBufferReq2LogId_.get(reqKey);
                if (reqLogId >= CONCURRENT_MAP_START_INDEX
                    && reqLogId >= safeToClearLateBufferLogId_) {
                    // We are sure that GarbageCollectTd has not reclaim it
                    req = lateBuffer_.get(reqLogId);
                    ASSERT(req != NULL);
                }
            }

            if (req) {
                // Find the req locally
                // Copy a new request, do not use the previous req
                // This can make garbage collection easier
                SHA_HASH myHash = CalculateHash(deadline, reqKey);
                SHA_HASH hash(myHash);
                uint32_t prevLogId = 0;
                uint32_t opKey = req->key();
                if (maxSyncedLogIdByKey_[opKey] >= CONCURRENT_MAP_START_INDEX) {
                    // This request has some pre non-commutative ones
                    // In that way, XOR the previous accumulated hash
                    prevLogId = maxSyncedLogIdByKey_[opKey];
                    opKeyAndId = CONCAT_UINT32(opKey, prevLogId);
                    const SHA_HASH& prev = syncedEntriesByKey_.get(opKeyAndId)->hash;
                    hash.XOR(prev);
                }

                LogEntry* entry = new LogEntry(deadline, reqKey, myHash, hash, opKey, prevLogId, UINT32_MAX, "", req->proxyid());
                syncedRequestMap_.assign(reqKey, req);
                syncedReq2LogId_.assign(reqKey, logId);
                syncedEntries_.assign(logId, entry);
                opKeyAndId = CONCAT_UINT32(opKey, logId);
                syncedEntriesByKey_.assign(opKeyAndId, entry);
                maxSyncedLogIdByKey_[opKey] = logId;
                maxSyncedLogId_++;
                // Chunk UnSynced logs
                uint32_t minUnSyncedLogId = minUnSyncedLogIdByKey_[opKey];
                if (minUnSyncedLogIdByKey_[opKey] >= CONCURRENT_MAP_START_INDEX) {
                    // Try to advance  minUnSyncedLogIdByKey_[opKey]
                    LogEntry* unSyncedEntry = unsyncedEntries_.get(minUnSyncedLogId);
                    ASSERT(unSyncedEntry != NULL);
                    while (unSyncedEntry->LessOrEqual(*entry) && unSyncedEntry->nextLogId < UINT32_MAX) {
                        minUnSyncedLogId = unSyncedEntry->nextLogId;
                        unSyncedEntry = unsyncedEntries_.get(minUnSyncedLogId);
                        ASSERT(unSyncedEntry != NULL);
                    }
                    // LOG(INFO) << "minUnSyncedLogId=" << minUnSyncedLogId;
                    minUnSyncedLogIdByKey_[opKey] = minUnSyncedLogId;

                }

            }
            else {
                missedReqKeys_.insert(reqKey);
            }


        }
        if (missedReqKeys_.empty()) {
            if (indexSyncContext_.endPoint_->isRegistered(requestAskTimer_)) {
                indexSyncContext_.endPoint_->UnregisterTimer(requestAskTimer_);
            }
            return true;
        }
        else {
            // Start Timer ask for missing logs
            if (!indexSyncContext_.endPoint_->isRegistered(requestAskTimer_)) {
                indexSyncContext_.endPoint_->RegisterTimer(requestAskTimer_);
            }

            return false;
        }
    }

    void Replica::MissedIndexAckTd() {
        activeWorkerNum_.fetch_add(1);
        while (status_ != ReplicaStatus::TERMINATED) {
            BlockWhenStatusIsNot(ReplicaStatus::NORMAL);
            if (viewId_ == 1) return;
            missedIndexAckContext_.Register();
            missedIndexAckContext_.endPoint_->LoopRun();
        }
        uint32_t preVal = activeWorkerNum_.fetch_sub(1);
        VLOG(2) << "MissedIndexAckTd Terminated " << preVal - 1 << " worker remaining";
    }

    void Replica::ReceiveAskMissedIdx(char* msgBuffer, int msgLen) {
        if (NULL == CheckMsgLength(msgBuffer, msgLen)) {
            return;
        }
        AskIndex askIndex;

        if (msgBuffer[0] == MessageType::MISSED_INDEX_ASK && askIndex.ParseFromArray(msgBuffer + sizeof(MessageHeader), msgLen - sizeof(MessageHeader))) {
            uint32_t logBegin = askIndex.logidbegin();
            while (logBegin <= maxSyncedLogId_) {
                // This replica can help
                IndexSync indexSyncMsg;
                indexSyncMsg.set_view(this->viewId_);
                indexSyncMsg.set_logidbegin(logBegin);
                uint32_t logEnd = maxSyncedLogId_;
                if (logEnd > askIndex.logidend()) {
                    logEnd = askIndex.logidend();
                }
                if (logEnd > logBegin + indexTransferBatch_) {
                    logEnd = logBegin + indexTransferBatch_;
                }
                indexSyncMsg.set_logidend(logEnd);
                for (uint32_t logid = indexSyncMsg.logidbegin(); logid <= indexSyncMsg.logidend(); logid++) {
                    LogEntry* entry = syncedEntries_.get(logid);
                    indexSyncMsg.add_deadlines(entry->deadline);
                    indexSyncMsg.add_reqkeys(entry->reqKey);
                }
                // Piggyback crash vector
                crashVectorInUse_[indexAckCVIdx_] = crashVectorInUse_[0].load();
                CrashVectorStruct* cv = crashVectorInUse_[indexAckCVIdx_].load();
                indexSyncMsg.clear_cv();
                indexSyncMsg.mutable_cv()->Add(cv->cv_.begin(), cv->cv_.end());
                indexAcker_->SendMsgTo(*(indexReceiver_[askIndex.replicaid()]), indexSyncMsg, MessageType::SYNC_INDEX);
                logBegin = logEnd + 1;
            }
        }
    }

    void Replica::MissedReqAckTd() {
        activeWorkerNum_.fetch_add(1);
        while (status_ != ReplicaStatus::TERMINATED) {
            BlockWhenStatusIsNot(ReplicaStatus::NORMAL);
            if (viewId_ == 1) return;
            missedReqAckContext_.Register();
            missedReqAckContext_.endPoint_->LoopRun();
        }
        uint32_t preVal = activeWorkerNum_.fetch_sub(1);
        VLOG(2) << "MissedReqAckTd Terminated " << preVal - 1 << " worker remaining";
    }


    void Replica::ReceiveAskMissedReq(char* msgBuffer, int msgLen) {
        if (!CheckMsgLength(msgBuffer, msgLen)) {
            return;
        }
        AskReq askReqMsg;
        if (msgBuffer[0] == MessageType::MISSED_REQ_ASK && askReqMsg.ParseFromArray(msgBuffer + sizeof(MessageHeader), msgLen - sizeof(MessageHeader))) {
            MissedReq missedReqMsg;
            missedReqMsg.set_replicaid(this->replicaId_);
            for (int i = 0; i < askReqMsg.missedreqkeys_size(); i++) {
                uint64_t reqKey = askReqMsg.missedreqkeys(i);
                Request* req = syncedRequestMap_.get(reqKey);
                if (req == NULL) {
                    // Cannot find in syncedRequestMap_, try in unsyncedRequestMap_
                    req = unsyncedRequestMap_.get(reqKey);
                }
                // Don't try late buffer (GarbageCollectTd may be relclaiming it)
                if (req) {
                    // the req is found
                    missedReqMsg.add_reqs()->CopyFrom(*req);
                }
                if ((uint32_t)(missedReqMsg.reqs_size()) >= requestTrasnferBatch_) {
                    missedReqAckContext_.endPoint_->SendMsgTo(*(indexReceiver_[askReqMsg.replicaid()]), missedReqMsg, MessageType::MISSED_REQ);
                    missedReqMsg.clear_reqs();
                }
            }

            if (missedReqMsg.reqs_size() > 0) {
                // This ack is still useful, so send it
                missedReqAckContext_.endPoint_->SendMsgTo(*(indexReceiver_[askReqMsg.replicaid()]), missedReqMsg, MessageType::MISSED_REQ);
            }
        }
    }


    void Replica::AskMissedIndex() {
        if (missedIndices_.first > missedIndices_.second) {
            indexSyncContext_.endPoint_->UnregisterTimer(indexAskTimer_);
            return;
        }
        AskIndex askIndexMsg;
        askIndexMsg.set_replicaid(this->replicaId_);
        askIndexMsg.set_logidbegin(missedIndices_.first);
        askIndexMsg.set_logidend(missedIndices_.second);
        // Do not ask leader every time, choose random replica to ask to avoid leader bottleneck
        reqRequester_->SendMsgTo(*(requestAskReceiver_[roundRobinIndexAskIdx_ % replicaNum_]), askIndexMsg, MessageType::MISSED_INDEX_ASK);
        roundRobinIndexAskIdx_++;
        if (roundRobinIndexAskIdx_ % replicaNum_ == replicaId_) {
            roundRobinIndexAskIdx_++;
        }

    }

    void Replica::AskMissedRequest() {
        // TODO: Test and confirm ev_timer_again start immediately after register
        if (missedReqKeys_.empty()) {
            // no need to start timer
            indexSyncContext_.endPoint_->UnregisterTimer(requestAskTimer_);
            return;
        }
        AskReq askReqMsg;
        askReqMsg.set_replicaid(this->replicaId_);
        for (const uint64_t& reqKey : missedReqKeys_) {
            askReqMsg.add_missedreqkeys(reqKey);
            if ((uint32_t)(askReqMsg.missedreqkeys_size()) >= requestKeyTransferBatch_) {
                reqRequester_->SendMsgTo(*(requestAskReceiver_[roundRobinRequestAskIdx_ % replicaNum_]), askReqMsg, MessageType::MISSED_REQ_ASK);
                roundRobinRequestAskIdx_++;
                if (roundRobinRequestAskIdx_ % replicaNum_ == replicaId_) {
                    roundRobinRequestAskIdx_++;
                }
                askReqMsg.clear_missedreqkeys();
            }
        }
        if (askReqMsg.missedreqkeys_size() > 0) {
            reqRequester_->SendMsgTo(*(requestAskReceiver_[roundRobinRequestAskIdx_ % replicaNum_]), askReqMsg, MessageType::MISSED_REQ_ASK);
            roundRobinRequestAskIdx_++;
            if (roundRobinRequestAskIdx_ % replicaNum_ == replicaId_) {
                roundRobinRequestAskIdx_++;
            }
        }

    }


    void Replica::GarbageCollectTd() {
        activeWorkerNum_.fetch_add(1);
        while (status_ != ReplicaStatus::TERMINATED) {
            BlockWhenStatusIsNot(ReplicaStatus::NORMAL);
            if (viewId_ == 1) return;
            // // Reclaim stale crashVector
            // ReclaimStaleCrashVector();
            // // Reclaim (unsynced) stale logs
            // ReclaimStaleLogs();
            // // Check LateBuffer and UnSyncedLog items and try to advance prepareToClearLateBufferLogId_ and prepareToClearUnSyncedLogId_
            // PrepareNextReclaim();
        }
        uint32_t preVal = activeWorkerNum_.fetch_sub(1);
        VLOG(2) << "GarbageCollectTd Terminated " << preVal - 1 << " worker remaining";
    }


    void Replica::ReclaimStaleCrashVector() {
        uint32_t masterCVVersion = crashVectorInUse_[0].load()->version_;
        while (cvVersionToClear_ <= masterCVVersion) {
            bool canDelete = true;
            for (uint32_t i = 0; i < crashVectorVecSize_; i++) {
                if (crashVectorInUse_[i].load()->version_ <= cvVersionToClear_) {
                    canDelete = false;
                    break;
                }
            }
            if (canDelete) {
                CrashVectorStruct* cvToClear = crashVector_.get(cvVersionToClear_);
                crashVector_.erase(cvVersionToClear_);
                delete cvToClear;
                cvVersionToClear_++;
            }
            else {
                break;
            }

        }
    }



    void Replica::ReclaimStaleLogs() {
        uint32_t safePoint = prepareToClearUnSyncedLogId_;

        for (uint32_t shardIdx = 0; shardIdx < fastReplyQu_.size() + 1; shardIdx++) {
            safePoint = std::min(safePoint, safeToClearUnSyncedLogId_[shardIdx].load());
        }
        // Reclaim entries
        while (safePoint >= CONCURRENT_MAP_START_INDEX) {
            LogEntry* entryToClear = unsyncedEntries_.get(safePoint);
            if (entryToClear == NULL) {
                // This key has been cleared in previous round, we can stop here
                break;
            }
            // Reclaim its request
            Request* req = unsyncedRequestMap_.get(entryToClear->reqKey);
            if (syncedRequestMap_.get(entryToClear->reqKey) != req) {
                // This req is not reused by synced entries, so we can delete it
                delete req;
            }

            unsyncedRequestMap_.erase(entryToClear->reqKey);
            unsyncedReq2LogId_.erase(entryToClear->reqKey);
            unsyncedEntries_.erase(safePoint);

            safePoint--;

        }

        // Reclaim requests in late-buffer
        safePoint = safeToClearLateBufferLogId_;
        while (safePoint >= CONCURRENT_MAP_START_INDEX) {
            Request* req = lateBuffer_.get(safePoint);
            if (req == NULL) {
                // We have cleared enough, stop and jump out
                break;
            }
            uint64_t reqKey = CONCAT_UINT32(req->clientid(), req->reqid());
            lateBufferReq2LogId_.erase(reqKey);
            lateBuffer_.erase(safePoint);
            if (syncedRequestMap_.get(reqKey) != req) {
                // This req is not reused by synced entries, so we can delete it
                delete req;
            }
            safePoint--;
        }
    }

    void Replica::PrepareNextReclaim() {
        uint32_t lastPrepare = prepareToClearLateBufferLogId_;
        while (lastPrepare <= maxLateBufferId_ && lastPrepare >= CONCURRENT_MAP_START_INDEX) {
            Request* request = lateBuffer_.get(lastPrepare);
            ASSERT(request != NULL);
            if (request->sendtime() + reclaimTimeout_ * 1000 < GetMicrosecondTimestamp()) {
                // this request is too old, no need to keep it any longer
                lastPrepare++;
            }
            else {
                break;
            }
        }
        prepareToClearLateBufferLogId_ = lastPrepare;

        lastPrepare = prepareToClearUnSyncedLogId_;
        while (lastPrepare <= maxUnSyncedLogId_ && lastPrepare >= CONCURRENT_MAP_START_INDEX) {
            LogEntry* entry = unsyncedEntries_.get(lastPrepare);
            ASSERT(entry != NULL);
            bool shouldReclaim = false;
            uint32_t syncPoint = maxSyncedLogIdByKey_[entry->opKey];
            if (syncPoint >= CONCURRENT_MAP_START_INDEX) {
                LogEntry* syncedEntry = syncedEntries_.get(syncPoint);
                // syncedEntriesByKey_ Map may be unnecessary
                if (entry->LessOrEqual(*syncedEntry)) {
                    // synced entry has surpassed it, no need to keep it
                    shouldReclaim = true;
                }
            }
            if ((!shouldReclaim) && entry->deadline + reclaimTimeout_ * 1000 < GetMicrosecondTimestamp()) {
                // Even if synced entry has not surpassed it, it has been kept on the follwoers for too long
                // We assume reclaimTimeout_ is a sufficient time: If leader has such an entry, it should have synced it with the follower after such a long time
                // Otherwise, it only proves that the leader does not have such entry, so it is also unnecessary for the follower to keep it
                shouldReclaim = true;
            }
            if (shouldReclaim) {
                lastPrepare++;
            }
            else {
                break;
            }
        }
        prepareToClearUnSyncedLogId_ = lastPrepare;

    }


    void Replica::CheckHeartBeat() {
        if (status_ == ReplicaStatus::TERMINATED) {
            masterContext_.endPoint_->LoopBreak();
            return;
        }
        if (AmLeader()) {
            return;
        }
        // inject for debug
        if (viewId_ == 1) {
            return;
        }
        if (status_ != ReplicaStatus::NORMAL) {
            // Some worker threads have detected viewchange and switch status_ to VIEWCHANGE
            // But workers have no priviledge to increment viewId_ and initiate view change process, so the master will do that
            VLOG(2) << "InitiateViewChange-10";
            InitiateViewChange(viewId_ + 1);
            return;
        }
        uint64_t nowTime = GetMicrosecondTimestamp();
        uint32_t threashold = replicaConfig_["heartbeat-threshold-ms"].as<uint32_t>() * 1000;

        if (lastHeartBeatTime_ + threashold < nowTime) {
            // I haven't heard from the leader for too long, it probably has died
            // Before start view change, clear context
            VLOG(2) << "InitiateViewChange-1";
            InitiateViewChange(viewId_ + 1);
        }
    }

    void Replica::ReceiveMasterMessage(char* msgBuffer, int msgLen, Address* sender, UDPSocketEndpoint* receiverEP) {
        MessageHeader* msgHdr = CheckMsgLength(msgBuffer, msgLen);
        if (!msgHdr) {
            return;
        }
        VLOG(4) << "msgType " << (uint32_t)(msgHdr->msgType);

        if (msgHdr->msgType == MessageType::VIEWCHANGE_REQ) {
            ViewChangeRequest viewChangeReq;
            if (viewChangeReq.ParseFromArray(msgBuffer + sizeof(MessageHeader), msgHdr->msgLen)) {
                ProcessViewChangeReq(viewChangeReq);
            }

        }
        else if (msgHdr->msgType == MessageType::VIEWCHANGE) {
            ViewChange viewChangeMsg;
            if (viewChangeMsg.ParseFromArray(msgBuffer + sizeof(MessageHeader), msgHdr->msgLen)) {
                ProcessViewChange(viewChangeMsg);
            }

        }
        else if (msgHdr->msgType == MessageType::STATE_TRANSFER_REQUEST) {
            StateTransferRequest stateTransferReq;
            if (stateTransferReq.ParseFromArray(msgBuffer + sizeof(MessageHeader), msgHdr->msgLen)) {
                ProcessStateTransferRequest(stateTransferReq);
            }
        }
        else if (msgHdr->msgType == MessageType::STATE_TRANSFER_REPLY) {
            StateTransferReply stateTransferRep;
            if (stateTransferRep.ParseFromArray(msgBuffer + sizeof(MessageHeader), msgHdr->msgLen)) {
                ProcessStateTransferReply(stateTransferRep);
            }
        }
        else if (msgHdr->msgType == MessageType::START_VIEW) {
            StartView startView;
            if (startView.ParseFromArray(msgBuffer + sizeof(MessageHeader), msgHdr->msgLen)) {
                ProcessStartView(startView);
            }
        }
        else if (msgHdr->msgType == MessageType::CRASH_VECTOR_REQUEST) {
            CrashVectorRequest request;
            if (request.ParseFromArray(msgBuffer + sizeof(MessageHeader), msgHdr->msgLen)) {
                // LOG(INFO) << "request=" << request.DebugString();
                ProcessCrashVectorRequest(request);
            }
        }
        else if (msgHdr->msgType == MessageType::CRASH_VECTOR_REPLY) {
            CrashVectorReply reply;
            if (reply.ParseFromArray(msgBuffer + sizeof(MessageHeader), msgHdr->msgLen)) {
                LOG(INFO) << "CrashVectorReply = " << reply.DebugString();
                ProcessCrashVectorReply(reply);
            }
        }
        else if (msgHdr->msgType == MessageType::RECOVERY_REQUEST) {
            RecoveryRequest request;
            if (request.ParseFromArray(msgBuffer + sizeof(MessageHeader), msgHdr->msgLen)) {
                LOG(INFO) << "recovery request " << request.DebugString() << " status=" << status_;
                ProcessRecoveryRequest(request);
            }
        }
        else if (msgHdr->msgType == MessageType::RECOVERY_REPLY) {
            RecoveryReply reply;
            if (reply.ParseFromArray(msgBuffer + sizeof(MessageHeader), msgHdr->msgLen)) {
                ProcessRecoveryReply(reply);
            }
        }
        else if (msgHdr->msgType == MessageType::SYNC_STATUS_REPORT) {
            SyncStatusReport report;
            if (report.ParseFromArray(msgBuffer + sizeof(MessageHeader), msgHdr->msgLen)) {
                ProcessSyncStatusReport(report);
            }
        }
        else if (msgHdr->msgType == MessageType::COMMIT_INSTRUCTION) {
            CommitInstruction commit;
            if (commit.ParseFromArray(msgBuffer + sizeof(MessageHeader), msgHdr->msgLen)) {
                ProcessCommitInstruction(commit);
            }
        }

        else {
            LOG(WARNING) << "Unexpected message type " << (int)msgBuffer[0];
        }

    }

    void Replica::SendViewChangeRequest(const int toReplicaId) {
        ViewChangeRequest viewChangeReq;
        viewChangeReq.set_view(viewId_);
        viewChangeReq.set_replicaid(replicaId_);
        CrashVectorStruct* cv = crashVectorInUse_[0].load();
        viewChangeReq.mutable_cv()->Add(cv->cv_.begin(), cv->cv_.end());

        if (toReplicaId < 0) {
            // send to all
            for (uint32_t i = 0; i < replicaNum_; i++) {
                if (i != replicaId_) {
                    // no need to send to myself
                    masterContext_.endPoint_->SendMsgTo(*(masterReceiver_[i]), viewChangeReq, MessageType::VIEWCHANGE_REQ);
                }

            }
        }
        else {
            masterContext_.endPoint_->SendMsgTo(*(masterReceiver_[toReplicaId]), viewChangeReq, MessageType::VIEWCHANGE_REQ);
        }
    }

    void Replica::SendViewChange() {
        if (AmLeader()) {
            // I am the leader of this new view, no need to send to myself
            return;
        }
        ViewChange viewChangeMsg;
        viewChangeMsg.set_view(viewId_);
        viewChangeMsg.set_replicaid(replicaId_);
        CrashVectorStruct* cv = crashVectorInUse_[0].load();
        viewChangeMsg.mutable_cv()->Add(cv->cv_.begin(), cv->cv_.end());
        viewChangeMsg.set_syncpoint(maxSyncedLogId_);
        viewChangeMsg.set_unsynclogbegin(minUnSyncedLogId_);
        viewChangeMsg.set_unsynclogend(maxUnSyncedLogId_);
        viewChangeMsg.set_lastnormalview(lastNormalView_);
        masterContext_.endPoint_->SendMsgTo(*(masterReceiver_[viewId_ % replicaNum_]), viewChangeMsg, MessageType::VIEWCHANGE);

    }

    void Replica::InitiateViewChange(const uint32_t view) {
        if (viewId_ > view) {
            LOG(ERROR) << "Invalid view change initiation currentView=" << viewId_ << "\ttargetView=" << view;
            return;
        }

        if (viewId_ == view && status_ == ReplicaStatus::VIEWCHANGE) {
            // Already in viewchange
            return;
        }

        status_ = ReplicaStatus::VIEWCHANGE;
        VLOG(2) << "status =" << status_ << "\t"
            << " view=" << viewId_ << "\t"
            << " targeting view=" << view;

        // Wait until every worker stop
        while (activeWorkerNum_ > 0) {
            VLOG(3) << "activeWorker Check=" << activeWorkerNum_;
            usleep(1000);
        }

        // Update minUnSyncedLogId_, used for state transfer
        minUnSyncedLogId_ = UINT32_MAX;
        for (uint32_t i = 0; i <= fastReplyQu_.size(); i++) {
            if (minUnSyncedLogId_ > safeToClearUnSyncedLogId_[i] + 1) {
                minUnSyncedLogId_ = safeToClearUnSyncedLogId_[i] + 1;
            }
        }



        viewId_ = view;
        // Unregister all timers, except the monitorTimer (so as the master thread can break when status=Terminated)
        masterContext_.endPoint_->UnRegisterAllTimers();
        masterContext_.endPoint_->RegisterTimer(masterContext_.monitorTimer_);
        LOG(INFO) << "Monitor Timer Registered ";
        // Launch viewChange timer
        masterContext_.endPoint_->RegisterTimer(viewChangeTimer_);
    }

    void Replica::BroadcastViewChange() {
        if (status_ == ReplicaStatus::NORMAL) {
            // Can stop the timer 
            masterContext_.endPoint_->UnregisterTimer(viewChangeTimer_);
            return;
        }
        // Broadcast VIEW-CHANGE-REQ to all replicas
        SendViewChangeRequest(-1);
        // Send VIEW-CHANGE to the leader in this view
        SendViewChange();

    }

    void Replica::SendStartView(const int toReplicaId) {
        StartView startView;
        startView.set_replicaid(replicaId_);
        startView.set_view(viewId_);
        CrashVectorStruct* cv = crashVectorInUse_[0];
        startView.mutable_cv()->Add(cv->cv_.begin(), cv->cv_.end());
        startView.set_syncedlogid(maxSyncedLogId_);
        if (toReplicaId >= 0) {
            // send to one
            masterContext_.endPoint_->SendMsgTo(*(masterReceiver_[toReplicaId]), startView, MessageType::STATE_TRANSFER_REQUEST);
        }
        else {
            // send to all 
            for (uint32_t i = 0; i < replicaNum_; i++) {
                if (i == replicaId_) {
                    // No need to send to self
                    continue;
                }
                masterContext_.endPoint_->SendMsgTo(*(masterReceiver_[i]), startView, MessageType::START_VIEW);
                LOG(INFO) << "Send StartView to " << i << "\t" << masterReceiver_[i]->DecodeIP() << ":" << masterReceiver_[i]->DecodePort();
            }
        }

    }

    void Replica::SendSyncStatusReport() {
        SyncStatusReport report;
        report.set_view(viewId_);
        report.set_replicaid(replicaId_);
        CrashVectorStruct* cv = crashVectorInUse_[0].load();
        report.mutable_cv()->Add(cv->cv_.begin(), cv->cv_.end());
        report.set_syncedlogid(maxSyncedLogId_);
        if (AmLeader()) {
            // leader directly process its own report
            ProcessSyncStatusReport(report);
        }
        else {
            // send to leader
            masterContext_.endPoint_->SendMsgTo(*(masterReceiver_[viewId_ % replicaNum_]), report, MessageType::SYNC_STATUS_REPORT);
        }

    }

    void Replica::SendCommit() {
        CommitInstruction commit;
        commit.set_view(viewId_);
        commit.set_replicaid(replicaId_);
        CrashVectorStruct* cv = crashVectorInUse_[0].load();
        commit.mutable_cv()->Add(cv->cv_.begin(), cv->cv_.end());
        commit.set_committedlogid(committedLogId_);
        for (uint32_t i = 0; i < replicaNum_; i++) {
            if (i != replicaId_) {
                masterContext_.endPoint_->SendMsgTo(*(masterReceiver_[viewId_ % replicaNum_]), commit, MessageType::COMMIT_INSTRUCTION);
            }
        }

    }


    void Replica::ProcessViewChangeReq(const ViewChangeRequest& viewChangeReq)
    {
        if (status_ == ReplicaStatus::RECOVERING) {
            // Recovering replicas do not participate in view change
            return;
        }
        if (!CheckCV(viewChangeReq.replicaid(), viewChangeReq.cv())) {
            // stray message
            return;
        }
        if (Aggregated(viewChangeReq.cv())) {
            // If cv is updated, then it is likely that some messages in viewChangeSet_ become stray, so remove them 
            for (uint32_t i = 0; i < replicaNum_; i++) {
                auto iter = viewChangeSet_.find(i);
                if (iter != viewChangeSet_.end() && (!CheckCV(i, iter->second.cv()))) {
                    viewChangeSet_.erase(i);
                }
            }
        }
        if (viewChangeReq.view() > viewId_) {
            VLOG(2) << "InitiateViewChange-2";
            InitiateViewChange(viewChangeReq.view());
        }
        else {
            if (status_ == ReplicaStatus::NORMAL) {
                SendStartView(viewChangeReq.replicaid());
            }
            else {
                SendViewChange();
            }
        }
    }

    void Replica::ProcessViewChange(const ViewChange& viewChange) {
        // LOG(INFO) << "viewChange: " << viewChange.DebugString();
        if (status_ == ReplicaStatus::RECOVERING) {
            // Recovering replicas do not participate in view change
            return;
        }
        if (!CheckCV(viewChange.replicaid(), viewChange.cv())) {
            // stray message
            LOG(WARNING) << "Stray Message";
            return;
        }

        Aggregated(viewChange.cv());

        if (status_ == ReplicaStatus::NORMAL) {
            if (viewChange.view() > viewId_) {
                VLOG(2) << "InitiateViewChange-3";
                InitiateViewChange(viewChange.view());
            }
            else {
                // The sender lags behind
                SendStartView(viewChange.replicaid());
            }
        }
        else if (status_ == ReplicaStatus::VIEWCHANGE) {
            if (viewChange.view() > viewId_) {
                VLOG(2) << "InitiateViewChange-4";
                InitiateViewChange(viewChange.view());
            }
            else if (viewChange.view() < viewId_) {
                SendViewChangeRequest(viewChange.replicaid());
            }
            // viewChange.view() == viewId
            else if (viewChangeSet_.size() >= replicaNum_ / 2 + 1) {
                // We have got enough valid viewchange messages, no need for this one
                return;
            }
            else {
                ASSERT(AmLeader());
                viewChangeSet_[viewChange.replicaid()] = viewChange;
                VLOG(3) << "viewChangeSet Size=" << viewChangeSet_.size();
                // If cv is updated, then it is likely that some messages in viewChangeSet_ become stray, so remove them 
                for (uint32_t i = 0; i < replicaNum_; i++) {
                    auto iter = viewChangeSet_.find(i);
                    if (iter != viewChangeSet_.end() && (!CheckCV(i, iter->second.cv()))) {
                        viewChangeSet_.erase(i);
                    }
                }
                if (viewChangeSet_.size() >= replicaNum_ / 2) {
                    ASSERT(viewChangeSet_.find(replicaId_) == viewChangeSet_.end());
                    // Got f viewChange
                    // Plus myself, got f+1 viewChange messages 
                    ViewChange myvc;
                    CrashVectorStruct* masterCV = crashVectorInUse_[0].load();
                    myvc.mutable_cv()->Add(masterCV->cv_.begin(), masterCV->cv_.end());
                    myvc.set_view(viewId_);
                    myvc.set_replicaid(replicaId_);
                    myvc.set_syncpoint(maxSyncedLogId_);
                    myvc.set_unsynclogbegin(minUnSyncedLogId_);
                    myvc.set_unsynclogend(maxUnSyncedLogId_);
                    myvc.set_lastnormalview(lastNormalView_);
                    viewChangeSet_[replicaId_] = myvc;
                    // Has got enough viewChange messages, stop viewChangeTimer
                    masterContext_.endPoint_->UnregisterTimer(viewChangeTimer_);
                    TransferSyncedLog();
                }

            }
        }
        else {
            LOG(WARNING) << "Unexpected Status " << status_;
        }
    }

    void Replica::TransferSyncedLog() {

        uint32_t largestNormalView = lastNormalView_;
        uint32_t largestSyncPoint = maxSyncedLogId_;
        for (auto& kv : viewChangeSet_) {
            if (largestNormalView < kv.second.lastnormalview()) {
                largestNormalView = kv.second.lastnormalview();
            }
        }

        uint32_t targetReplicaId = replicaId_;
        transferSyncedEntry_ = true;
        for (auto& kv : viewChangeSet_) {
            if (kv.second.lastnormalview() == largestNormalView && largestSyncPoint < kv.second.syncpoint()) {
                largestSyncPoint = kv.second.syncpoint();
                targetReplicaId = kv.second.replicaid();
            }
        }

        stateTransferIndices_.clear();
        VLOG(3) << "maxSyncedLogId_=" << maxSyncedLogId_ << "\t"
            << "largestSyncPoint=" << largestSyncPoint << "\t"
            << "largestNormalView = " << largestNormalView << "\t"
            << "lastNormalView_=" << lastNormalView_;
        // Directly copy the synced entries
        if (largestNormalView == lastNormalView_) {
            if (maxSyncedLogId_ < largestSyncPoint) {
                stateTransferIndices_[targetReplicaId] = { maxSyncedLogId_ + 1, largestSyncPoint };
            }
            // Else: no need to do state transfer, because this replica has all synced entries
        }
        else {
            stateTransferIndices_[targetReplicaId] = { committedLogId_ + 1, largestSyncPoint };
        }

        if (!stateTransferIndices_.empty()) {
            // Start state transfer
            // After this state transfer has been completed, continue to execute the callback (MergeUnsyncedLog)

            stateTransferCallback_ = std::bind(&Replica::TransferUnSyncedLog, this);
            stateTransferTerminateTime_ = GetMicrosecondTimestamp() + stateTransferTimeout_ * 1000;
            stateTransferTerminateCallback_ = std::bind(&Replica::RollbackToViewChange, this);
            // Start the state tranfer timer
            masterContext_.endPoint_->RegisterTimer(stateTransferTimer_);
        }
        else {
            // Directly go to the second stage: transfer unsynced log
            TransferUnSyncedLog();
        }
    }

    void Replica::TransferUnSyncedLog() {
        // Get the unsynced logs from the f+1 remaining replicas
        // If this process cannot be completed, rollback to view change
        VLOG(3) << "TransferUnSyncedLog";
        stateTransferIndices_.clear();
        for (auto& kv : viewChangeSet_) {
            if (kv.first == replicaId_) {
                // No need to transfer log entries from self
                continue;
            }
            if (kv.second.unsynclogbegin() > kv.second.unsynclogend()) {
                // This replica has no unsynced logs 
                continue;
            }
            // request transfer of the filteredUnSyncedRequests vec
            stateTransferIndices_[kv.first] = { kv.second.unsynclogbegin(),  kv.second.unsynclogend() };
        }
        if (stateTransferIndices_.empty()) {
            // No need to do state transfer for unsynced logs
            // Directly go to new view
            EnterNewView();
            return;
        }
        transferSyncedEntry_ = false;
        // After this state transfer is completed, this replica will enter the new view
        stateTransferCallback_ = std::bind(&Replica::MergeUnSyncedLog, this);
        stateTransferTerminateTime_ = GetMicrosecondTimestamp() + stateTransferTimeout_ * 1000;
        stateTransferTerminateCallback_ = std::bind(&Replica::RollbackToViewChange, this);
        masterContext_.endPoint_->RegisterTimer(stateTransferTimer_);
    }

    void Replica::MergeUnSyncedLog() {
        int f = replicaNum_ / 2;
        int quorum = (f % 2 == 0) ? (f / 2 + 1) : (f / 2 + 2);
        for (auto& kv : requestsToMerge_) {
            uint64_t deadline = kv.first.first;
            uint64_t reqKey = kv.first.second;
            Request* request = kv.second.first;
            int count = kv.second.second;
            if (count >= quorum) {
                if (syncedRequestMap_.get(reqKey) != NULL) {
                    // at-most once
                    delete request;
                    continue;
                }
                syncedRequestMap_.assign(reqKey, request);
                ProcessRequest(deadline, reqKey, *request);
            }
        }
        requestsToMerge_.clear();
        EnterNewView();
    }

    void Replica::EnterNewView() {
        LOG(INFO) << "Enter New View " << viewId_;
        // Leader sends StartView to all the others
        if (AmLeader()) {
            SendStartView(-1);
        } // Else: followers directly start

        status_ = ReplicaStatus::NORMAL;
        lastNormalView_.store(viewId_);
        // Update crashVector, all synced with master
        CrashVectorStruct* masterCV = crashVectorInUse_[0].load();
        for (uint32_t i = 1; i < crashVectorVecSize_; i++) {
            crashVectorInUse_[i] = masterCV;
        }
        crashVector_.assign(masterCV->version_, masterCV);

        // More lightweight than CreateContext
        ResetContext();
        // Notify the blocking workers until all workers become active 
        while (activeWorkerNum_ < totalWorkerNum_) {
            waitVar_.notify_all();
            usleep(1000);
        }

        VLOG(2) << "View=" << viewId_ << " Recovered " << activeWorkerNum_;
    }


    void Replica::SendStateTransferRequest() {
        if (GetMicrosecondTimestamp() >= stateTransferTerminateTime_) {
            // If statetransfer cannot be completed within a certain amount of time, rollback to view change
            masterContext_.endPoint_->UnregisterTimer(stateTransferTimer_);
            stateTransferTerminateCallback_();
            return;
        }

        StateTransferRequest request;
        request.set_view(viewId_);
        request.set_replicaid(replicaId_);
        request.set_issynced(transferSyncedEntry_);
        for (auto& stateTransferInfo : stateTransferIndices_) {
            // Do not request too many entries at one time, otherwise, UDP packet cannot handle that
            request.set_logbegin(stateTransferInfo.second.first);
            if (stateTransferInfo.second.first + requestTrasnferBatch_ <= stateTransferInfo.second.second) {
                request.set_logend(stateTransferInfo.second.first + requestTrasnferBatch_);
            }
            else {
                request.set_logend(stateTransferInfo.second.second);
            }

            VLOG(3) << "stateTransferRequest = " << request.replicaid() << "\t" << request.logbegin() << "\t" << request.logend() << "\tisSynced=" << request.issynced();
            VLOG(4) << "stateTransferRequest = " << request.DebugString();
            masterContext_.endPoint_->SendMsgTo(*(masterReceiver_[stateTransferInfo.first]), request, MessageType::STATE_TRANSFER_REQUEST);
        }

    }

    void Replica::ProcessStateTransferRequest(const StateTransferRequest& stateTransferRequest) {
        VLOG(3) << "stateTransferRequest=" << stateTransferRequest.logbegin() << "\t" << stateTransferRequest.logend() << "\trequestReplicaid=" << stateTransferRequest.replicaid() << "\tsyncedMax:" << maxSyncedLogId_ << "\t" << "\tunsyncedMin:" << minUnSyncedLogId_
            << "\tunsyncedMax:" << maxUnSyncedLogId_ << "\t" << (uint32_t)stateTransferRequest.issynced();

        if (stateTransferRequest.view() != viewId_) {
            if (stateTransferRequest.view() > viewId_) {
                VLOG(2) << "InitiateViewChange-5";
                InitiateViewChange(stateTransferRequest.view());
            }
            return;
        }
        StateTransferReply reply;
        CrashVectorStruct* cv = crashVectorInUse_[0].load();
        const Address* requesterAddr = masterReceiver_[stateTransferRequest.replicaid()];
        reply.set_replicaid(replicaId_);
        reply.set_view(viewId_);
        reply.mutable_cv()->Add(cv->cv_.begin(), cv->cv_.end());
        reply.set_issynced(stateTransferRequest.issynced());
        reply.set_logbegin(stateTransferRequest.logbegin());
        reply.set_logend(stateTransferRequest.logend());

        ConcurrentMap<uint32_t, LogEntry*>& entryMap = (reply.issynced() ? syncedEntries_ : unsyncedEntries_);

        ConcurrentMap<uint64_t, Request*>& requestMap = (reply.issynced() ? syncedRequestMap_ : unsyncedRequestMap_);

        if (reply.issynced()) {
            ASSERT(maxSyncedLogId_ >= reply.logend());
        }
        else {
            ASSERT(maxUnSyncedLogId_ >= reply.logend());
        }


        for (uint32_t j = reply.logbegin(); j <= reply.logend(); j++) {
            LogEntry* entry = NULL;
            Request* request = NULL;
            uint32_t failcnt = 0;
            do {
                entry = entryMap.get(j);
                failcnt++;
            } while (entry == NULL);
            do {
                request = requestMap.get(entry->reqKey);
            } while (request == NULL);

            reply.add_reqs()->CopyFrom(*request);
            // Also piggyback the deadline
            reply.mutable_reqs()->rbegin()->set_sendtime(entry->deadline);
            reply.mutable_reqs()->rbegin()->set_bound(0);
        }


        masterContext_.endPoint_->SendMsgTo(*requesterAddr, reply, MessageType::STATE_TRANSFER_REPLY);

    }

    void Replica::ProcessStateTransferReply(const StateTransferReply& stateTransferReply) {
        if (status_ == ReplicaStatus::NORMAL) {
            // Normal replicas do not need state transfer
            return;
        }
        if (!CheckCV(stateTransferReply.replicaid(), stateTransferReply.cv())) {
            return;
        }
        else {
            Aggregated(stateTransferReply.cv());
        }

        if (!(masterContext_.endPoint_->isRegistered(stateTransferTimer_))) {
            // We are not doing state transfer, so ignore this message
            return;
        }

        if (stateTransferReply.view() < viewId_) {
            // Old view: ignore
            return;
        }
        else if (stateTransferReply.view() > viewId_) {
            masterContext_.endPoint_->UnregisterTimer(stateTransferTimer_);
            if (status_ == ReplicaStatus::RECOVERING) {
                // This state transfer is useless, stop it and restart recovery request
                masterContext_.endPoint_->RegisterTimer(recoveryRequestTimer_);
            }
            else if (status_ == ReplicaStatus::VIEWCHANGE) {
                VLOG(2) << "InitiateViewChange-6";
                InitiateViewChange(stateTransferReply.view());
            }
            else {
                LOG(ERROR) << "Unknown replica status " << (uint32_t)status_;
            }
            return;
        }

        // Else: Same view

        if (transferSyncedEntry_ != stateTransferReply.issynced()) {
            return;
        }

        const auto& iter = stateTransferIndices_.find(stateTransferReply.replicaid());
        if (iter == stateTransferIndices_.end() || stateTransferReply.logend() < iter->second.first) {
            // We do not need these log entries
            return;
        }

        if (stateTransferReply.issynced()) {
            // This is the state-transfer for synced requests
            for (uint32_t i = iter->second.first; i <= stateTransferReply.logend(); i++) {
                if (syncedRequestMap_.get(i) != NULL) {
                    LOG(INFO) << "logid=" << i << "\t" << "stateMissingBegin=" << iter->second.first << "\tstateMissingEnd=" << iter->second.second << "\t maxSyncedLogId=" << maxSyncedLogId_ << "\t" << "stateTransferReply.logbegin=" << stateTransferReply.logbegin() << "\t" << "stateTransferReply.logend=" << stateTransferReply.logend();
                }
                ASSERT(syncedRequestMap_.get(i) == NULL);
                Request* request = new Request(stateTransferReply.reqs(i - iter->second.first));
                syncedRequestMap_.assign(i, request);
                uint64_t deadline = request->sendtime();
                uint64_t reqKey = request->clientid();
                reqKey = ((reqKey << 32u) | (request->reqid()));
                ProcessRequest(deadline, reqKey, *request, true, false);
            }
        }
        else {
            // This is the state-transfer for unsynced request (log merge)
            for (int i = 0; i < stateTransferReply.reqs_size(); i++) {
                uint64_t deadline = stateTransferReply.reqs(i).sendtime();
                uint32_t clientId = stateTransferReply.reqs(i).clientid();
                uint32_t reqId = stateTransferReply.reqs(i).reqid();
                uint64_t reqKey = CONCAT_UINT32(clientId, reqId);
                std::pair<uint64_t, uint64_t> key(deadline, reqKey);
                if (requestsToMerge_.find(key) != requestsToMerge_.end()) {
                    Request* request = new Request(stateTransferReply.reqs(i));
                    requestsToMerge_[key] = { request, 1 };
                }
                else {
                    requestsToMerge_[key].second++;
                }
            }
        }

        iter->second.first = stateTransferReply.logend() + 1;
        VLOG(2) << "Transfer Synced? " << stateTransferReply.issynced() << "\t" << " In Progress: " << iter->first << ":" << iter->second.first << "-" << iter->second.second;

        if (iter->second.first > iter->second.second) {
            // We have completed the state transfer for this target replica
            stateTransferIndices_.erase(iter->first);
        }

        if (stateTransferIndices_.empty()) {
            // This state transfer is completed, unregister the timer
            masterContext_.endPoint_->UnregisterTimer(stateTransferTimer_);
            // If we have a callback, then call it
            if (stateTransferCallback_) {
                stateTransferCallback_();
            }
        }

    }

    void Replica::RewindSyncedLogTo(uint32_t rewindPoint) {
        uint32_t logIdToClear = rewindPoint + 1;
        std::set<uint32_t> keysToClear;
        VLOG(3) << "maxSyncedLogId_=" << maxSyncedLogId_ << "\t"
            << "rewindPoint=" << rewindPoint;
        // Rewind syncedEntries_ and maxSyncedLogId_
        while (maxSyncedLogId_ > rewindPoint) {
            LogEntry* entry = syncedEntries_.get(logIdToClear);
            keysToClear.insert(entry->opKey);
            syncedEntries_.erase(maxSyncedLogId_);
            maxSyncedLogId_--;
        }
        // Rewind syncedRequestMap_, syncedReq2LogId_, syncedEntriesByKey_ and maxSyncedLogIdByKey_
        for (const uint32_t& key : keysToClear) {
            while (maxSyncedLogIdByKey_[key] >= CONCURRENT_MAP_START_INDEX) {
                uint64_t keyAndId = key;
                keyAndId = ((keyAndId << 32u) | maxSyncedLogIdByKey_[key]);
                LogEntry* entry = syncedEntriesByKey_.get(keyAndId);
                ASSERT(entry != NULL);
                uint32_t logId = syncedReq2LogId_.get(entry->reqKey);
                if (logId > maxSyncedLogId_) {
                    // Should be cleared
                    syncedReq2LogId_.erase(entry->reqKey);
                    syncedEntriesByKey_.erase(keyAndId);
                    Request* request = syncedRequestMap_.get(entry->reqKey);
                    syncedRequestMap_.erase(entry->reqKey);
                    if (unsyncedEntries_.get(entry->reqKey) == NULL) {
                        delete request;
                    }// Else: This request is still owned by unsyncedRequestMap
                        // So we do not free it, the gc-thread will do that
                    delete entry;
                    maxSyncedLogIdByKey_[key]--;
                }
                else {
                    // We have completely cleared the entries that go beyond the maxSyncedLogId_
                    // Break the while loop and go to clear the next key
                    break;
                }

            }
        }


    }

    void Replica::ProcessStartView(const StartView& startView) {
        VLOG(3) << startView.DebugString();

        if (!CheckCV(startView.replicaid(), startView.cv())) {
            return;
        }
        else {
            Aggregated(startView.cv());
        }

        if (status_ == ReplicaStatus::VIEWCHANGE) {
            if (startView.view() > viewId_) {
                VLOG(2) << "InitiateViewChange-7";
                InitiateViewChange(startView.view());
            }
            else if (startView.view() == viewId_) {
                VLOG(3) << "committedLogId_=" << committedLogId_ << "\t"
                    << "syncedlogid=" << startView.syncedlogid();
                if (committedLogId_ < startView.syncedlogid()) {
                    // Start StateTransfer
                    RewindSyncedLogTo(committedLogId_);
                    stateTransferIndices_.clear();
                    stateTransferIndices_[startView.replicaid()] = { committedLogId_ + 1, startView.syncedlogid() };
                    stateTransferCallback_ = std::bind(&Replica::EnterNewView, this);
                    stateTransferTerminateTime_ = GetMicrosecondTimestamp() + stateTransferTimeout_ * 1000;
                    stateTransferTerminateCallback_ = std::bind(&Replica::RollbackToViewChange, this);
                    transferSyncedEntry_ = true;
                    masterContext_.endPoint_->RegisterTimer(stateTransferTimer_);
                }
                else {
                    RewindSyncedLogTo(committedLogId_);
                    EnterNewView();
                }

            } // else: startView.view()<viewId_, old message, ignore it
        }
        else if (status_ == ReplicaStatus::NORMAL) {
            if (startView.view() > viewId_) {
                VLOG(2) << "InitiateViewChange-8";
                InitiateViewChange(startView.view());
            }
            else if (startView.view() < viewId_) {
                // My view is fresher
                SendStartView(startView.replicaid());
            }
            // Else: We are in the same view and this replica is normal, no need startView
        }
        // If status == RECOVERING, it does not participate in view change

    }


    void Replica::BroadcastCrashVectorRequest() {
        CrashVectorRequest request;
        boost::uuids::random_generator generator;
        boost::uuids::uuid uuid = generator();
        nonce_ = boost::uuids::to_string(uuid);
        request.set_nonce(nonce_);
        request.set_replicaid(replicaId_);
        crashVectorReplySet_.clear();
        for (uint32_t i = 0; i < replicaNum_; i++) {
            if (i == replicaId_) {
                continue;
            }
            masterContext_.endPoint_->SendMsgTo(*(masterReceiver_[i]), request, MessageType::CRASH_VECTOR_REQUEST);
        }

    }

    void Replica::BroadcastRecoveryRequest() {
        RecoveryRequest request;
        CrashVectorStruct* cv = crashVectorInUse_[0].load();
        request.mutable_cv()->Add(cv->cv_.begin(), cv->cv_.end());
        request.set_replicaid(replicaId_);
        for (uint32_t i = 0; i < replicaNum_; i++) {
            if (i == replicaId_) {
                continue;
            }
            masterContext_.endPoint_->SendMsgTo(*(masterReceiver_[i]), request, MessageType::RECOVERY_REQUEST);
        }
    }

    void Replica::ProcessCrashVectorRequest(const CrashVectorRequest& request) {
        if (status_ != ReplicaStatus::NORMAL) {
            return;
        }

        CrashVectorReply reply;
        reply.set_nonce(request.nonce());
        reply.set_replicaid(replicaId_);
        CrashVectorStruct* cv = crashVectorInUse_[0].load();
        reply.mutable_cv()->Add(cv->cv_.begin(), cv->cv_.end());
        masterContext_.endPoint_->SendMsgTo(*(masterReceiver_[request.replicaid()]), reply, MessageType::CRASH_VECTOR_REPLY);
    }

    void Replica::ProcessCrashVectorReply(const CrashVectorReply& reply) {
        if (status_ != ReplicaStatus::RECOVERING) {
            LOG(INFO) << "nolong Recovering " << status_;
            return;
        }

        if (nonce_ != reply.nonce()) {
            LOG(INFO) << "nonce inconistent " << crashVectorReplySet_.size();
            return;
        }

        if (masterContext_.endPoint_->isRegistered(crashVectorRequestTimer_) == false) {
            // We no longer request crash vectors
            LOG(INFO) << "no longer register crashVectorRequest " << crashVectorReplySet_.size();
            return;
        }

        crashVectorReplySet_[reply.replicaid()] = reply;

        if (crashVectorReplySet_.size() >= replicaNum_ / 2 + 1) {
            // Got enough quorum
            CrashVectorStruct* oldCV = crashVectorInUse_[0].load();
            CrashVectorStruct* newCV = new CrashVectorStruct(*oldCV);
            newCV->version_++;
            for (const auto& kv : crashVectorReplySet_) {
                for (uint32_t i = 0; i < replicaNum_; i++) {
                    if (kv.second.cv(i) > newCV->cv_[i]) {
                        newCV->cv_[i] = kv.second.cv(i);
                    }
                }
            }
            // Increment self counter 
            newCV->cv_[replicaId_]++;
            crashVector_.assign(newCV->version_, newCV);
            for (uint32_t i = 0; i < crashVectorVecSize_; i++) {
                crashVectorInUse_[i] = newCV;
            }
            masterContext_.endPoint_->UnregisterTimer(crashVectorRequestTimer_);
            crashVectorReplySet_.clear();
            // Start Recovery Request
            masterContext_.endPoint_->RegisterTimer(recoveryRequestTimer_);

        }

    }

    void Replica::ProcessRecoveryRequest(const RecoveryRequest& request) {
        if (status_ != ReplicaStatus::NORMAL) {
            return;
        }

        if (!CheckCV(request.replicaid(), request.cv())) {
            return;
        }
        else {
            Aggregated(request.cv());
        }

        RecoveryReply reply;
        CrashVectorStruct* cv = crashVectorInUse_[0].load();
        reply.set_replicaid(replicaId_);
        reply.set_view(viewId_);
        reply.mutable_cv()->Add(cv->cv_.begin(), cv->cv_.end());
        reply.set_syncedlogid(maxSyncedLogId_);
        masterContext_.endPoint_->SendMsgTo(*(masterReceiver_[request.replicaid()]), reply, MessageType::RECOVERY_REPLY);

    }

    void Replica::ProcessRecoveryReply(const RecoveryReply& reply) {
        if (!CheckCV(reply.replicaid(), reply.cv())) {
            return;
        }
        else {
            if (Aggregated(reply.cv())) {
                // If cv is updated, then it is likely that some messages in recoveryReplySet_ become stray, so remove them 
                for (uint32_t i = 0; i < replicaNum_; i++) {
                    auto iter = recoveryReplySet_.find(i);
                    if (iter != recoveryReplySet_.end() && (!CheckCV(i, iter->second.cv()))) {
                        recoveryReplySet_.erase(i);
                    }
                }
            }
        }

        if (masterContext_.endPoint_->isRegistered(recoveryRequestTimer_) == false) {
            // We no longer request recovery reply
            return;
        }
        recoveryReplySet_[reply.replicaid()] = reply;
        if (recoveryReplySet_.size() >= replicaNum_ / 2 + 1) {
            // Got enough quorum
            masterContext_.endPoint_->UnregisterTimer(recoveryRequestTimer_);
            uint32_t maxView = 0;
            uint32_t syncedLogId = 0;
            for (const auto& kv : recoveryReplySet_) {
                if (kv.second.view() > maxView) {
                    maxView = kv.second.view();
                    syncedLogId = kv.second.syncedlogid();
                }
            }
            // Get the maxView, launch state transfer with the corresponding leader
            viewId_ = maxView;
            recoveryReplySet_.clear();
            if (AmLeader()) {
                // If the recoverying replica happens to be the leader of the new view, don't participate. Wait until the healthy replicas elect a new leader
                usleep(1000); // sleep some time and restart the recovery process
                masterContext_.endPoint_->RegisterTimer(recoveryRequestTimer_);
            }
            else {
                // Launch state transfer for synced log entries
                stateTransferIndices_.clear();
                if (syncedLogId >= CONCURRENT_MAP_START_INDEX) {
                    // There are some synced log entries that should be transferred
                    transferSyncedEntry_ = true;
                    stateTransferIndices_[maxView % replicaNum_] = { CONCURRENT_MAP_START_INDEX, syncedLogId };
                    stateTransferCallback_ = std::bind(&Replica::EnterNewView, this);
                    stateTransferTerminateTime_ = GetMicrosecondTimestamp() + stateTransferTimeout_ * 1000;
                    stateTransferTerminateCallback_ = std::bind(&Replica::RollbackToRecovery, this);
                    masterContext_.endPoint_->RegisterTimer(stateTransferTimer_);
                }
                else {
                    // No log entries to recover, directly enter new view
                    EnterNewView();
                }

            }
        }
    }

    bool Replica::CheckViewAndCV(const uint32_t view, const google::protobuf::RepeatedField<uint32_t>& cv) {
        if (view < this->viewId_) {
            // old message
            return false;
        }
        if (view > this->viewId_) {
            // new view, update status and wait for master thread to handle the situation
            status_ = ReplicaStatus::VIEWCHANGE;
            return false;
        }
        // View is okay, freshen heartbeat time
        lastHeartBeatTime_ = GetMicrosecondTimestamp();

        // Check crashVector
        CrashVectorStruct* masterCV = crashVectorInUse_[0];
        for (int i = 0; i < cv.size(); i++) {
            if (masterCV->cv_[i] > cv.at(i)) {
                // The message has old cv, may be a stray message
                return false;
            }
        }
        return true;
    }

    void Replica::ProcessSyncStatusReport(const SyncStatusReport& report) {
        if (!CheckCV(report.replicaid(), report.cv())) {
            // Stray message
            return;
        }
        else {
            if (Aggregated(report.cv())) {
                // Possibly make existing msg become stray
                for (uint32_t i = 0; i < replicaId_; i++) {
                    auto iter = syncStatusSet_.find(i);
                    if (iter != syncStatusSet_.end() && (!CheckCV(i, iter->second.cv()))) {
                        syncStatusSet_.erase(i);
                    }
                }
            }
        }

        if (!CheckView(report.view())) {
            return;
        }

        auto iter = syncStatusSet_.find(report.replicaid());
        if (iter != syncStatusSet_.end() && iter->second.syncedlogid() < report.syncedlogid()) {
            syncStatusSet_[report.replicaid()] = report;
        }

        if (syncStatusSet_.size() >= replicaNum_ / 2 + 1) {
            uint32_t minLogId = UINT32_MAX;
            for (const auto& kv : syncStatusSet_) {
                if (minLogId > kv.second.syncedlogid()) {
                    minLogId = kv.second.syncedlogid();
                }
            }
            if (minLogId >= committedLogId_) {
                committedLogId_ = minLogId;
                SendCommit();
            }
        }

    }

    void Replica::ProcessCommitInstruction(const CommitInstruction& commit) {
        if (!CheckCV(commit.replicaid(), commit.cv())) {
            return;
        }
        else {
            Aggregated(commit.cv());
        }
        if (!CheckView(commit.view())) {
            return;
        }
        if (commit.committedlogid() > committedLogId_) {
            committedLogId_ = commit.committedlogid();
        }
    }

    bool Replica::CheckView(const uint32_t view) {
        if (view < this->viewId_) {
            // old message
            return false;
        }
        if (view > this->viewId_) {
            // new view, update status and wait for master thread to handle the situation
            VLOG(2) << "InitiateViewChange-9";
            InitiateViewChange(view);
            return false;
        }
        return true;
    }

    bool Replica::CheckCV(const uint32_t senderId, const google::protobuf::RepeatedField<uint32_t>& cv) {
        CrashVectorStruct* masterCV = crashVectorInUse_[0].load();
        return (cv.at(senderId) >= masterCV->cv_[senderId]);
    }

    bool Replica::Aggregated(const google::protobuf::RepeatedField<uint32_t>& cv) {
        CrashVectorStruct* masterCV = crashVectorInUse_[0].load();
        std::vector<uint32_t> maxCV(masterCV->cv_);
        bool needAggregate = false;
        for (uint32_t i = 0; i < replicaNum_; i++) {
            if (maxCV[i] < cv.at(i)) {
                // The incoming cv has fresher elements
                needAggregate = true;
                maxCV[i] = cv.at(i);
            }
        }
        if (needAggregate) {

            CrashVectorStruct* newCV = new CrashVectorStruct(maxCV, masterCV->version_ + 1);
            crashVector_.assign(newCV->version_, newCV);
            crashVectorInUse_[0] = newCV;
            if (status_ == ReplicaStatus::NORMAL) {
                // Wait until the reply threads has known the new cv
                while (true) {
                    bool ready = true;
                    for (uint32_t i = 1; i <= fastReplyQu_.size(); i++) {
                        if (crashVectorInUse_[i].load()->version_ < newCV->version_) {
                            ready = false;
                        }
                    }
                    if (ready) {
                        break;
                    }
                    else {
                        usleep(1000);
                    }
                }
            } // Else (status_=ViewChange), then there is only master thread alive, no need to wait for reply thread

        }
        return needAggregate;
    }

    void Replica::RollbackToViewChange() {
        status_ = ReplicaStatus::VIEWCHANGE;
        viewChangeSet_.clear();
        if (false == masterContext_.endPoint_->isRegistered(viewChangeTimer_)) {
            masterContext_.endPoint_->RegisterTimer(viewChangeTimer_);
        }
    }

    void Replica::RollbackToRecovery() {
        status_ = ReplicaStatus::RECOVERING;
        recoveryReplySet_.clear();
        if (false == masterContext_.endPoint_->isRegistered(recoveryRequestTimer_)) {
            masterContext_.endPoint_->RegisterTimer(recoveryRequestTimer_);
        }
    }


    std::string Replica::ApplicationExecute(const Request& req) {
        return "";
    }
    bool Replica::AmLeader() {
        return (viewId_ % replicaNum_ == replicaId_);
    }
    MessageHeader* Replica::CheckMsgLength(const char* msgBuffer, const int msgLen) {
        if (msgLen < 0) {
            LOG(WARNING) << "\tmsgLen=" << msgLen;
            return NULL;
        }
        if ((uint32_t)msgLen < sizeof(MessageHeader)) {
            LOG(WARNING) << "\tmsgLen=" << msgLen;
            return NULL;
        }
        MessageHeader* msgHdr = (MessageHeader*)(void*)msgBuffer;

        if (msgHdr->msgLen == msgLen - sizeof(MessageHeader)) {
            return msgHdr;
        }
        else {
            LOG(WARNING) << "Incomplete message: expected length " << msgHdr->msgLen + sizeof(MessageHeader) << "\tbut got " << msgLen;
            return NULL;
        }

    }
}

