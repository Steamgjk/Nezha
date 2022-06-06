#include "nezha/replica.h"

namespace nezha {
#define GJK_DEBUG
#ifdef GJK_DEBUG
#define ASSERT(x) assert(x)
#else 
#define ASSERT(x) {}
#endif

    Replica::Replica(const std::string& configFile) :viewId_(0), lastNormalView_(0)
    {
        // Load Config
        replicaConfig_ = YAML::LoadFile(configFile);
        CreateContext();
        LOG(INFO) << "viewId_=" << viewId_
            << "\treplicaId=" << replicaId_
            << "\treplicaNum=" << replicaNum_
            << "\tkeyNum=" << keyNum_;
        // Launch Threads (based on Config)
        LaunchThreads();
        // Master thread run
        masterContext_.Register();
        if (status_ == ReplicaStatus::RECOVERING) {
            masterContext_.endPoint_->RegisterTimer(crashVectorRequestTimer_);
        }
        masterContext_.endPoint_->LoopRun();

    }

    Replica::~Replica()
    {
        status_ = ReplicaStatus::TERMINATED;
        for (auto& kv : threadPool_) {
            kv.second->join();
            delete kv.second;
            LOG(INFO) << "Deleted\t" << kv.first;
        }
    }

    void Replica::CreateContext() {
        replicaId_ = replicaConfig_["replica-id"].as<int>();
        replicaNum_ = replicaConfig_["replica-ips"].size();
        keyNum_ = replicaConfig_["key-num"].as<uint32_t>();
        lastReleasedEntryByKeys_.resize(keyNum_, { 0ul,0ul });
        // Since ConcurrentMap reseres 0 and 1, we can only use log-id from 2
        maxSyncedLogId_ = CONCURRENT_MAP_START_INDEX - 1;
        minUnSyncedLogId_ = CONCURRENT_MAP_START_INDEX;
        maxUnSyncedLogId_ = CONCURRENT_MAP_START_INDEX - 1;
        maxSyncedLogIdByKey_.resize(keyNum_, CONCURRENT_MAP_START_INDEX - 1);
        minUnSyncedLogIdByKey_.resize(keyNum_, CONCURRENT_MAP_START_INDEX);
        maxUnSyncedLogIdByKey_.resize(keyNum_, CONCURRENT_MAP_START_INDEX - 1);
        unsyncedLogIdByKeyToClear_.resize(keyNum_, CONCURRENT_MAP_START_INDEX);
        committedLogId_ = CONCURRENT_MAP_START_INDEX - 1;

        if (replicaConfig_["recovering"].as<uint32_t>() > 0) {
            status_ = ReplicaStatus::RECOVERING;
        }
        else {
            status_ = ReplicaStatus::NORMAL;
        }

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
            ((Replica*)ctx)->CheckHeartBeat();
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
            std::string ip = replicaConfig_["replica-ips"].as<std::string>();
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
        port = replicaConfig_["ack-missed-index-port"].as<int>();
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
        port = replicaConfig_["ack-missed-req-port"].as<int>();
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
        crashVectorVecSize_ = 1 + replyShardNum * 2 + replicaConfig_["index-sync-shards"].as<int>() + 2;
        indexRecvCVIdx_ = crashVectorVecSize_ - 2;
        indexAckCVIdx_ = crashVectorVecSize_ - 1;
        crashVectorInUse_ = new std::atomic<CrashVectorStruct*>[crashVectorVecSize_];
        for (uint32_t i = 0; i < crashVectorVecSize_; i++) {
            crashVectorInUse_[i] = cv;
        }

        // Create other useful timers
        indexAskTimer_ = new TimerStruct([](void* ctx, UDPSocketEndpoint* receiverEP) {
            ((Replica*)ctx)->AskMissedIndex();
            }, this, replicaConfig_["index-ask-period-ms"].as<int>());
        roundRobinIndexAskIdx_ = 0;
        // Initially, no missed indices, so we make first > second
        missedIndices_.first = 1;
        missedIndices_.second = 0;

        requestAskTimer_ = new TimerStruct([](void* ctx, UDPSocketEndpoint* receiverEP) {
            ((Replica*)ctx)->AskMissedRequest();
            }, this, replicaConfig_["request-ask-period-ms"].as<int>());
        roundRobinRequestAskIdx_ = 0;
        missedReqKeys_.clear();

        viewChangeTimer_ = new TimerStruct([](void* ctx, UDPSocketEndpoint* receiverEP) {
            ((Replica*)ctx)->BroadcastViewChange();
            }, this, replicaConfig_["view-change-period-ms"].as<int>());
        roundRobinRequestProcessIdx_ = 0;


        periodicSyncTimer_ = new TimerStruct([](void* ctx, UDPSocketEndpoint* receiverEP) {
            ((Replica*)ctx)->SendSyncStatusReport();
            }, this, replicaConfig_["sync-report-period-ms"].as<int>());


        stateTransferTimer_ = new TimerStruct([](void* ctx, UDPSocketEndpoint* receiverEP) {
            ((Replica*)ctx)->SendStateTransferRequest();
            }, this, replicaConfig_["state-transfer-period-ms"].as<int>());

        stateRequestTransferBatch_ = replicaConfig_["request-transfer-max-batch"].as<uint32_t>();
        stateTransferTimeout_ = replicaConfig_["state-transfer-timeout-ms"].as<uint64_t>();

        crashVectorRequestTimer_ = new TimerStruct([](void* ctx, UDPSocketEndpoint* receiverEP) {
            ((Replica*)ctx)->BroadcastCrashVectorRequest();
            }, this, replicaConfig_["crash-vector-request-period-ms"].as<int>());

        recoveryRequestTimer_ = new TimerStruct([](void* ctx, UDPSocketEndpoint* receiverEP) {
            ((Replica*)ctx)->BroadcastRecoveryRequest();
            }, this, replicaConfig_["recovery-request-period-ms"].as<int>());

    }

    void Replica::ResetContext() {
        // Clear reply queues 
        for (uint32_t i = 0; i < fastReplyQu_.size(); i++) {
            LogEntry* entry;
            while (fastReplyQu_[i].try_dequeue(entry)) {}
            while (slowReplyQu_[i].try_dequeue(entry)) {}
        }

        // Clear UnSyncedLogs
        // Since ConcurrentMap reseres 0 and 1, we can only use log-id from 2
        minUnSyncedLogId_ = CONCURRENT_MAP_START_INDEX;
        maxUnSyncedLogId_ = CONCURRENT_MAP_START_INDEX - 1;
        minUnSyncedLogIdByKey_.clear();
        maxUnSyncedLogIdByKey_.clear();
        unsyncedLogIdByKeyToClear_.clear();
        minUnSyncedLogIdByKey_.resize(keyNum_, CONCURRENT_MAP_START_INDEX);
        maxUnSyncedLogIdByKey_.resize(keyNum_, CONCURRENT_MAP_START_INDEX - 1);
        unsyncedLogIdByKeyToClear_.resize(keyNum_, CONCURRENT_MAP_START_INDEX);
        committedLogId_ = CONCURRENT_MAP_START_INDEX - 1;

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
        missedIndices_.first = 1;
        missedIndices_.second = 0;
        roundRobinRequestAskIdx_ = 0;
        missedReqKeys_.clear();
        roundRobinRequestProcessIdx_ = 0;

        stateTransferIndices_.clear();
        viewChangeSet_.clear();
        crashVectorReplySet_.clear();
        recoveryReplySet_.clear();
        syncStatusSet_.clear();
    }

    void Replica::LaunchThreads() {
        activeWorkerNum_ = 0;
        // RequestReceive
        for (int i = 0; i < replicaConfig_["receiver-shards"].as<int>(); i++) {
            activeWorkerNum_.fetch_add(1);
            std::thread* td = new std::thread(&Replica::ReceiveTd, this, i);
            std::string key("ReceiveTd-" + std::to_string(i));
            threadPool_[key] = td;
            LOG(INFO) << "Launched " << key << "\t" << td->native_handle();
        }

        // RequestProcess
        for (int i = 0; i < replicaConfig_["process-shards"].as<int>(); i++) {
            activeWorkerNum_.fetch_add(1);
            std::thread* td = new std::thread(&Replica::ProcessTd, this, i);
            std::string key("ProcessTd-" + std::to_string(i));
            threadPool_[key] = td;
            LOG(INFO) << "Launched " << key << "\t" << td->native_handle();
        }

        // RequestReply
        int replyShardNum = replicaConfig_["reply-shards"].as<int>();
        for (int i = 0; i < replyShardNum; i++) {
            activeWorkerNum_.fetch_add(1);
            std::thread* td = new std::thread(&Replica::FastReplyTd, this, i, i + 1);
            std::string key("FastReplyTd-" + std::to_string(i));
            threadPool_[key] = td;
            LOG(INFO) << "Launched " << key << "\t" << td->native_handle();
        }
        for (int i = 0; i < replyShardNum; i++) {
            activeWorkerNum_.fetch_add(1);
            std::thread* td = new std::thread(&Replica::SlowReplyTd, this, i, i + replyShardNum + 1);
            std::string key("SlowReplyTd-" + std::to_string(i));
            threadPool_[key] = td;
            LOG(INFO) << "Launched " << key << "\t" << td->native_handle();
        }

        // IndexSync
        for (int i = 0; i < replicaConfig_["index-sync-shards"].as<int>(); i++) {
            activeWorkerNum_.fetch_add(1);
            std::thread* td = new std::thread(&Replica::IndexSendTd, this, i, i + replyShardNum * 2 + 1);
            std::string key("IndexSendTd-" + std::to_string(i));
            threadPool_[key] = td;
            LOG(INFO) << "Launched " << key << "\t" << td->native_handle();
            if (!AmLeader()) {
                // follower only needs one sync thread
                break;
            }
        }

        activeWorkerNum_.fetch_add(1);
        threadPool_["IndexRecvTd"] = new std::thread(&Replica::IndexRecvTd, this);
        LOG(INFO) << "Launched IndexRecvTd\t" << threadPool_["IndexRecvTd"]->native_handle();

        activeWorkerNum_.fetch_add(1);
        threadPool_["MissedIndexAckTd"] = new std::thread(&Replica::MissedIndexAckTd, this);
        LOG(INFO) << "Launched MissedIndexAckTd\t" << threadPool_["MissedIndexAckTd"]->native_handle();

        activeWorkerNum_.fetch_add(1);
        threadPool_["MissedReqAckTd"] = new std::thread(&Replica::MissedReqAckTd, this);
        LOG(INFO) << "Launched MissedReqAckTd\t" << threadPool_["MissedReqAckTd"]->native_handle();

        activeWorkerNum_.fetch_add(1);
        threadPool_["GarbageCollectTd"] = new std::thread(&Replica::GarbageCollectTd, this);
        LOG(INFO) << "Launch  GarbageCollectTd " << threadPool_["GarbageCollectTd"]->native_handle();

        totalWorkerNum_ = activeWorkerNum_;
        LOG(INFO) << "totalWorkerNum_=" << totalWorkerNum_;
    }

    void Replica::ReceiveClientRequest(char* msgBuffer, int msgLen, Address* sender, UDPSocketEndpoint* receiverEP) {
        if (!CheckMsgLength(msgBuffer, msgLen)) {
            return;
        }
        MessageHeader* msgHdr = (MessageHeader*)(void*)msgBuffer;
        if (msgHdr->msgType == MessageType::CLIENT_REQUEST
            || msgHdr->msgType == MessageType::LEADER_REQUEST) {
            Request* request = new Request();
            if (request->ParseFromArray(msgBuffer + sizeof(MessageHeader), msgHdr->msgLen)) {
                // Alert: When this request is sent by leader, do not update proxyAddressMap
                if (msgHdr->msgType == MessageType::CLIENT_REQUEST && proxyAddressMap_.get(request->proxyid()) == 0) {
                    Address* addr = new Address(*sender);
                    // Alert: When one proxy sends the request, it needs to specify a proper *unique* proxyid related to one specific receiver thread on the replica, so that this replica's different receiver threads will not insert the same entry concurrently (otherwise, it may cause memory leakage) 
                    proxyAddressMap_.assign(request->proxyid(), addr);
                }

                processQu_.enqueue(request);
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

    void Replica::BlockWhenStatusIs(char targetStatus) {
        if (status_ == targetStatus) {
            activeWorkerNum_.fetch_sub(1);
            std::unique_lock<std::mutex> lk(waitMutext_);
            waitVar_.wait(lk, [this, targetStatus] {
                if (status_ == ReplicaStatus::TERMINATED || status_ != targetStatus) {
                    // unlock 
                    activeWorkerNum_.fetch_add(1);
                    return true;
                }
                else {
                    return false;
                }
                });
        }
    }

    void Replica::ReceiveTd(int id) {
        while (status_ != ReplicaStatus::TERMINATED) {
            requestContext_[id].Register();
            requestContext_[id].endPoint_->LoopRun();
            // Normally, it should be blocked in LoopRun, if it comes here, then there are 2 possible cases: (1) Terminated (2) ViewChange
            BlockWhenStatusIs(ReplicaStatus::VIEWCHANGE);
        }
    }

    void Replica::ProcessTd(int id) {
        uint64_t reqKey = 0;
        Request* req = NULL;
        bool amLeader = AmLeader();
        while (status_ != ReplicaStatus::TERMINATED) {
            BlockWhenStatusIs(ReplicaStatus::VIEWCHANGE);
            if (processQu_.try_dequeue(req)) {
                reqKey = req->clientid();
                reqKey = ((reqKey << 32u) | (req->reqid()));
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
                        // at-most-once: duplicate requests are not executed twice
                        // We simply send the previous reply messages
                        LogEntry* entry = syncedEntries_.get(duplicateLogIdx);
                        // update proxy id in case the client has changed its proxy
                        entry->proxyId = req->proxyid();
                        fastReplyQu_[(roundRobinRequestProcessIdx_++) % fastReplyQu_.size()].enqueue(entry);
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
                        slowReplyQu_[(roundRobinRequestProcessIdx_++) % fastReplyQu_.size()].enqueue(entry);
                    }
                    else {
                        duplicateLogIdx = unsyncedReq2LogId_.get(reqKey);
                        // If  duplicateLogIdx < minUnSyncedLogId_, just consider it as non-duplicate, because it will soon be cleared by GarbageCollect-Td
                        if (duplicateLogIdx >= minUnSyncedLogId_) {
                            // Duplicate: resend fast-reply for this request
                            LogEntry* entry = unsyncedEntries_.get(duplicateLogIdx);
                            // Update proxy id in case the client has changed its proxy
                            entry->proxyId = req->proxyid();
                            fastReplyQu_[(roundRobinRequestProcessIdx_++) % fastReplyQu_.size()].enqueue(entry);
                            delete req;
                        }
                        else {
                            // Not duplicate
                            uint64_t deadline = req->sendtime() + req->bound();
                            std::pair<uint64_t, uint64_t>myEntry(deadline, reqKey);
                            unsyncedRequestMap_.assign(reqKey, req);
                            if (myEntry > lastReleasedEntryByKeys_[req->key()]) {
                                earlyBuffer_[myEntry] = req;
                            }
                            // ELse, followers donot care about it (i.e. leave it in late buffer)
                        }
                    }
                }

            }

            // Polling early-buffer
            uint64_t nowTime = GetMicrosecondTimestamp();

            while ((!earlyBuffer_.empty()) && nowTime >= earlyBuffer_.begin()->first.first) {
                uint64_t deadline = earlyBuffer_.begin()->first.first;
                uint64_t reqKey = earlyBuffer_.begin()->first.second;
                Request* req = earlyBuffer_.begin()->second;
                lastReleasedEntryByKeys_[req->key()] = { deadline, reqKey };
                ProcessRequest(deadline, reqKey, *req, AmLeader(), true);
                earlyBuffer_.erase(earlyBuffer_.begin());
            }
        }
    }

    void Replica::ProcessRequest(const uint64_t deadline, const uint64_t reqKey, const Request& request, const bool isSynedReq, const bool sendReply) {
        SHA_HASH myHash = CalculateHash(deadline, reqKey);
        SHA_HASH hash = myHash;
        uint64_t opKeyAndId = 0ul;
        LogEntry* entry = NULL;
        if (isSynedReq) {
            if (maxSyncedLogIdByKey_[request.key()] > 1) {
                // There are previous (non-commutative) requests appended
                opKeyAndId = request.key();
                opKeyAndId = ((opKeyAndId << 32u) | (maxSyncedLogIdByKey_[request.key()]));
                LogEntry* prev = syncedEntriesByKey_.get(opKeyAndId);
                hash.XOR(prev->hash);
            }
            std::string result = ApplicationExecute(request);
            entry = new LogEntry(deadline, reqKey, myHash, hash, request.key(), result, request.proxyid());
            if (sendReply) {
                fastReplyQu_[(roundRobinRequestProcessIdx_++) % fastReplyQu_.size()].enqueue(entry);
            }
            uint32_t logId = maxSyncedLogId_ + 1;
            syncedEntries_.assign(logId, entry);
            syncedReq2LogId_.assign(reqKey, logId);
            maxSyncedLogIdByKey_[request.key()]++;
            opKeyAndId = request.key();
            opKeyAndId = ((opKeyAndId << 32u) | (maxSyncedLogIdByKey_[request.key()]));
            syncedEntriesByKey_.assign(opKeyAndId, entry);
            maxSyncedLogId_++;
        }
        else {
            if (maxUnSyncedLogIdByKey_[request.key()] > 1) {
                // There are previous (non-commutative) requests appended
                opKeyAndId = request.key();
                opKeyAndId = ((opKeyAndId << 32u) | (maxUnSyncedLogIdByKey_[request.key()]));
                LogEntry* prev = unsyncedEntries_.get(opKeyAndId);
                hash.XOR(prev->hash);
            }
            entry = new LogEntry(deadline, reqKey, myHash, hash, request.key(), "", request.proxyid());
            if (sendReply) {
                fastReplyQu_[(roundRobinRequestProcessIdx_++) % fastReplyQu_.size()].enqueue(entry);
            }
            uint32_t logId = maxUnSyncedLogId_ + 1;
            unsyncedEntries_.assign(logId, entry);
            unsyncedReq2LogId_.assign(reqKey, logId);
            maxUnSyncedLogIdByKey_[request.key()]++;
            unsyncedEntriesByKey_.assign(opKeyAndId, entry);
            maxUnSyncedLogId_++;
        }
    }

    void Replica::FastReplyTd(int id, int cvId) {
        LogEntry* entry = NULL;
        Reply reply;
        reply.set_view(viewId_);
        reply.set_replytype(MessageType::FAST_REPLY);
        reply.set_replicaid(replicaId_);
        bool amLeader = AmLeader();
        CrashVectorStruct* cv = crashVectorInUse_[cvId];
        uint64_t opKeyAndId = 0ul;
        while (status_ != ReplicaStatus::TERMINATED) {
            BlockWhenStatusIs(ReplicaStatus::VIEWCHANGE);
            if (fastReplyQu_[id].try_dequeue(entry)) {
                reply.set_clientid((entry->reqKey) >> 32);
                reply.set_reqid((uint32_t)(entry->reqKey));
                CrashVectorStruct* masterCV = crashVectorInUse_[0].load();
                if (cv->version_ < masterCV->version_) {
                    // My cv is stale, update it
                    crashVectorInUse_[cvId] = masterCV;
                    cv = masterCV;
                }
                if (amLeader) {
                    SHA_HASH hash = entry->hash;
                    hash.XOR(cv->cvHash_);
                    reply.set_hash(hash.hash, SHA_DIGEST_LENGTH);
                }
                else {
                    if (maxSyncedLogIdByKey_[entry->opKey] > 1) {
                        uint32_t syncPoint = maxSyncedLogIdByKey_[entry->opKey];
                        opKeyAndId = entry->opKey;
                        opKeyAndId = ((opKeyAndId << 32u) | syncPoint);
                        LogEntry* syncedEntry = NULL;
                        do {
                            // Normally, this do-while will break by just executing once
                            syncedEntry = syncedEntriesByKey_.get(opKeyAndId);
                        } while (syncedEntry == NULL);

                        uint32_t unsyncedPointStart = minUnSyncedLogIdByKey_[entry->opKey];
                        uint32_t unsyncedPointEnd = maxUnSyncedLogIdByKey_[entry->opKey];

                        if (syncedEntry->LessThan(*entry) && unsyncedPointStart <= unsyncedPointEnd) {
                            LogEntry* unsyncedEntryBoundary = NULL;
                            while (unsyncedPointStart <= unsyncedPointEnd) {
                                opKeyAndId = entry->opKey;
                                opKeyAndId = ((opKeyAndId << 32u) | unsyncedPointStart);
                                unsyncedEntryBoundary = unsyncedEntriesByKey_.get(opKeyAndId);
                                ASSERT(unsyncedEntryBoundary != NULL);
                                if (unsyncedEntryBoundary->LessThan(*syncedEntry)) {
                                    unsyncedPointStart++;
                                }
                                else {
                                    break;
                                }
                            }
                            if (unsyncedPointStart <= unsyncedPointEnd) {
                                SHA_HASH hash(entry->hash);
                                hash.XOR(unsyncedEntryBoundary->hash);
                                hash.XOR(unsyncedEntryBoundary->myhash); // so far, hash encodes all unsynced info
                                hash.XOR(syncedEntry->hash);// plus the synced info
                                hash.XOR(cv->cvHash_); // plus crash-vector to avoid stray message effect
                                reply.set_hash(hash.hash, SHA_DIGEST_LENGTH);
                            }
                        }
                        // else (very unlikely): syncedEntry has surpassed unsyncedEntry, so we do not need to send fast-reply for this entry, because (1) a slow-reply must have been sent (2) or this entry does not exist on the leader
                    }
                }
                reply.set_result(entry->result);
                Address* addr = proxyAddressMap_.get(entry->proxyId);
                if (addr != NULL) {
                    fastReplySender_[id]->SendMsgTo(*addr, reply, MessageType::FAST_REPLY);
                }
                // else (unlikely): this replica does not have the addr of the proxy
            }
        }
    }

    void Replica::SlowReplyTd(int id, int cvId) {
        if (AmLeader()) {
            // Leader does not send slow replies
            return;
        }
        LogEntry* entry = NULL;
        Reply reply;
        reply.set_view(viewId_);
        reply.set_replytype(MessageType::SLOW_REPLY);
        reply.set_replicaid(replicaId_);
        reply.set_hash("");
        reply.set_result("");
        while (status_ == ReplicaStatus::NORMAL) {
            BlockWhenStatusIs(ReplicaStatus::VIEWCHANGE);
            if (slowReplyQu_[id].try_dequeue(entry)) {
                reply.set_clientid((entry->reqKey) >> 32);
                reply.set_reqid((uint32_t)(entry->reqKey));
                // Optimize: SLOW_REPLY => COMMIT_REPLY
                if (syncedReq2LogId_.get(entry->reqKey) <= committedLogId_) {
                    reply.set_replytype(MessageType::COMMIT_REPLY);
                }

                Address* addr = proxyAddressMap_.get(entry->proxyId);
                if (addr != NULL) {
                    slowReplySender_[id]->SendMsgTo(*addr, reply, MessageType::SLOW_REPLY);
                }
            }
        }
    }

    void Replica::IndexSendTd(int id, int cvId) {
        if (!AmLeader()) {
            // Followers do not broadcast indices
            return;
        }
        uint32_t lastSyncedLogId = CONCURRENT_MAP_START_INDEX - 1;
        IndexSync indexSyncMsg;
        CrashVectorStruct* cv = crashVectorInUse_[cvId].load();
        while (status_ == ReplicaStatus::NORMAL) {
            BlockWhenStatusIs(ReplicaStatus::VIEWCHANGE);
            uint32_t logEnd = maxSyncedLogId_;
            if (lastSyncedLogId < logEnd) {
                // Leader has some indices to sync
                indexSyncMsg.set_view(this->viewId_);
                indexSyncMsg.set_logidbegin(lastSyncedLogId + 1);
                logEnd = (lastSyncedLogId + 50 < logEnd) ? (lastSyncedLogId + 50) : logEnd;
                indexSyncMsg.set_logidend(logEnd);
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
                // Send
                for (uint32_t r = 0; r < replicaNum_; r++) {
                    if (r != this->replicaNum_) {
                        indexSender_[id]->SendMsgTo(*(indexReceiver_[r]), indexSyncMsg, MessageType::SYNC_INDEX);
                    }
                }
            }
            usleep(20);
        }
    }

    void Replica::IndexRecvTd() {
        while (status_ != ReplicaStatus::TERMINATED) {
            indexSyncContext_.Register();
            indexSyncContext_.endPoint_->LoopRun();
            BlockWhenStatusIs(ReplicaStatus::VIEWCHANGE);
        }
    }

    void Replica::ReceiveIndexSyncMessage(char* msgBuffer, int msgLen) {
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
                if (maxSyncedLogId_ + 1 < idxSyncMsg.logidbegin()) {
                    pendingIndexSync_[idxSyncMsg.logidbegin()] = idxSyncMsg;
                    if (this->missedIndices_.first == 0) {
                        // We are missing some idxSyncMsgs
                       // We haven't launched the timer to ask indices
                        this->missedIndices_.first = maxSyncedLogId_ + 1;
                        this->missedIndices_.second = idxSyncMsg.logidbegin() - 1;
                        this->indexSyncContext_.endPoint_->RegisterTimer(indexAskTimer_);
                        return;
                    }
                }
                else if (maxSyncedLogId_ < idxSyncMsg.logidend()) {
                    // This idxSyncMsg is useful
                    ProcessIndexSync(idxSyncMsg);
                }
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
        if (idxSyncMsg.logidend() <= maxSyncedLogId_) {
            // This idxSyncMsg is useless 
            return true;
        }
        if (idxSyncMsg.logidbegin() > maxSyncedLogId_ + 1) {
            return false;
        }
        if (missedIndices_.first < idxSyncMsg.logidend()) {
            // Narrow the gap
            missedIndices_.first = idxSyncMsg.logidend() + 1;
        }

        if (missedIndices_.first > missedIndices_.second) {
            // We have fixed the missing gap of indices, no need to continue the indexask Timer
            if (indexSyncContext_.endPoint_->isRegistered(this->indexAskTimer_)) {
                indexSyncContext_.endPoint_->UnregisterTimer(this->indexAskTimer_);
            }
        }
        uint64_t opKeyAndId = 0ul;
        for (uint32_t logid = maxSyncedLogId_ + 1; logid <= idxSyncMsg.logidend(); logid++) {
            uint32_t offset = logid - idxSyncMsg.logidbegin();
            uint64_t reqKey = idxSyncMsg.reqkeys(offset);
            uint64_t deadline = idxSyncMsg.deadlines(offset);
            Request* req = unsyncedRequestMap_.get(reqKey);
            if (req) {
                // Find the req locally
                SHA_HASH myHash = CalculateHash(deadline, reqKey);
                SHA_HASH hash = myHash;
                if (maxSyncedLogIdByKey_[req->key()] > 1) {
                    // This req has some pre non-commutative ones
                    // In that way, XOR the previous accumulated hash
                    opKeyAndId = req->key();
                    opKeyAndId = ((opKeyAndId << 32u) | maxSyncedLogIdByKey_[req->key()]);
                    const SHA_HASH& prev = syncedEntriesByKey_.get(opKeyAndId)->hash;
                    hash.XOR(prev);
                }

                LogEntry* entry = new LogEntry(deadline, reqKey, myHash, hash, req->key(), "", req->proxyid());
                syncedRequestMap_.assign(reqKey, req);
                syncedReq2LogId_.assign(reqKey, logid);
                syncedEntries_.assign(logid, entry);
                maxSyncedLogIdByKey_[req->key()]++;
                opKeyAndId = req->key();
                opKeyAndId = ((opKeyAndId << 32u) | (maxSyncedLogIdByKey_[req->key()]));
                syncedEntriesByKey_.assign(opKeyAndId, entry);
                maxSyncedLogId_++;
                // Chunk UnSynced logs
                uint32_t m = minUnSyncedLogIdByKey_[req->key()];
                while (m <= maxUnSyncedLogIdByKey_[req->key()]) {
                    opKeyAndId = req->key();
                    opKeyAndId = ((opKeyAndId << 32u) | m);
                    LogEntry* minEntry = unsyncedEntriesByKey_.get(opKeyAndId);
                    ASSERT(minEntry != NULL);
                    if (minEntry->LessThan(*entry)) {
                        // This minEntry is useless
                        m++;
                    }
                    else {
                        break;
                    }
                }
                minUnSyncedLogIdByKey_[req->key()] = m;

            }
            else {
                this->missedReqKeys_.insert(reqKey);
            }

        }
        if (this->missedReqKeys_.empty()) {
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
        while (status_ != ReplicaStatus::TERMINATED) {
            missedIndexAckContext_.Register();
            missedIndexAckContext_.endPoint_->LoopRun();
            BlockWhenStatusIs(ReplicaStatus::VIEWCHANGE);
        }
    }

    void Replica::ReceiveAskMissedIdx(char* msgBuffer, int msgLen) {
        if (NULL == CheckMsgLength(msgBuffer, msgLen)) {
            return;
        }
        AskIndex askIndex;

        if (msgBuffer[0] == MessageType::MISSED_INDEX_ASK && askIndex.ParseFromArray(msgBuffer + sizeof(MessageHeader), msgLen - sizeof(MessageHeader))) {
            uint32_t logBegin = askIndex.logidbegin();
            while (logBegin <= maxSyncedLogId_) {
                // I can help
                IndexSync indexSyncMsg;
                indexSyncMsg.set_view(this->viewId_);
                indexSyncMsg.set_logidbegin(logBegin);
                uint32_t logEnd = maxSyncedLogId_;
                if (logEnd > askIndex.logidend()) {
                    logEnd = askIndex.logidend();
                }
                if (logEnd > logBegin + 25) {
                    logEnd = logBegin + 25;
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
        while (status_ != ReplicaStatus::TERMINATED) {
            missedReqAckContext_.Register();
            missedReqAckContext_.endPoint_->LoopRun();
            BlockWhenStatusIs(ReplicaStatus::VIEWCHANGE);
        }
    }


    void Replica::ReceiveAskMissedReq(char* msgBuffer, int msgLen) {
        if (!CheckMsgLength(msgBuffer, msgLen)) {
            return;
        }
        AskReq askReqMsg;
        if (msgBuffer[0] == MessageType::MISSED_REQ_ASK && askReqMsg.ParseFromArray(msgBuffer + sizeof(MessageHeader), msgLen - sizeof(MessageHeader))) {
            MissedReq missedReqMsg;
            missedReqMsg.set_replicaid(this->replicaId_);
            for (int i = 0; i < askReqMsg.missedreqkeys().size(); i++) {
                uint64_t reqKey = askReqMsg.missedreqkeys(i);
                Request* req = syncedRequestMap_.get(reqKey);
                if (req == NULL) {
                    req = unsyncedRequestMap_.get(reqKey);
                }
                if (req) {
                    // the req is found
                    missedReqMsg.add_reqs()->CopyFrom(*req);
                }
                if (missedReqMsg.reqs().size() > 10) {
                    break;
                }
            }
            if (missedReqMsg.reqs().size() > 0) {
                // This ack is useful, so send it
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
        }
        reqRequester_->SendMsgTo(*(requestAskReceiver_[roundRobinRequestAskIdx_ % replicaNum_]), askReqMsg, MessageType::MISSED_REQ_ASK);
        roundRobinRequestAskIdx_++;
        if (roundRobinRequestAskIdx_ % replicaNum_ == replicaId_) {
            roundRobinRequestAskIdx_++;
        }
    }


    void Replica::GarbageCollectTd() {
        while (status_ != ReplicaStatus::TERMINATED) {
            BlockWhenStatusIs(ReplicaStatus::VIEWCHANGE);
            // Reclaim stale crashVector
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
            // Reclaim stale (unsynced) log entry and requests
            if (!AmLeader()) {
                for (uint32_t i = 0; i < keyNum_; i++) {
                    while (unsyncedLogIdByKeyToClear_[i] < minUnSyncedLogIdByKey_[i]) {
                        uint64_t opKeyAndId = i;
                        opKeyAndId = ((opKeyAndId << 32u) | unsyncedLogIdByKeyToClear_[i]);
                        LogEntry* entry = unsyncedEntriesByKey_.get(opKeyAndId);
                        if (entry) {
                            uint32_t logId = unsyncedReq2LogId_.get(entry->reqKey);
                            unsyncedEntries_.erase(logId);
                            unsyncedReq2LogId_.erase(entry->reqKey);
                            if (syncedRequestMap_.get(entry->reqKey) == NULL) {
                                // We can delete the unsynced request
                                Request* request = unsyncedRequestMap_.get(entry->reqKey);
                                ASSERT(request != NULL);
                                delete request;
                            }
                            unsyncedEntriesByKey_.erase(opKeyAndId);
                            delete entry;
                        }
                        else {
                            LOG(ERROR) << "Entry not found " << i << ":" << unsyncedLogIdByKeyToClear_[i];
                        }
                        unsyncedLogIdByKeyToClear_[i]++;
                    }
                }
            }

            usleep(10000);
        }
    }

    void Replica::CheckHeartBeat() {
        if (status_ == ReplicaStatus::TERMINATED) {
            masterContext_.endPoint_->LoopBreak();
            return;
        }
        if (AmLeader()) {
            return;
        }
        if (status_ != ReplicaStatus::NORMAL) {
            // Some worker threads have detected viewchange and switch status_ to VIEWCHANGE
            // But workers have no priviledge to increment viewId_ and initiate view change process, so the master will do that
            InitiateViewChange(viewId_ + 1);
            return;
        }
        uint64_t nowTime = GetMicrosecondTimestamp();
        uint32_t threashold = replicaConfig_["heartbeat-threshold-ms"].as<uint32_t>() * 1000;
        if (lastHeartBeatTime_ + threashold < nowTime) {
            // I haven't heard from the leader for too long, it probably has died
            // Before start view change, clear context
            InitiateViewChange(viewId_ + 1);
        }
    }

    void Replica::ReceiveMasterMessage(char* msgBuffer, int msgLen, Address* sender, UDPSocketEndpoint* receiverEP) {
        MessageHeader* msgHdr = CheckMsgLength(msgBuffer, msgLen);
        if (!msgHdr) {
            return;
        }
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
                ProcessCrashVectorRequest(request);
            }
        }
        else if (msgHdr->msgType == MessageType::CRASH_VECTOR_REPLY) {
            CrashVectorReply reply;
            if (reply.ParseFromArray(msgBuffer + sizeof(MessageHeader), msgHdr->msgLen)) {
                ProcessCrashVectorReply(reply);
            }
        }
        else if (msgHdr->msgType == MessageType::RECOVERY_REQUEST) {
            RecoveryRequest request;
            if (request.ParseFromArray(msgBuffer + sizeof(MessageHeader), msgHdr->msgLen)) {
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
        ViewChange viewChangeMsg;
        viewChangeMsg.set_view(viewId_);
        viewChangeMsg.set_replicaid(replicaId_);
        CrashVectorStruct* cv = crashVectorInUse_[0].load();
        viewChangeMsg.mutable_cv()->Add(cv->cv_.begin(), cv->cv_.end());
        viewChangeMsg.set_syncpoint(maxSyncedLogId_);
        viewChangeMsg.set_unsyncedlogbegin(minUnSyncedLogId_);
        viewChangeMsg.set_unsyncedlogend(maxUnSyncedLogId_);
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
        viewId_ = view;
        status_ = ReplicaStatus::VIEWCHANGE;

        // Wait until every worker stop
        while (activeWorkerNum_ > 0) {
            usleep(1000);
        }
        // Launch the timer
        if (masterContext_.endPoint_->isRegistered(viewChangeTimer_) == false) {
            masterContext_.endPoint_->RegisterTimer(viewChangeTimer_);
        }
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
                masterContext_.endPoint_->SendMsgTo(*(masterReceiver_[i]), startView, MessageType::STATE_TRANSFER_REQUEST);
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
        if (status_ == ReplicaStatus::RECOVERING) {
            // Recovering replicas do not participate in view change
            return;
        }
        if (!CheckCV(viewChange.replicaid(), viewChange.cv())) {
            // stray message
            return;
        }

        Aggregated(viewChange.cv());

        if (status_ == ReplicaStatus::NORMAL) {
            if (viewChange.view() > viewId_) {
                InitiateViewChange(viewChange.view());
            }
            else {
                // The sender lags behind
                SendStartView(viewChange.replicaid());
            }
        }
        else if (status_ == ReplicaStatus::VIEWCHANGE) {
            if (viewChange.view() > viewId_) {
                InitiateViewChange(viewChange.view());
            }
            else if (viewChange.view() < viewId_) {
                SendViewChangeRequest(viewChange.replicaid());
            }
            else {
                viewChangeSet_[viewChange.replicaid()] = viewChange;
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
                    myvc.set_unsyncedlogbegin(minUnSyncedLogId_);
                    myvc.set_unsyncedlogend(maxUnSyncedLogId_);
                    myvc.set_lastnormalview(lastNormalView_);
                    viewChangeSet_[replicaId_] = myvc;
                    TransferSyncedLog();
                }

            }
        }
        else {
            LOG(WARNING) << "Unexpected Status " << status_;
        }
    }

    void Replica::TransferSyncedLog() {

        uint32_t largestNormalView = viewChangeSet_.begin()->second.lastnormalview();
        uint32_t largestSyncPoint = CONCURRENT_MAP_START_INDEX;
        for (auto& kv : viewChangeSet_) {
            if (largestNormalView < kv.second.view()) {
                largestNormalView = kv.second.view();
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
        // Directly copy the synced entries
        if (largestNormalView == this->lastNormalView_) {
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
        // TODO: If this process cannot be completed, rollback to view change
        stateTransferIndices_.clear();
        for (auto& kv : viewChangeSet_) {
            stateTransferIndices_[kv.first] = { kv.second.unsyncedlogbegin(), kv.second.unsyncedlogend() };
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
    }


    void Replica::SendStateTransferRequest() {
        // TODO: If statetransfer cannot be completed within a certain amount of time, rollback to view change
        if (GetMicrosecondTimestamp() >= stateTransferTerminateTime_) {
            masterContext_.endPoint_->UnregisterTimer(stateTransferTimer_);
            stateTransferTerminateCallback_();
            return;
        }

        StateTransferRequest request;
        request.set_view(viewId_);
        request.set_replicaid(replicaId_);
        request.set_issynced(transferSyncedEntry_);
        if (!transferSyncedEntry_) {
            LogEntry* entry = syncedEntries_.get(maxSyncedLogId_);
            request.add_syncpoint(entry->deadline);
            request.add_syncpoint(entry->reqKey);
        }
        for (auto& stateTransferInfo : stateTransferIndices_) {
            // Do not request too many entries at one time, otherwise, UDP packet cannot handle that
            request.set_logbegin(stateTransferInfo.second.first);
            if (stateTransferInfo.second.first + stateRequestTransferBatch_ <= stateTransferInfo.second.second) {
                request.set_logend(stateTransferInfo.second.first + stateRequestTransferBatch_);
            }
            else {
                request.set_logend(stateTransferInfo.second.second);
            }

            masterContext_.endPoint_->SendMsgTo(*(masterReceiver_[stateTransferInfo.first]), request, MessageType::STATE_TRANSFER_REQUEST);
        }

    }

    void Replica::ProcessStateTransferRequest(const StateTransferRequest& stateTransferRequest) {
        if (stateTransferRequest.view() != viewId_) {
            if (stateTransferRequest.view() > viewId_) {
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
        ConcurrentMap<uint64_t, Request*>& requestMap = reply.issynced() ? syncedRequestMap_ : unsyncedRequestMap_;
        ConcurrentMap<uint32_t, LogEntry*>& entryMap = reply.issynced() ? syncedEntries_ : unsyncedEntries_;

        // For Debug: Should be deleted after testing
        if (reply.issynced()) {
            ASSERT(maxSyncedLogId_ >= reply.logend());
        }
        else {
            ASSERT(maxUnSyncedLogId_ >= reply.logend() && minUnSyncedLogId_ <= reply.logbegin());
        }

        std::pair<uint64_t, uint64_t> syncPoint(0, 0);
        if (stateTransferRequest.syncpoint().size() >= 2) {
            syncPoint.first = stateTransferRequest.syncpoint(0);
            syncPoint.second = stateTransferRequest.syncpoint(1);
        }

        for (uint32_t j = reply.logbegin(); j <= reply.logend(); j++) {
            LogEntry* entry = NULL;
            Request* request = NULL;
            do {
                entry = entryMap.get(j);
            } while (entry == NULL);
            do {
                request = requestMap.get(entry->reqKey);
            } while (request == NULL);

            std::pair<uint64_t, uint64_t> key(entry->deadline, entry->reqKey);
            if (key <= syncPoint) {
                ASSERT(reply.issynced() == false);
                continue;
            }
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
            // Old View: ignore
            return;
        }
        else if (stateTransferReply.view() > viewId_) {
            masterContext_.endPoint_->UnregisterTimer(stateTransferTimer_);
            if (status_ == ReplicaStatus::RECOVERING) {
                // This state transfer is useless, stop it and restart recovery request
                masterContext_.endPoint_->RegisterTimer(recoveryRequestTimer_);
            }
            else if (status_ == ReplicaStatus::VIEWCHANGE) {
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
            for (int i = 0; i < stateTransferReply.reqs().size(); i++) {
                uint64_t deadline = stateTransferReply.reqs(i).sendtime();
                uint64_t reqKey = stateTransferReply.reqs(i).clientid();
                reqKey = ((reqKey << 32u) | (stateTransferReply.reqs(i).reqid()));
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

        iter->second.first = stateTransferReply.logend();
        if (iter->second.first >= iter->second.second) {
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
        if (!CheckCV(startView.replicaid(), startView.cv())) {
            return;
        }
        else {
            Aggregated(startView.cv());
        }

        if (status_ == ReplicaStatus::VIEWCHANGE) {
            if (startView.view() > viewId_) {
                InitiateViewChange(startView.view());
            }
            else if (startView.view() == viewId_) {
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
            return;
        }

        if (nonce_ != reply.nonce()) {
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

        }
        masterContext_.endPoint_->UnregisterTimer(crashVectorRequestTimer_);
        // Start Recovery Request
        masterContext_.endPoint_->RegisterTimer(recoveryRequestTimer_);
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
        reply.set_replicaid(request.replicaid());
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
            if (AmLeader()) {
                // If the recoverying replica happens to be the leader of the new view, don't participate. Wait until the healthy replicas elect a new leader
                recoveryReplySet_.clear();
                usleep(1000); // sleep some time and restart the recovery process
                masterContext_.endPoint_->RegisterTimer(recoveryRequestTimer_);
            }
            else {
                // Launch state transfer
                stateTransferIndices_.clear();
                stateTransferIndices_[maxView % replicaNum_] = { 2, syncedLogId };
                stateTransferCallback_ = std::bind(&Replica::EnterNewView, this);
                transferSyncedEntry_ = true;
                stateTransferTerminateTime_ = GetMicrosecondTimestamp() + stateTransferTimeout_ * 1000;
                stateTransferTerminateCallback_ = std::bind(&Replica::RollbackToRecovery, this);
                masterContext_.endPoint_->RegisterTimer(stateTransferTimer_);
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
            InitiateViewChange(view);
            return false;
        }
        return true;
    }

    bool Replica::CheckCV(const uint32_t senderId, const google::protobuf::RepeatedField<uint32_t>& cv) {
        CrashVectorStruct* masterCV = crashVectorInUse_[0].load();
        return (cv.at(senderId) < masterCV->cv_[senderId]);
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
                    for (uint32_t i = 1; i <= fastReplyQu_.size() + slowReplyQu_.size(); i++) {
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

