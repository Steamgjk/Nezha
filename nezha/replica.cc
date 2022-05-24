#include "nezha/replica.h"

namespace nezha {
    Replica::Replica(const std::string& configFile)
    {
        // Load Config
        replicaConfig_ = YAML::LoadFile(configFile);

        viewNum_ = 0;
        lastNormalView_ = 0;
        replicaId_ = replicaConfig_["replica-id"].as<int>();
        replicaNum_ = replicaConfig_["replica-ips"].size();
        uint32_t keyNum = replicaConfig_["key-num"].as<uint32_t>();
        lastReleasedEntryByKeys_.resize(keyNum, std::pair<uint64_t, uint64_t>(0, 0));
        // Since ConcurrentMap reseres 0 and 1, we can only use log-id from 2
        maxSyncedLogId_ = 1;
        minUnSyncedLogId_ = 2;
        maxUnSyncedLogId_ = 1;
        // syncedLogIdByKey_.resize(keyNum, 1);
        // unsyncedLogIdByKey_.resize(keyNum, 1);

        maxSyncedLogIdByKey_.resize(keyNum, 1);
        minUnSyncedLogIdByKey_.resize(keyNum, 2);
        maxUnSyncedLogIdByKey_.resize(keyNum, 1);


        status_ = ReplicaStatus::NORMAL;
        LOG(INFO) << "viewNum_=" << viewNum_
            << "\treplicaId=" << replicaId_
            << "\treplicaNum=" << replicaNum_
            << "\tkeyNum=" << keyNum;


        CreateContext();
        // Launch Threads (based on Config)
        LaunchThreads();

        // Master thread run
        masterContext_.endPoint_->LoopRun();

    }

    Replica::~Replica()
    {
        for (auto& kv : threadPool_) {
            delete kv.second;
            LOG(INFO) << "Deleted\t" << kv.first;
        }
    }

    void Replica::CreateContext() {
        // Create master endpoints and context
        std::string ip = replicaConfig_["replica-ips"][replicaId_.load()].as<std::string>();
        int port = replicaConfig_["master-port"].as<int>();
        int monitorPeriodMs = replicaConfig_["monitor-period-ms"].as<int>();
        masterContext_.endPoint_ = new UDPSocketEndpoint(ip, port, true);
        masterContext_.msgHandler_ = new MsgHandlerStruct([](char* msgBuffer, int bufferLen, Address* sender, void* ctx, UDPSocketEndpoint* receiverEP) {
            ((Replica*)ctx)->ReceiverOtherMessage(msgBuffer, bufferLen, sender, receiverEP);
            }, this);
        // Register a timer to monitor replica status
        masterContext_.monitorTimer_ = new TimerStruct([](void* ctx, UDPSocketEndpoint* receiverEP) {
            ((Replica*)ctx)->CheckHeartBeat();
            }, this, monitorPeriodMs);
        masterContext_.Register();

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
            requestContext_[i].Register();
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
        indexSyncContext_.Register();

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
        missedIndexAckContext_.Register();

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
        missedReqAckContext_.Register();

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
        crashVector_.assign(2, cv);
        uint32_t cvVecSize = 1 + replyShardNum * 2 + replicaConfig_["index-sync-shards"].as<int>() + 2;
        indexRecvCVIdx_ = cvVecSize - 2;
        indexAckCVIdx_ = cvVecSize - 1;
        crashVectorInUse_ = new std::atomic<CrashVectorStruct*>[cvVecSize];
        for (uint32_t i = 0; i < cvVecSize; i++) {
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
            ((Replica*)ctx)->InitiateViewChange();
            }, this, replicaConfig_["view-change-period-ms"].as<int>());
    }


    void Replica::LaunchThreads() {
        // RequestReceive
        for (int i = 0; i < replicaConfig_["receiver-shards"].as<int>(); i++) {
            std::thread* td = new std::thread(&Replica::ReceiveTd, this, i);
            std::string key("ReceiveTd-" + std::to_string(i));
            threadPool_[key] = td;
            LOG(INFO) << "Launched " << key << "\t" << td->native_handle();
        }

        // RequestProcess
        for (int i = 0; i < replicaConfig_["process-shards"].as<int>(); i++) {
            std::thread* td = new std::thread(&Replica::ProcessTd, this, i);
            std::string key("ProcessTd-" + std::to_string(i));
            threadPool_[key] = td;
            LOG(INFO) << "Launched " << key << "\t" << td->native_handle();
        }

        // RequestReply
        int replyShardNum = replicaConfig_["reply-shards"].as<int>();
        for (int i = 0; i < replyShardNum; i++) {
            std::thread* td = new std::thread(&Replica::FastReplyTd, this, i, i + 1);
            std::string key("FastReplyTd-" + std::to_string(i));
            threadPool_[key] = td;
            LOG(INFO) << "Launched " << key << "\t" << td->native_handle();
        }
        for (int i = 0; i < replyShardNum; i++) {
            std::thread* td = new std::thread(&Replica::SlowReplyTd, this, i, i + replyShardNum + 1);
            std::string key("SlowReplyTd-" + std::to_string(i));
            threadPool_[key] = td;
            LOG(INFO) << "Launched " << key << "\t" << td->native_handle();
        }

        // IndexSync
        for (int i = 0; i < replicaConfig_["index-sync-shards"].as<int>(); i++) {
            std::thread* td = new std::thread(&Replica::IndexSendTd, this, i, i + replyShardNum * 2 + 1);
            std::string key("IndexSendTd-" + std::to_string(i));
            threadPool_[key] = td;
            LOG(INFO) << "Launched " << key << "\t" << td->native_handle();
            if (!AmLeader()) {
                // follower only needs one sync thread
                break;
            }
        }

        threadPool_["IndexRecvTd"] = new std::thread(&Replica::IndexRecvTd, this);
        LOG(INFO) << "Launched IndexRecvTd\t" << threadPool_["IndexRecvTd"]->native_handle();

        threadPool_["MissedIndexAckTd"] = new std::thread(&Replica::MissedIndexAckTd, this);
        LOG(INFO) << "Launched MissedIndexAckTd\t" << threadPool_["MissedIndexAckTd"]->native_handle();

        threadPool_["MissedReqAckTd"] = new std::thread(&Replica::MissedReqAckTd, this);
        LOG(INFO) << "Launched MissedReqAckTd\t" << threadPool_["MissedReqAckTd"]->native_handle();
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

    void Replica::ReceiveTd(int id) {
        workerCounter_.fetch_add(1);
        requestContext_[id].endPoint_->LoopRun();
        workerCounter_.fetch_sub(1);
    }

    void Replica::ProcessTd(int id) {
        workerCounter_.fetch_add(1);
        uint64_t reqKey = 0;
        Request* req = NULL;
        uint32_t roundRobin = 0;
        uint32_t replyShard = replicaConfig_["reply-shards"].as<uint32_t>();
        bool amLeader = AmLeader();
        while (status_ == ReplicaStatus::NORMAL) {
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
                        fastReplyQu_[(roundRobin++) % replyShard].enqueue(entry);
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
                        slowReplyQu_[(roundRobin++) % replyShard].enqueue(entry);
                    }
                    else {
                        duplicateLogIdx = unsyncedReq2LogId_.get(reqKey);
                        if (duplicateLogIdx > 0) {
                            // Duplicate: resend fast-reply for this request
                            LogEntry* entry = unsyncedEntries_.get(duplicateLogIdx);
                            // Update proxy id in case the client has changed its proxy
                            entry->proxyId = req->proxyid();
                            fastReplyQu_[(roundRobin++) % replyShard].enqueue(entry);
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
            uint64_t opKeyAndId = 0ul;
            while ((!earlyBuffer_.empty()) && nowTime >= earlyBuffer_.begin()->first.first) {
                uint64_t deadline = earlyBuffer_.begin()->first.first;
                uint64_t reqKey = earlyBuffer_.begin()->first.second;
                Request* req = earlyBuffer_.begin()->second;
                SHA_HASH myHash = CalculateHash(deadline, reqKey);
                SHA_HASH hash = myHash;
                if (amLeader) {
                    if (maxSyncedLogIdByKey_[req->key()] > 1) {
                        // There are previous (non-commutative) requests appended
                        opKeyAndId = req->key();
                        opKeyAndId = ((opKeyAndId << 32u) | (maxSyncedLogIdByKey_[req->key()]));
                        LogEntry* prev = syncedEntriesByKey_.get(opKeyAndId);
                        hash.XOR(prev->hash);
                    }
                    std::string result = ApplicationExecute(req);
                    LogEntry* entry = new LogEntry(deadline, reqKey, myHash, hash, req->key(), result, req->proxyid());
                    fastReplyQu_[(roundRobin++) % replyShard].enqueue(entry);
                    uint32_t logId = maxSyncedLogId_ + 1;
                    syncedEntries_.assign(logId, entry);
                    syncedReq2LogId_.assign(reqKey, logId);
                    maxSyncedLogIdByKey_[req->key()]++;
                    opKeyAndId = req->key();
                    opKeyAndId = ((opKeyAndId << 32u) | (maxSyncedLogIdByKey_[req->key()]));
                    syncedEntriesByKey_.assign(opKeyAndId, entry);
                    maxSyncedLogId_++;
                }
                else {
                    if (maxUnSyncedLogIdByKey_[req->key()] > 1) {
                        // There are previous (non-commutative) requests appended
                        opKeyAndId = req->key();
                        opKeyAndId = ((opKeyAndId << 32u) | (maxUnSyncedLogIdByKey_[req->key()]));
                        LogEntry* prev = unsyncedEntries_.get(opKeyAndId);
                        hash.XOR(prev->hash);
                    }
                    LogEntry* entry = new LogEntry(deadline, reqKey, myHash, hash, req->key(), "", req->proxyid());
                    fastReplyQu_[(roundRobin++) % replyShard].enqueue(entry);
                    uint32_t logId = maxUnSyncedLogId_ + 1;
                    unsyncedEntries_.assign(logId, entry);
                    unsyncedReq2LogId_.assign(reqKey, logId);
                    maxUnSyncedLogIdByKey_[req->key()]++;
                    unsyncedEntriesByKey_.assign(opKeyAndId, entry);
                    maxUnSyncedLogId_++;
                }
                earlyBuffer_.erase(earlyBuffer_.begin());
            }
        }
        // When this thread exits (for view change), decrease the atomic variable workerCounter_
        workerCounter_.fetch_sub(1);
    }

    void Replica::FastReplyTd(int id, int cvId) {
        workerCounter_.fetch_add(1);
        LogEntry* entry = NULL;
        Reply reply;
        reply.set_view(viewNum_);
        reply.set_replytype(MessageType::FAST_REPLY);
        reply.set_replicaid(replicaId_);
        bool amLeader = AmLeader();
        CrashVectorStruct* cv = crashVectorInUse_[cvId];
        char buffer[UDP_BUFFER_SIZE];
        MessageHeader* replyHeader = (MessageHeader*)(void*)buffer;
        replyHeader->msgType = MessageType::FAST_REPLY;
        uint64_t opKeyAndId = 0ul;
        while (status_ == ReplicaStatus::NORMAL) {
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
                                assert(unsyncedEntryBoundary != NULL);
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

                std::string serializedString = reply.SerializeAsString();
                replyHeader->msgLen = serializedString.length();

                Address* addr = proxyAddressMap_.get(entry->proxyId);
                if (addr != NULL && serializedString.length() > 0) {
                    memcpy(buffer + sizeof(MessageHeader), serializedString.c_str(), serializedString.length());
                    fastReplySender_[id]->SendMsgTo(*addr, buffer, replyHeader->msgLen + sizeof(MessageHeader));
                }
                // else (unlikely): this replica does not have the addr of the proxy
            }
        }
        workerCounter_.fetch_sub(1);
    }

    void Replica::SlowReplyTd(int id, int cvId) {
        if (AmLeader()) {
            // Leader does not send slow replies
            return;
        }
        workerCounter_.fetch_add(1);
        char buffer[UDP_BUFFER_SIZE];
        LogEntry* entry = NULL;
        Reply reply;
        reply.set_view(viewNum_);
        reply.set_replytype(MessageType::SLOW_REPLY);
        reply.set_replicaid(replicaId_);
        reply.set_hash("");
        reply.set_result("");
        MessageHeader* replyHeader = (MessageHeader*)(void*)buffer;
        replyHeader->msgType = MessageType::SLOW_REPLY;

        while (status_ == ReplicaStatus::NORMAL) {
            if (slowReplyQu_[id].try_dequeue(entry)) {
                reply.set_clientid((entry->reqKey) >> 32);
                reply.set_reqid((uint32_t)(entry->reqKey));
                // Optimize: SLOW_REPLY => COMMIT_REPLY
                if (syncedReq2LogId_.get(entry->reqKey) <= committedLogId_) {
                    reply.set_replytype(MessageType::COMMIT_REPLY);
                }

                Address* addr = proxyAddressMap_.get(entry->proxyId);

                std::string serializedString = reply.SerializeAsString();
                replyHeader->msgLen = serializedString.length();
                if (addr != NULL && serializedString.length() > 0) {
                    memcpy(buffer + sizeof(MessageHeader), serializedString.c_str(), serializedString.length());
                    slowReplySender_[id]->SendMsgTo(*addr, buffer, replyHeader->msgLen + sizeof(MessageHeader));
                }
            }
        }

        workerCounter_.fetch_sub(1);
    }

    void Replica::IndexSendTd(int id, int cvId) {
        if (!AmLeader()) {
            // Followers do not broadcast indices
            return;
        }
        workerCounter_.fetch_add(1);
        uint32_t lastSyncedLogId = 2;
        IndexSync indexSyncMsg;
        CrashVectorStruct* cv = crashVectorInUse_[cvId].load();
        char buffer[UDP_BUFFER_SIZE];
        MessageHeader* msgHdr = (MessageHeader*)buffer;
        msgHdr->msgType = MessageType::SYNC_INDEX;
        while (status_ == ReplicaStatus::NORMAL) {
            uint32_t logEnd = maxSyncedLogId_;
            if (lastSyncedLogId < logEnd) {
                // Leader has some indices to sync
                indexSyncMsg.set_view(this->viewNum_);
                indexSyncMsg.set_logidbegin(lastSyncedLogId + 1);
                logEnd = (lastSyncedLogId + 50 < logEnd) ? (lastSyncedLogId + 50) : logEnd;
                indexSyncMsg.set_logidend(logEnd);
                for (uint32_t i = indexSyncMsg.logidbegin(); i <= indexSyncMsg.logidend(); i++) {
                    LogEntry* entry = syncedEntries_.get(i);
                    assert(entry != NULL);
                    indexSyncMsg.add_deadlines(entry->deadline);
                    indexSyncMsg.add_reqkeys(entry->reqKey);
                }

                CrashVectorStruct* masterCV = crashVectorInUse_[0].load();
                if (cv->version_ < masterCV->version_) {
                    crashVectorInUse_[cvId] = masterCV;
                    cv = masterCV;
                }
                // Add cv to broadcast
                for (uint32_t i = 0; i < 5; i++) {
                    indexSyncMsg.add_cv(cv->cvHash_.item[i]);
                }

                std::string serializedString = indexSyncMsg.SerializeAsString();
                msgHdr->msgLen = serializedString.length();
                memcpy(buffer + sizeof(MessageHeader), serializedString.c_str(), serializedString.length());

                // Send
                for (uint32_t r = 0; r < replicaNum_; r++) {
                    if (r != this->replicaNum_) {
                        indexSender_[id]->SendMsgTo(*(indexReceiver_[r]), buffer, msgHdr->msgLen + sizeof(MessageHeader));
                    }
                }
            }
            usleep(20);
        }
        workerCounter_.fetch_sub(1);
    }

    void Replica::IndexRecvTd() {
        workerCounter_.fetch_add(1);
        indexSyncContext_.endPoint_->LoopRun();
        workerCounter_.fetch_sub(1);
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
                    assert(minEntry != NULL);
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
        workerCounter_.fetch_add(1);
        missedIndexAckContext_.endPoint_->LoopRun();
        workerCounter_.fetch_sub(1);
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
                indexSyncMsg.set_view(this->viewNum_);
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

                for (uint32_t i = 0; i < replicaNum_; i++) {
                    indexSyncMsg.add_cv(cv->cv_[i]);
                }

                char buffer[UDP_BUFFER_SIZE];
                std::string serializedString = indexSyncMsg.SerializeAsString();
                MessageHeader* msgHdr = (MessageHeader*)(void*)buffer;
                msgHdr->msgType = MessageType::SYNC_INDEX;
                msgHdr->msgLen = serializedString.length();

                if (serializedString.length() > 0) {
                    memcpy(buffer + sizeof(MessageHeader), serializedString.c_str(), serializedString.length());
                    indexAcker_->SendMsgTo(*(indexReceiver_[askIndex.replicaid()]), buffer, msgHdr->msgLen + sizeof(MessageHeader));
                }
                logBegin = logEnd + 1;
            }
        }
    }

    void Replica::MissedReqAckTd() {
        workerCounter_.fetch_add(1);
        missedReqAckContext_.endPoint_->LoopRun();
        workerCounter_.fetch_sub(1);
    }


    // TODO: ack to request receiver
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
                char buffer[UDP_BUFFER_SIZE];
                std::string serializedString = missedReqMsg.SerializeAsString();
                MessageHeader* msgHdr = (MessageHeader*)(void*)buffer;
                msgHdr->msgType = MessageType::MISSED_REQ;
                msgHdr->msgLen = serializedString.length();

                if (serializedString.length() > 0) {
                    memcpy(buffer + sizeof(MessageHeader), serializedString.c_str(), serializedString.length());
                    missedReqAckContext_.endPoint_->SendMsgTo(*(indexReceiver_[askReqMsg.replicaid()]), buffer, msgHdr->msgLen + sizeof(MessageHeader));
                }

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
        std::string serializedString = askIndexMsg.SerializeAsString();
        char buffer[UDP_BUFFER_SIZE];
        MessageHeader* msgHdr = (MessageHeader*)(void*)msgHdr;
        msgHdr->msgType = MessageType::MISSED_INDEX_ASK;
        msgHdr->msgLen = serializedString.length();
        if (serializedString.length() > 0) {
            memcpy(buffer + sizeof(MessageHeader), serializedString.c_str(), serializedString.length());
            // Do not ask leader every time, choose random replica to ask to avoid leader bottleneck
            reqRequester_->SendMsgTo(*(requestAskReceiver_[roundRobinIndexAskIdx_ % replicaNum_]), buffer, msgHdr->msgLen + sizeof(MessageHeader));
            roundRobinIndexAskIdx_++;
            if (roundRobinIndexAskIdx_ % replicaNum_ == replicaId_) {
                roundRobinIndexAskIdx_++;
            }
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
        std::string serializedString = askReqMsg.SerializeAsString();
        char buffer[UDP_BUFFER_SIZE];
        MessageHeader* msgHdr = (MessageHeader*)(void*)msgHdr;
        msgHdr->msgType = MessageType::MISSED_REQ_ASK;
        msgHdr->msgLen = serializedString.length();
        if (serializedString.length() > 0) {
            // Do not ask leader every time, choose random replica to ask to avoid leader bottleneck
            memcpy(buffer + sizeof(MessageHeader), serializedString.c_str(), serializedString.length());
            reqRequester_->SendMsgTo(*(requestAskReceiver_[roundRobinRequestAskIdx_ % replicaNum_]), buffer, msgHdr->msgLen + sizeof(MessageHeader));
            roundRobinRequestAskIdx_++;
            if (roundRobinRequestAskIdx_ % replicaNum_ == replicaId_) {
                roundRobinRequestAskIdx_++;
            }
        }

    }

    void Replica::CheckHeartBeat() {
        if (AmLeader()) {
            return;
        }
        if (status_ != ReplicaStatus::NORMAL) {
            StartViewChange();
            return;
        }
        uint64_t nowTime = GetMicrosecondTimestamp();
        uint32_t threashold = replicaConfig_["heartbeat-threshold-ms"].as<uint32_t>() * 1000;
        if (lastHeartBeatTime_ + threashold < nowTime) {
            // I haven't heard from the leader for too long, it probably has died
            // Before start view change, clear context
            StartViewChange();
        }
    }

    void Replica::ReceiverOtherMessage(char* msgBuffer, int msgLen, Address* sender, UDPSocketEndpoint* receiverEP) {
        MessageHeader* msgHdr = CheckMsgLength(msgBuffer, msgLen);
        if (!msgHdr) {
            return;
        }
        if (msgBuffer[0] == MessageType::VIEWCHANGE_REQ) {
            ViewChangeRequest viewChangeReq;
            if (viewChangeReq.ParseFromArray(msgBuffer + sizeof(MessageHeader), msgHdr->msgLen)) {
                ProcessViewChangeReq(viewChangeReq);
            }

        }
        else {
            LOG(WARNING) << "Unexpected message type " << (int)msgBuffer[0];
        }

    }

    void Replica::StartViewChange() {
        status_ = ReplicaStatus::VIEWCHANGE;
        if (masterContext_.endPoint_->isRegistered(viewChangeTimer_)) {
            // Already launched viewchange timer
            return;
        }

        // ViewChange has not been initiated, so let's do that
        while (workerCounter_ > 0) {
            // Wait until all workers have exit
            usleep(1000);
        }
        // Every other threads have stopped, no worry about data race any more
        viewNum_++;
        // Initiate the ViewChange
        masterContext_.endPoint_->RegisterTimer(viewChangeTimer_);

    }

    void Replica::InitiateViewChange() {
        // Broadcast VIEW-CHANGE-REQ to all replicas
        // VIEW-CHANGE-REQ and VIEW-CHANGE are combined here (refer to Algorithm-2 in the paper)
        ViewChangeRequest viewChangeReq;
        viewChangeReq.set_view(viewNum_);
        viewChangeReq.set_replicaid(replicaId_);
        CrashVectorStruct* cv = crashVectorInUse_[0].load();
        for (uint32_t i = 0; i < replicaNum_; i++) {
            viewChangeReq.add_cv(cv->cv_[i]);
        }
        viewChangeReq.set_syncpoint(maxSyncedLogId_);
        viewChangeReq.set_lastnormalview(lastNormalView_);

        std::string serializedString = viewChangeReq.SerializeAsString();
        char buffer[UDP_BUFFER_SIZE];
        MessageHeader* msgHdr = (MessageHeader*)(void*)buffer;
        msgHdr->msgType = MessageType::VIEWCHANGE_REQ;
        msgHdr->msgLen = serializedString.length();
        memcpy(buffer + sizeof(MessageHeader), serializedString.c_str(), serializedString.length());

        for (uint32_t i = 0; i < replicaNum_; i++) {
            if (i != replicaId_) {
                // no need to send to myself
                masterContext_.endPoint_->SendMsgTo(*(masterReceiver_[i]), buffer, msgHdr->msgLen + sizeof(MessageHeader));
            }

        }

    }

    void Replica::ProcessViewChangeReq(const ViewChangeRequest& viewChangeReq)
    {
        // if (!CheckViewAndCV(viewChangeReq.view(), viewChangeReq.cv())) {
        //     return;
        // }


    }

    bool Replica::CheckViewAndCV(const uint32_t view, const google::protobuf::RepeatedField<uint32_t>& cv) {
        if (view < this->viewNum_) {
            // old message
            return false;
        }
        if (view > this->viewNum_) {
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
    std::string Replica::ApplicationExecute(Request* req) {
        return "";
    }
    bool Replica::AmLeader() {
        return (viewNum_ % replicaNum_ == replicaId_);
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

