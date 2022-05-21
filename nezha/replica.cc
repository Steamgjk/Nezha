#include "nezha/replica.h"

namespace nezha {
    Replica::Replica(const std::string& configFile)
    {
        // Load Config
        replicaConfig_ = YAML::LoadFile(configFile);

        viewNum_ = 0;
        replicaId_ = replicaConfig_["replica-id"].as<int>();
        replicaNum_ = replicaConfig_["replica-ips"].size();
        uint32_t keyNum = replicaConfig_["key-num"].as<uint32_t>();
        lastReleasedEntryByKeys_.resize(keyNum, std::pair<uint64_t, uint64_t>(0, 0));
        // Since ConcurrentMap reseres 0 and 1, we can only use log-id from 2
        maxSyncedLogId_ = 1;
        minUnSyncedLogId_ = 2;
        maxUnSyncedLogId_ = 1;
        syncedLogIdByKey_.resize(keyNum, 1);
        unsyncedLogIdByKey_.resize(keyNum, 1);

        status_ = NORMAL;
        LOG(INFO) << "viewNum_=" << viewNum_
            << "\treplicaId=" << replicaId_
            << "\treplicaNum=" << replicaNum_
            << "\tkeyNum=" << keyNum;


        CreateContext();
        // // Launch Threads (based on Config)
        // LaunchThreads();

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
        masterContext_.endPoint_ = new UDPSocketEndpoint(ip, port, true);
        masterContext_.msgHandler_ = new MsgHandlerStruct([](char* msgBuffer, int bufferLen, Address* sender, void* ctx, UDPSocketEndpoint* receiverEP) {
            ((Replica*)ctx)->ReceiverOtherMessage(msgBuffer, bufferLen, sender, receiverEP);
            }, this);
        // Register a timer to monitor replica status
        masterContext_.monitorTimer_ = new TimerStruct([](void* ctx, UDPSocketEndpoint* receiverEP) {
            // ???
            }, this, 10);
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
                if (((Replica*)ctx)->status_ != NORMAL) {
                    receiverEP->LoopBreak();
                }
                }, this, 10);
            requestContext_[i].Register();
        }

        // (Leader) Use these endpoints to broadcast indices
        for (int i = 0; i < replicaConfig_["index-sync-shards"].as<int>(); i++) {
            indexSender_.push_back(new UDPSocketEndpoint());
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
            if (((Replica*)ctx)->status_ != NORMAL) {
                receiverEP->LoopBreak();
            }
            }, this, 10);
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
            if (((Replica*)ctx)->status_ != NORMAL) {
                receiverEP->LoopBreak();
            }
            }, this, 10);
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
            if (((Replica*)ctx)->status_ != NORMAL) {
                receiverEP->LoopBreak();
            }
            }, this, 10);
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
        cvVersionInUse_ = new std::atomic<uint32_t>[replyShardNum * 2 + 1];
        for (int i = 0; i < replyShardNum * 2 + 1; i++) {
            cvVersionInUse_[i] = 0;
        }

        // Create other useful timers

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
            std::thread* td = new std::thread(&Replica::IndexSendTd, this, i);
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
        if (msgLen <= 0) {
            LOG(WARNING) << "\tmsgLen=" << msgLen;
            return;
        }
        if (msgBuffer[0] == CLIENT_REQUEST) {
            uint32_t requestLen = 0;
            memcpy(&requestLen, msgBuffer + 1, sizeof(uint32_t));
            if (requestLen + 1 + sizeof(uint32_t) == (uint32_t)msgLen) {
                Request* request = new Request();
                if (request->ParseFromArray(msgBuffer + 1 + sizeof(uint32_t), requestLen)) {
                    if (proxyAddressMap_.get(request->proxyid()) == 0) {
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
                LOG(WARNING) << "Incomplete message: expected length " << requestLen + 1 + sizeof(uint32_t) << "\tbut got " << msgLen;
            }
        }
        else {
            LOG(WARNING) << "Invalid message type: expected " << (int)CLIENT_REQUEST << "\tbut got " << (int)(msgBuffer[0]);
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
        while (status_ == NORMAL) {
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
            while ((!earlyBuffer_.empty()) && nowTime >= earlyBuffer_.begin()->first.first) {
                uint64_t deadline = earlyBuffer_.begin()->first.first;
                uint64_t reqKey = earlyBuffer_.begin()->first.second;
                Request* req = earlyBuffer_.begin()->second;
                SHA_HASH myHash = CalculateHash(deadline, reqKey);
                SHA_HASH hash = myHash;
                if (amLeader) {
                    if (syncedLogIdByKey_[req->key()] > 1) {
                        // There are previous (non-commutative) requests appended
                        LogEntry* prev = syncedEntries_.get(syncedLogIdByKey_[req->key()]);
                        hash.XOR(prev->hash);
                    }
                    std::string result = ApplicationExecute(req);
                    LogEntry* entry = new LogEntry(deadline, reqKey, myHash, hash, req->key(), result, req->proxyid());
                    fastReplyQu_[(roundRobin++) % replyShard].enqueue(entry);
                    uint32_t logId = maxSyncedLogId_ + 1;
                    syncedEntries_.assign(logId, entry);
                    syncedReq2LogId_.assign(reqKey, logId);
                    syncedLogIdByKey_[req->key()] = logId;
                    maxSyncedLogId_++;
                }
                else {
                    if (unsyncedLogIdByKey_[req->key()] > 1) {
                        // There are previous (non-commutative) requests appended
                        LogEntry* prev = unsyncedEntries_.get(unsyncedLogIdByKey_[req->key()]);
                        hash.XOR(prev->hash);
                    }
                    LogEntry* entry = new LogEntry(deadline, reqKey, myHash, hash, req->key(), "", req->proxyid());
                    fastReplyQu_[(roundRobin++) % replyShard].enqueue(entry);
                    uint32_t logId = maxUnSyncedLogId_ + 1;
                    unsyncedEntries_.assign(logId, entry);
                    unsyncedReq2LogId_.assign(reqKey, logId);
                    unsyncedLogIdByKey_[req->key()] = logId;
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
        // LogEntry* entry = NULL;
        // Reply reply;
        // reply.set_view(viewNum_);
        // reply.set_replytype(FAST_REPLY);
        // reply.set_replicaid(replicaId_);
        // std::map<uint64_t, struct sockaddr_in> addrMap;
        // bool amLeader = AmLeader();
        // while (status_ == NORMAL) {
        //     if (fastReplyQu_[id].try_dequeue(entry)) {
        //         reply.set_clientid((entry->reqKey) >> 32);
        //         reply.set_reqid((uint32_t)(entry->reqKey));
        //         if (amLeader) {
        //             reply.set_hash(entry->hash.hash, SHA_DIGEST_LENGTH);
        //         }
        //         else {
        //             // TODO:consider both synced and unsynced entries
        //             uint32_t logid = unsyncedReq2LogId_.get(entry->reqKey);
        //             if (syncedLogIdByKey_[entry->opKey] > 1) {
        //                 uint32_t syncPoint = syncedLogIdByKey_[entry->opKey];
        //                 LogEntry* syncedEntry = syncedEntries_.get(syncPoint);
        //                 if (syncedEntry->deadline < entry->deadline || (syncedEntry->deadline == entry->deadline && syncedEntry->reqKey < entry->reqKey)) {
        //                     uint32_t unsyncedPoint = minUnSyncedLogId_;
        //                     LogEntry* unsyncedEntry = NULL;
        //                     while (unsyncedPoint <= logid) {
        //                         unsyncedEntry = unsyncedEntries_.get(unsyncedPoint);
        //                         if (unsyncedEntry->opKey != entry->opKey) {
        //                             unsyncedPoint++;
        //                         }
        //                         if (unsyncedEntry->deadline < syncedEntry->deadline || (unsyncedEntry->deadline == syncedEntry->deadline && unsyncedEntry->reqKey <= syncedEntry->reqKey)) {
        //                             unsyncedPoint++;
        //                         }
        //                         else {
        //                             break;
        //                         }
        //                     }
        //                     // !!! Commutativity
        //                     // TODO: think whether unsyncedEntry = NULL is possible
        //                     assert(unsyncedPoint <= logid);
        //                     SHA_HASH hash = entry->hash;
        //                     hash.XOR(unsyncedEntry->hash);
        //                     hash.XOR(unsyncedEntry->myhash);
        //                     reply.set_hash(hash.hash, SHA_DIGEST_LENGTH);

        //                 }
        //                 // else (unlikely): syncedEntry has surpassed unsyncedEntry, so we do not need
        //                 // to send fast-reply for this entry, because (1) a slow-reply must have been sent (2) or this entry does not exist on the leader

        //             }
        //         }

        //         reply.set_result(entry->result);
        //         size_t msgLen = reply.ByteSizeLong();
        //         if (addrMap.find(entry->proxyId) == addrMap.end()) {
        //             struct sockaddr_in addr;
        //             bzero(&addr, sizeof(addr));
        //             addr.sin_family = AF_INET;
        //             addr.sin_port = htons((uint32_t)(entry->proxyId));
        //             addr.sin_addr.s_addr = proxyIPs_[(entry->proxyId) >> 32];
        //             addrMap[entry->proxyId] = addr;
        //         }
        //         if (reply.SerializeToArray(buffer, msgLen)) {
        //             // sendto(fd, buffer, msgLen, 0, (struct sockaddr*)&(addrMap[entry->proxyId]), sizeof(sockaddr_in));
        //         }
        //     }
        // }
        workerCounter_.fetch_sub(1);
    }

    void Replica::SlowReplyTd(int id, int cvId) {
        workerCounter_.fetch_add(1);
        // std::string key = "SlowReplyTd-" + std::to_string(id);
        // int fd = socketFds_[key];
        // char* buffer = buffers_[key];
        // LogEntry* entry = NULL;
        // Reply reply;
        // reply.set_view(viewNum_);
        // reply.set_replytype(SLOW_REPLY);
        // reply.set_replicaid(replicaId_);
        // reply.set_hash("");
        // reply.set_result("");
        // std::map<uint64_t, struct sockaddr_in> addrMap;
        // while (status_ == NORMAL) {
        //     if (slowReplyQus_[id].try_dequeue(entry)) {
        //         reply.set_clientid((entry->reqKey) >> 32);
        //         reply.set_reqid((uint32_t)(entry->reqKey));
        //         size_t msgLen = reply.ByteSizeLong();
        //         if (addrMap.find(entry->proxyId) == addrMap.end()) {
        //             struct sockaddr_in addr;
        //             bzero(&addr, sizeof(addr));
        //             addr.sin_family = AF_INET;
        //             addr.sin_port = htons((uint32_t)(entry->proxyId));
        //             addr.sin_addr.s_addr = proxyIPs_[(entry->proxyId) >> 32];
        //             addrMap[entry->proxyId] = addr;
        //         }
        //         // To optimize: SLOW_REPLY => COMMIT_REPLY
        //         if (reply.SerializeToArray(buffer, msgLen)) {
        //             sendto(fd, buffer, msgLen, 0, (struct sockaddr*)&(addrMap[entry->proxyId]), sizeof(sockaddr_in));
        //         }
        //     }
        // }

        workerCounter_.fetch_sub(1);
    }

    void Replica::IndexSendTd(int id) {
        workerCounter_.fetch_add(1);
        while (status_ == NORMAL) {

        }
        workerCounter_.fetch_sub(1);
    }

    void Replica::IndexRecvTd() {}

    void Replica::MissedIndexAckTd() {}

    void Replica::MissedReqAckTd() {}

    void Replica::ReceiveIndexSyncMessage(char* msgBuffer, int msgLen) {

    }

    void Replica::ReceiveAskMissedReq(char* msgBuffer, int msgLen) {

    }

    void Replica::ReceiveAskMissedIdx(char* msgBuffer, int msgLen) {

    }

    void Replica::ReceiverOtherMessage(char* msgBuffer, int msgLen, Address* sender, UDPSocketEndpoint* receiverEP) {

    }


    // void Replica::FollowerIndexSyncReceive(int id, int fd) {
    //     struct sockaddr_in addr;
    //     socklen_t addrlen;
    //     char* buffer = requestBuffers_[id];
    //     int sz = recvfrom(fd, buffer, BUFFER_SIZE, 0, (struct sockaddr*)&addr, &addrlen);
    //     IndexSync idxSyncMsg;
    //     MissedReq missedReqMsg;
    //     AskReq askReqMsg;
    //     AskIndex askIdxMsg;
    //     std::map<uint32_t, IndexSync> pendingIndexSync;
    //     if (sz > 0) {
    //         // recognize msg type based on sender port
    //         int port = ntohs(addr.sin_port);
    //         if (port == senderPorts_[INDEX_SYNC]) {
    //             if (idxSyncMsg.ParseFromArray(buffer, sz)) {
    //                 if (!CheckViewAndCV()) {
    //                     return;
    //                 }
    //                 if (maxSyncedLogId_ + 1 < idxSyncMsg.logidbegin()) {
    //                     pendingIndexSync[idxSyncMsg.logidbegin()] = idxSyncMsg;
    //                     // We are missing some idxSyncMsgs
    //                     askIdxMsg.set_logidbegin(maxSyncedLogId_ + 1);
    //                     askIdxMsg.set_logidend(idxSyncMsg.logidbegin() - 1);
    //                     addr.sin_port = htons(senderPorts_[ASK_INDEX]);
    //                     sendto(senderFds_[ASK_INDEX], senderBuffers_[ASK_INDEX], askIdxMsg.ByteSizeLong(), 0, (struct sockaddr*)&addr, addrlen);
    //                 }
    //                 else if (maxSyncedLogId_ < idxSyncMsg.logidend()) {
    //                     // This idxSyncMsg is useful
    //                     ProcessIndexSync(idxSyncMsg);
    //                 }
    //                 // Process pendingIndexSync, if any
    //                 while (!pendingIndexSync.empty()) {
    //                     if (ProcessIndexSync(pendingIndexSync.begin()->second)) {
    //                         pendingIndexSync.erase(pendingIndexSync.begin());
    //                     }
    //                     else {
    //                         break;
    //                     }
    //                 }
    //             }
    //         }
    //         // TODO: Maybe delegate to Master Thread
    //         else if (port == senderPorts_[MISSED_REQ]) {

    //         }

    //     }
    // }

    // bool Replica::ProcessIndexSync(const IndexSync& idxSyncMsg) {
    //     if (idxSyncMsg.logidend() <= maxSyncedLogId_) {
    //         // This idxSyncMsg is useless 
    //         return true;
    //     }
    //     if (idxSyncMsg.logidbegin() > maxSyncedLogId_ + 1) {
    //         return false;
    //     }
    //     for (uint32_t logid = maxSyncedLogId_ + 1; logid <= idxSyncMsg.logidend(); logid++) {
    //         uint32_t offset = logid - idxSyncMsg.logidbegin();
    //         uint64_t reqKey = idxSyncMsg.reqkeys(offset);
    //         uint64_t deadline = idxSyncMsg.deadlines(offset);
    //         Request* req = unsyncedRequestMap_.get(reqKey);
    //         if (req) {
    //             // Find the req locally
    //             SHA_HASH myHash = CalculateHash(deadline, reqKey);
    //             SHA_HASH hash = myHash;
    //             assert(syncedEntries_.get(syncedLogIdByKey_[req->key()]) != NULL);
    //             const SHA_HASH& prev = syncedEntries_.get(syncedLogIdByKey_[req->key()])->hash;
    //             hash.XOR(prev);
    //             LogEntry* entry = new LogEntry(deadline, reqKey, myHash, hash, req->key(), "", req->proxyid());
    //             syncedRequestMap_.assign(reqKey, req);
    //             syncedReq2LogId_.assign(reqKey, logid);
    //             syncedEntries_.assign(logid, entry);
    //             syncedLogIdByKey_[req->key()] = logid;
    //             maxSyncedLogId_++;
    //         }
    //         // TO Continue

    //     }
    //     return true;
    // }

    // void Replica::Master() {
    //     uint64_t nowTime = GetMicrosecondTimestamp();
    //     // I have not heard from the leader for a long time, start a view change
    //     bool leaderMayDie = ((!AmLeader()) && nowTime > lastHeartBeatTime_ + replicaConfig_["heartbeat-threshold-ms"].as<uint64_t>());
    //     if (leaderMayDie || status_ == VIEWCHANGE) {
    //         status_ = VIEWCHANGE;
    //         while (workerCounter_ > 0) {
    //             // Wait until all workers have exit
    //             usleep(10);
    //         }
    //         // Increment viewNum
    //         viewNum_++;
    //         // Stop masterTimer
    //         ev_timer_stop(evLoops_["master"], evTimers_["master"]);

    //         // Start ViewChange
    //         StartViewChange();
    //     }
    // }
    // void Replica::MasterReceive(int fd) {
    //     struct sockaddr_in addr;
    //     socklen_t addrlen;
    //     char* buffer = buffers_["master"];
    //     int sz = recvfrom(fd, buffer, BUFFER_SIZE, 0, (struct sockaddr*)&addr, &addrlen);
    //     if (sz > 0) {
    //         // Parse and process message
    //         Request* request = new Request();
    //         if (request->ParseFromArray(buffer, sz)) {
    //             requestMap_.assign(request->sendtime(), request);
    //         }
    //     }
    // }
    void Replica::StartViewChange() {

    }

    bool Replica::CheckViewAndCV() {
        return true;
    }
    std::string Replica::ApplicationExecute(Request* req) {
        return "";
    }
    bool Replica::AmLeader() {
        return (viewNum_ % replicaNum_ == replicaId_);
    }
}

