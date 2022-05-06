#include "nezha/replica.h"

namespace nezha {
    Replica::Replica(const std::string& configFile)
    {
        // Load Config
        std::ifstream configIfs(configFile);
        if (!configIfs.is_open()) {
            LOG(ERROR) << "replica config open fail " << configFile;
        }
        configIfs >> replicaConfig_;
        configIfs.close();

        viewNum_ = 0;
        replicaId_ = replicaConfig_["replica-id"];
        replicaNum_ = replicaConfig_["replica-ips"].size();
        uint32_t keyNum = replicaConfig_["key-num"].get<uint32_t>();
        lastReleasedEntryByKeys_.resize(keyNum, std::pair<uint64_t, uint64_t>(0, 0));
        // Since ConcurrentMap reseres 0 and 1, we can only use log-id from 2
        maxSyncedLogId_ = 1;
        minUnSyncedLogId_ = 1;
        maxUnSyncedLogId_ = 1;
        syncedLogIdByKey_.resize(keyNum, 1);
        unsyncedLogIdByKey_.resize(keyNum, 1);

        status_ = NORMAL;
        LOG(INFO) << "viewNum_=" << viewNum_
            << "\treplicaId=" << replicaId_
            << "\treplicaNum=" << replicaNum_
            << "\tkeyNum=" << keyNum;

        CreateMasterContext();
        CreateReceiverContext();
        // Launch Threads (based on Config)
        LaunchThreads();
        ev_run(evLoops_["master"], 0);
    }

    Replica::~Replica()
    {
        for (auto& kv : threadPool_) {
            delete kv.second;
            LOG(INFO) << "Deleted\t" << kv.first;
        }
    }
    void Replica::CreateMasterContext() {
        char* buffer = new char[BUFFER_SIZE];
        struct ev_loop* evLoop = ev_default_loop();
        struct ev_timer* evTimer = new ev_timer();
        evTimer->repeat = replicaConfig_["main-loop-period-ms"].get<uint32_t>() * 1e-3;
        evTimer->data = (void*)this;
        struct ev_io* evIO = new ev_io();
        evIO->data = (void*)this;
        int fd = socket(PF_INET, SOCK_DGRAM, 0);
        if (fd < 0) {
            LOG(ERROR) << "masterSocketFd_ fail";
        }
        struct sockaddr_in addr;
        bzero(&addr, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_port = htons(replicaConfig_["master-ports"][replicaId_].get<uint32_t>());
        addr.sin_addr.s_addr = inet_addr(replicaConfig_["replica-ips"][replicaId_].get<std::string>().c_str());
        // Bind socket to address
        if (bind(fd, (struct sockaddr*)&addr, sizeof(addr)) != 0) {
            LOG(ERROR) << "bind error";
        }
        // Register Master Timer 
        ev_init(evTimer, [](struct ev_loop* loop, struct ev_timer* w, int revents) {
            static_cast<Replica*>(w->data)->Master();
            });

        ev_timer_again(evLoop, evTimer);

        // Register Master IO
        ev_io_init(evIO, [](struct ev_loop* loop, struct ev_io* w, int revents) {
            static_cast<Replica*>(w->data)->MasterReceive(w->fd);
            }, fd, EV_READ);
        ev_io_start(evLoop, evIO);

        std::string key("master");
        evLoops_[key] = evLoop;
        evIOs_[key] = evIO;
        evTimers_[key] = evTimer;
        socketFds_[key] = fd;
        receiverBuffers_[key] = buffer;
    }

    void Replica::CreateReceiverContext() {
        for (int i = 0; i < replicaConfig_["receiver-shards"]; i++) {
            char* buffer = new char[BUFFER_SIZE];
            struct ev_loop* evLoop = ev_loop_new();
            struct ev_io* evIO = new ev_io();
            int fd = socket(PF_INET, SOCK_DGRAM, 0);
            if (fd < 0) {
                LOG(ERROR) << "Receiver Fd fail " << i;
            }
            struct sockaddr_in addr;
            bzero(&addr, sizeof(addr));
            addr.sin_family = AF_INET;
            addr.sin_port = htons(replicaConfig_["receiver-ports"][replicaId_].get<uint32_t>() + i);
            addr.sin_addr.s_addr = inet_addr(replicaConfig_["replica-ips"][replicaId_].get<std::string>().c_str());
            // Bind socket to address
            if (bind(fd, (struct sockaddr*)&addr, sizeof(addr)) != 0) {
                LOG(ERROR) << "bind error";
            }
            ev_io_init(evIO, [](struct ev_loop* loop, struct ev_io* w, int revents) {
                int* idPtr = (int*)(w->data);
                static_cast<Replica*>(w->data)->RequestReceive(*idPtr, w->fd);
                }, fd, EV_READ);
            ev_io_start(evLoop, evIO);

            std::string key("ReceiveTd-" + std::to_string(i));
            evLoops_[key] = evLoop;
            evIOs_[key] = evIO;
            socketFds_[key] = fd;
            receiverBuffers_[key] = buffer;
            requestBuffers_.push_back(buffer);
        }
    }
    void Replica::LaunchThreads() {
        // RequestReceive
        for (int i = 0; i < replicaConfig_["receiver-shards"]; i++) {
            std::thread* td = new std::thread(&Replica::ReceiveTd, this, i);
            std::string key("ReceiveTd-" + std::to_string(i));
            threadPool_[key] = td;
            LOG(INFO) << "Launched " << key << "\t" << td->native_handle();
        }
        // RequestProcess
        for (int i = 0; i < replicaConfig_["process-shards"]; i++) {
            std::thread* td = new std::thread(&Replica::ProcessTd, this, i);
            std::string key("ProcessTd-" + std::to_string(i));
            threadPool_[key] = td;
            LOG(INFO) << "Launched " << key << "\t" << td->native_handle();
        }
        // RequestReply
        for (int i = 0; i < replicaConfig_["reply-shards"]; i++) {
            std::thread* td = new std::thread(&Replica::FastReplyTd, this, i);
            std::string key("FastReplyTd-" + std::to_string(i));
            threadPool_[key] = td;
            LOG(INFO) << "Launched " << key << "\t" << td->native_handle();
        }

    }

    void Replica::ReceiveTd(int id) {
        workerCounter_.fetch_add(1);
        std::string key("ReceiveTd-" + std::to_string(id));
        ev_run(evLoops_[key], 0);
        workerCounter_.fetch_sub(1);
    }
    void Replica::ProcessTd(int id) {
        workerCounter_.fetch_add(1);
        uint64_t reqKey = 0;
        Request* req = NULL;
        uint32_t roundRobin = 0;
        uint32_t replyShard = replicaConfig_['reply-shards'].get<uint32_t>();
        bool amLeader = AmLeader();
        while (status_ == NORMAL) {
            if (processQu_.try_dequeue(req)) {
                reqKey = req->clientid();
                reqKey = ((reqKey << 32u) | (req->reqid()));
                if (amLeader) {
                    uint32_t duplicateLogIdx = syncedReq2LogId_.get(reqKey);
                    if (duplicateLogIdx == 0) {
                        // not duplicate
                        uint64_t deadline = req->sendtime() + req->bound();
                        std::pair<uint64_t, uint64_t>myEntry(deadline, reqKey);
                        syncedRequestMap_.assign(reqKey, req);
                        if (myEntry > lastReleasedEntryByKeys_[req->key()]) {
                            earlyBuffer_[myEntry] = req;
                        }
                        else {
                            // req cannot enter early buffer
                            // leader modifies its deadline
                            uint64_t newDeadline = lastReleasedEntryByKeys_[req->key()].first + 1;
                            std::pair<uint64_t, uint64_t>myEntry(newDeadline, reqKey);
                            earlyBuffer_[myEntry] = req;
                        }
                    }
                    else {
                        // at-most-once: duplicate requests are not executed twice
                        // we simply send the previous reply messages
                        LogEntry* entry = syncedEntries_.get(duplicateLogIdx);
                        fastReplyQus_[(roundRobin++) % replyShard].enqueue(entry);
                        // free this req
                        delete req;
                    }
                }
                else {
                    uint32_t duplicateLogIdx = unsyncedReq2LogId_.get(reqKey);
                    if (duplicateLogIdx == 0) {
                        uint64_t deadline = req->sendtime() + req->bound();
                        std::pair<uint64_t, uint64_t>myEntry(deadline, reqKey);
                        unsyncedRequestMap_.assign(reqKey, req);
                        if (myEntry > lastReleasedEntryByKeys_[req->key()]) {
                            earlyBuffer_[myEntry] = req;
                        }
                        //  Followers donot care about it (i.e. leave it in late buffer)
                    }
                    else {
                        LogEntry* entry = unsyncedEntries_.get(duplicateLogIdx);
                        fastReplyQus_[(roundRobin++) % replyShard].enqueue(entry);
                        delete req;
                    }

                }

            }

            // Polling early-buffer
            uint64_t nowTime = GetMicrosecondTimestamp();
            while ((!earlyBuffer_.empty()) && nowTime >= earlyBuffer_.begin()->first.first) {
                uint64_t deadline = earlyBuffer_.begin()->first.first;
                uint64_t reqKey = earlyBuffer_.begin()->first.second;
                Request* req = earlyBuffer_.begin()->second;
                SHA_HASH hash = CalculateHash(deadline, reqKey);
                if (amLeader) {
                    if (syncedLogIdByKey_[req->key()] > 1) {
                        // There are previous (non-commutative) requests appended
                        LogEntry* prev = syncedEntries_.get(syncedLogIdByKey_[req->key()]);
                        hash.XOR(prev->hash);
                    }
                    std::string result = ApplicationExecute(req);
                    LogEntry* entry = new LogEntry(deadline, reqKey, hash, result);
                    fastReplyQus_[(roundRobin++) % replyShard].enqueue(entry);
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
                    LogEntry* entry = new LogEntry(deadline, reqKey, hash, "");
                    fastReplyQus_[(roundRobin++) % replyShard].enqueue(entry);
                    uint32_t logId = maxUnSyncedLogId_ + 1;
                    unsyncedEntries_.assign(logId, entry);
                    unsyncedReq2LogId_.assign(reqKey, logId);
                    unsyncedLogIdByKey_[req->key()] = logId;
                    maxUnSyncedLogId_++;
                }
                earlyBuffer_.erase(earlyBuffer_.begin());
            }
        }
        workerCounter_.fetch_sub(1);
    }
    void Replica::FastReplyTd(int id) {
        workerCounter_.fetch_add(1);
        std::string key = "FastReplyTd-" + std::to_string(id);
        int fd = socketFds_[key];
        char* buffer = receiverBuffers_[key];
        LogEntry* entry = NULL;
        Reply reply;
        reply.set_view(viewNum_);
        reply.set_isfast(true);
        reply.set_replicaid(replicaId_);
        while (status_ == NORMAL) {
            if (fastReplyQus_[id].try_dequeue(entry)) {
                reply.set_clientid((entry->reqKey) >> 32);
                reply.set_reqid((uint32_t)(entry->reqKey));
                reply.set_hash(entry->hash.hash, SHA_DIGEST_LENGTH);
                reply.set_result(entry->result);
                size_t msgLen = reply.ByteSizeLong();
                if (reply.SerializeToArray(buffer, msgLen)) {
                    // sendto(fd, buffer, msgLen, 0, );
                }


            }
        }
        workerCounter_.fetch_sub(1);
    }
    void Replica::RequestReceive(int id, int fd) {
        struct sockaddr_in addr;
        socklen_t addrlen;
        char* buffer = requestBuffers_[id];
        int sz = recvfrom(fd, buffer, BUFFER_SIZE, 0, (struct sockaddr*)&addr, &addrlen);
        if (sz > 0) {
            // Parse and process message
            Request* request = new Request();
            if (request->ParseFromArray(buffer, sz)) {
                processQu_.enqueue(request);
            }
        }
    }

    void Replica::Master() {
        uint64_t nowTime = GetMicrosecondTimestamp();
        // I have not heard from the leader for a long time, start a view change
        bool leaderMayDie = ((!AmLeader()) && nowTime > lastHeartBeatTime_ + replicaConfig_["heartbeat-threshold-ms"].get<uint64_t>());
        if (leaderMayDie || status_ == VIEWCHANGE) {
            status_ = VIEWCHANGE;
            while (workerCounter_ > 0) {
                // Wait until all workers have exit
                usleep(10);
            }
            // Increment viewNum
            viewNum_++;
            // Stop masterTimer
            ev_timer_stop(evLoops_["master"], evTimers_["master"]);

            // Start ViewChange
            StartViewChange();
        }
    }
    void Replica::MasterReceive(int fd) {
        struct sockaddr_in addr;
        socklen_t addrlen;
        char* buffer = receiverBuffers_["master"];
        int sz = recvfrom(fd, buffer, BUFFER_SIZE, 0, (struct sockaddr*)&addr, &addrlen);
        if (sz > 0) {
            // Parse and process message
            Request* request = new Request();
            if (request->ParseFromArray(buffer, sz)) {
                requestMap_.assign(request->sendtime(), request);
            }
        }
    }
    void Replica::StartViewChange() {

    }

    std::string Replica::ApplicationExecute(Request* req) {
        return "";
    }
    bool Replica::AmLeader() {
        return (viewNum_ % replicaNum_ == replicaId_);
    }
}

