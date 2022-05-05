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
        status_ = NORMAL;
        LOG(INFO) << "viewNum_=" << viewNum_
            << "\treplicaId=" << replicaId_
            << "\treplicaNum=" << replicaNum_;

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
            std::thread* td = new std::thread(&Replica::ReplyTd, this, i);
            std::string key("ReplyTd-" + std::to_string(i));
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
        while (status_ == NORMAL) {
            if (processQu_.try_dequeue(reqKey)) {
                Request* req = requestMap_.get(reqKey);
                if (req != NULL) {
                    uint64_t deadline = req->sendtime() + req->bound();
                    std::pair<uint64_t, uint64_t>myEntry(deadline, reqKey);
                    if (myEntry > lastReleasedEntryByKeys_[req->key()]) {
                        earlyBuffer_.insert(myEntry);
                    }
                    else {
                        // req cannot enter early buffer
                        if (AmLeader()) {
                            // leader modifies its deadline
                            // followers donot care about it (i.e. leave it in late buffer)

                        }
                    }

                }
                else {
                    LOG(WARNING) << "Abnormal behavior: req is NULL";
                }
            }

            // Polling early-buffer
        }
        workerCounter_.fetch_sub(1);
    }
    void Replica::ReplyTd(int id) {
        workerCounter_.fetch_add(1);
        int fd = socketFds_["ReplyTd-" + std::to_string(id)];
        while (status_ == NORMAL) {

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
                uint64_t reqKey = request->clientid();
                reqKey = ((reqKey << 32u) | (request->reqid()));
                requestMap_.assign(reqKey, request);
                processQu_.enqueue(reqKey);
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
    bool Replica::AmLeader() {
        return (viewNum_ % replicaNum_ == replicaId_);
    }
}

