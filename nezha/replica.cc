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

        CreateContext();
        // Launch Threads (based on Config)
        LaunchThreads();

        // Register Master Timer 
        ev_init(&masterTimer_, [](struct ev_loop* loop, struct ev_timer* w, int revents) {
            static_cast<Replica*>(w->data)->Master();
            });

        ev_timer_again(masterLoop_, &masterTimer_);

        // Register Master IO
        ev_io_init(&masterIO_, [](struct ev_loop* loop, struct ev_io* w, int revents) {
            static_cast<Replica*>(w->data)->MasterReceive();
            }, masterSocketFd_, EV_READ);
        ev_io_start(masterLoop_, &masterIO_);

        ev_run(masterLoop_, 0);
    }

    Replica::~Replica()
    {

        for (auto& kv : threadPool_) {
            delete kv.second;
            LOG(INFO) << "Deleted\t" << kv.first;
        }
    }
    void Replica::CreateContext() {
        masterLoop_ = ev_loop_new();
        masterTimer_.repeat = replicaConfig_["main-loop-period-ms"].get<uint32_t>() * 1e-3;
        masterTimer_.data = (void*)this;
        masterIO_.data = (void*)this;
        masterSocketFd_ = socket(PF_INET, SOCK_DGRAM, 0);
        if (masterSocketFd_ < 0) {
            LOG(ERROR) << "masterSocketFd_ fail";
        }
        struct sockaddr_in addr;
        bzero(&addr, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_port = htons(replicaConfig_["master-ports"][replicaId_].get<uint32_t>());
        addr.sin_addr.s_addr = inet_addr(replicaConfig_["replica-ips"][replicaId_].get<std::string>().c_str());
        // Bind socket to address
        if (bind(masterSocketFd_, (struct sockaddr*)&addr, sizeof(addr)) != 0) {
            LOG(ERROR) << "bind error";
        }

        for (int i = 0; i < replicaConfig_["receiver-shards"]; i++) {
            std::string key("receiverTd-" + std::to_string(i));
            evLoops_[key] = ev_loop_new();
        }
    }
    void Replica::LaunchThreads() {
        // RequestReceive
        for (int i = 0; i < replicaConfig_["receiver-shards"]; i++) {
            std::thread* td = new std::thread(&Replica::RequestReceive, this, i);
            std::string key("receiverTd-" + std::to_string(i));
            threadPool_[key] = td;
            LOG(INFO) << "Launched " << key << "\t" << td->native_handle();
        }
        // RequestProcess
        for (int i = 0; i < replicaConfig_["process-shards"]; i++) {
            std::thread* td = new std::thread(&Replica::RequestProcess, this, i);
            std::string key("processTd-" + std::to_string(i));
            threadPool_[key] = td;
            LOG(INFO) << "Launched " << key << "\t" << td->native_handle();
        }
        // RequestReply
        for (int i = 0; i < replicaConfig_["reply-shards"]; i++) {
            std::thread* td = new std::thread(&Replica::RequestReply, this, i);
            std::string key("replyTd-" + std::to_string(i));
            threadPool_[key] = td;
            LOG(INFO) << "Launched " << key << "\t" << td->native_handle();
        }

    }
    void Replica::RequestReceive(int id) {
        workerCounter_.fetch_add(1);
        struct sockaddr_in addr;
        socklen_t addrlen;
        workerCounter_.fetch_sub(1);
    }
    void Replica::RequestProcess(int id) {
        workerCounter_.fetch_add(1);

        workerCounter_.fetch_sub(1);
    }
    void Replica::RequestReply(int id) {
        workerCounter_.fetch_add(1);

        workerCounter_.fetch_sub(1);
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
            ev_timer_stop(masterLoop_, &masterTimer_);

            // Start ViewChange
            StartViewChange();
        }
    }
    void Replica::MasterReceive() {
        struct sockaddr_in addr;
        socklen_t addrlen;
        int sz = recvfrom(masterSocketFd_, masterBuffer, BUFFER_SIZE, 0, (struct sockaddr*)&addr, &addrlen);
        if (sz > 0) {
            // Parse and process message
            Request* request = new Request();
            if (request->ParseFromArray(masterBuffer, sz)) {
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

