#include "nezha/client.h"


namespace nezha {
    Client::Client(const std::string& configFile)
    {
        clientConfig_ = YAML::LoadFile(configFile);
        clientId_ = clientConfig_["client-id"].as<int>();
        LOG(INFO) << "clientId=" << clientId_;
        std::string clientIP = clientConfig_["client-ip"].as<std::string>();
        int requestPort = clientConfig_["request-port"].as<int>();
        requestEP_ = new UDPSocketEndpoint(clientIP, requestPort, true);
        replyHandler_ = new MsgHandlerStruct([](char* msgBuffer, int bufferLen, Address* sender, void* ctx, UDPSocketEndpoint* receiverEP) {
            ((Client*)ctx)->ReceiveReply(msgBuffer, bufferLen, sender);
            }, this);
        monitorTimer_ = new TimerStruct([](void* ctx, UDPSocketEndpoint* receiverEP) {
            if (((Client*)ctx)->running_ == false) {
                receiverEP->LoopBreak();
            }
            }, this, 10);

        proxyAddrs_.resize(clientConfig_["proxy-ips"].as<int>());
        for (uint32_t i = 0; i < proxyAddrs_.size(); i++) {
            proxyAddrs_[i].resize(clientConfig_["proxy-shards"].as<int>());
            for (uint32_t j = 0; j < proxyAddrs_[i].size(); j++) {
                proxyAddrs_[i][j] = new Address(clientConfig_["proxy-ips"][i].as<std::string>(), clientConfig_["request-port-base"].as<int>() + j);
            }
        }

        if (clientConfig_["is-openloop"].as<bool>()) {
            poissonRate_ = clientConfig_["poisson-rate"].as<int>();
            LOG(INFO) << "OpenLoop Client rate=" << poissonRate_;
            poissonTrace_.resize(1000, 0);
            std::default_random_engine generator(clientId_); // clientId as the seed
            std::poisson_distribution<int> distribution(poissonRate_);
            for (int i = 0; i < 1000; i++) {
                int reqNum = distribution(generator);
                if (reqNum < 0) {
                    poissonTrace_[i] = 0;
                }
                else {
                    poissonTrace_[i] = reqNum;
                }
            }

        }
        int keyNum = clientConfig_["key-num"].as<int>();
        float skewFactor = clientConfig_["skew-factor"].as<float>();
        LOG(INFO) << "keyNum=" << keyNum << "\tskewFactor=" << skewFactor;
        zipfianKeys_.resize(10000, 0);
        retryTimeoutus_ = clientConfig_["request-retry-time-us"].as<uint32_t>();
        if (keyNum > 1) {
            std::default_random_engine generator(clientId_); // clientId as the seed
            zipfian_int_distribution<uint32_t> zipfianDistribution(0, keyNum - 1, skewFactor);
            for (uint32_t i = 0; i < zipfianKeys_.size(); i++) {
                zipfianKeys_[i] = zipfianDistribution(generator);
            }
        }
        committedReqId_ = 0;
        reclaimedReqId_ = 0;
        nextReqId_ = 1;

    }

    void Client::Run() {
        running_ = true;
        suspending_ = false;
        LaunchThreads();
    }

    void Client::LaunchThreads() {
        threadPool_["ProcessReplyTd"] = new std::thread(&Client::ProcessReplyTd, this);
        if (clientConfig_["is-openloop"].as<bool>()) {
            LOG(INFO) << "OpenLoop Client";
            threadPool_["OpenLoopSubmissionTd"] = new std::thread(&Client::OpenLoopSubmissionTd, this);
        }
        else {
            LOG(INFO) << "ClosedLoop Client";
            threadPool_["CloseLoopSubmissionTd"] = new std::thread(&Client::CloseLoopSubmissionTd, this);
        }
    }

    void Client::ProcessReplyTd() {
        requestEP_->RegisterMsgHandler(replyHandler_);
        requestEP_->RegisterTimer(monitorTimer_);
        requestEP_->LoopRun();
    }

    void Client::ReceiveReply(char* msgBuffer, int bufferLen, Address* sender) {
        if (bufferLen < 0) {
            return;
        }
        Reply reply;
        if (reply.ParseFromArray(msgBuffer, bufferLen)) {
            // LOG(INFO) << "reply:" << reply.DebugString();
            Request* request = outstandingRequests_.get(reply.reqid());
            if (request) {
                LogInfo* log = new LogInfo();
                *log = { reply.reqid(),
                        outstandingRequestSendTime_.get(reply.reqid()),
                        GetMicrosecondTimestamp(),
                        reply.replytype() };
                logQu_.enqueue(log);
            }

        }
    }

    void Client::OpenLoopSubmissionTd() {
        int roundRobinIdx = 0;
        uint64_t startTime = GetMicrosecondTimestamp();
        uint64_t endTime = startTime + clientConfig_["duration-sec"].as<uint64_t>() * 1000000;
        while (running_) {
            if (suspending_) {
                // do something
                continue;
            }

            // Poisson rate is 10ms as one unit
            for (uint32_t i = 0; i < clientConfig_["duration-sec"].as<uint32_t>() * 100; i++) {
                uint32_t reqNum = poissonTrace_[i % poissonTrace_.size()];
                if (reqNum <= 0) {
                    usleep(10000);
                    continue;
                }
                uint32_t intval = 10000 / reqNum;
                uint64_t startTime = GetMicrosecondTimestamp();
                for (uint32_t j = 0; j < reqNum; j++) {
                    while (GetMicrosecondTimestamp() < startTime + j * intval) {}
                    // Send the request 
                    uint32_t mapIdx = roundRobinIdx % (proxyAddrs_.size() * proxyAddrs_[0].size());
                    Request* request = NULL;
                    if (retryQu_.try_dequeue(request)) {
                        // retry this request
                        std::string requestStr = request->SerializeAsString();
                        Address* roundRobinAddr = proxyAddrs_[mapIdx % proxyAddrs_.size()][mapIdx / proxyAddrs_.size()];
                        requestEP_->SendMsgTo(*roundRobinAddr, requestStr);
                        outstandingRequestSendTime_.assign(request->reqid(), GetMicrosecondTimestamp());
                        roundRobinIdx++;
                    }
                    else {
                        // submit new requests
                        request = new Request();
                        request->set_clientid(clientId_);
                        request->set_reqid(nextReqId_);
                        request->set_command("");
                        request->set_key(zipfianKeys_[nextReqId_ % zipfianKeys_.size()]);
                        std::string requestStr = request->SerializeAsString();
                        Address* roundRobinAddr = proxyAddrs_[mapIdx % proxyAddrs_.size()][mapIdx / proxyAddrs_.size()];
                        requestEP_->SendMsgTo(*roundRobinAddr, requestStr);
                        outstandingRequests_.assign(request->reqid(), request);
                        outstandingRequestSendTime_.assign(request->reqid(), GetMicrosecondTimestamp());
                        nextReqId_++;
                        roundRobinIdx++;

                    }

                }
                if (GetMicrosecondTimestamp() >= endTime) {
                    // Client has executed long enough, should terminate
                    return;
                }
            }
        }

    }

    void Client::CloseLoopSubmissionTd() {

        int roundRobinIdx = 0;
        uint64_t startTime = GetMicrosecondTimestamp();
        uint64_t endTime = startTime + clientConfig_["duration-sec"].as<uint64_t>() * 1000000;
        while (running_) {
            if (suspending_) {
                // do something
                continue;
            }
            if (GetMicrosecondTimestamp() >= endTime) {
                // Client has executed long enough, should terminate
                return;
            }
            Request* request = NULL;
            uint32_t mapIdx = roundRobinIdx % (proxyAddrs_.size() * proxyAddrs_[0].size());
            if (nextReqId_ == committedReqId_ + 1) {
                // submit new request
                request = new Request();
                request->set_clientid(clientId_);
                request->set_reqid(nextReqId_);
                request->set_command("");
                request->set_key(zipfianKeys_[nextReqId_ % zipfianKeys_.size()]);
                std::string requestStr = request->SerializeAsString();
                Address* roundRobinAddr = proxyAddrs_[mapIdx % proxyAddrs_.size()][mapIdx / proxyAddrs_.size()];
                requestEP_->SendMsgTo(*roundRobinAddr, requestStr);
                outstandingRequests_.assign(request->reqid(), request);
                outstandingRequestSendTime_.assign(request->reqid(), GetMicrosecondTimestamp());
                nextReqId_++;
                roundRobinIdx++;
            }
            else {
                if (retryQu_.try_dequeue(request)) {
                    // have some requests to retry
                    std::string requestStr = request->SerializeAsString();
                    Address* roundRobinAddr = proxyAddrs_[mapIdx % proxyAddrs_.size()][mapIdx / proxyAddrs_.size()];
                    requestEP_->SendMsgTo(*roundRobinAddr, requestStr);
                    outstandingRequestSendTime_.assign(request->reqid(), GetMicrosecondTimestamp());
                    nextReqId_++;
                    roundRobinIdx++;
                }
            }
        }

    }

    void Client::LogTd() {
        LogInfo* log = NULL;
        while (running_) {
            if (logQu_.try_dequeue(log)) {
                // erase the footprint of commited requests
                outstandingRequestSendTime_.erase(log->reqId);
                while (committedReqId_ + 1 < log->reqId) {
                    if (outstandingRequestSendTime_.get(committedReqId_ + 1) == 0) {
                        // this reqId has also been committed (i.e. cannot find its footprint)
                        // advance committedReqId;
                        committedReqId_++;
                    }
                    else {
                        break;
                    }
                }

                delete log;
            }

            for (uint32_t reqId = committedReqId_ + 1; reqId < nextReqId_; reqId++)
            {
                uint64_t sendTime = outstandingRequestSendTime_.get(reqId);
                if (sendTime > 0) {
                    // Find it
                    if (GetMicrosecondTimestamp() - sendTime > retryTimeoutus_) {
                        // timeout, should retry
                        Request* request = outstandingRequests_.get(reqId);
                        outstandingRequestSendTime_.erase(reqId);
                        retryQu_.enqueue(request);
                    }
                }
            }
            while (reclaimedReqId_ + 1000 < committedReqId_) {
                // do not reclaim request too aggressive
                // If we reclaim too aggressive, there can be some edge case of dangling request pointer
                Request* request = outstandingRequests_.get(reclaimedReqId_);
                if (request) {
                    outstandingRequests_.erase(request->reqid());
                    delete request;
                }
                reclaimedReqId_++;

            }
        }
    }

    Client::~Client()
    {
        running_ = false;
        for (auto& kv : threadPool_) {
            kv.second->join();
            delete kv.second;
        }
        while (reclaimedReqId_ <= nextReqId_) {
            Request* request = outstandingRequests_.get(reclaimedReqId_);
            if (request) {
                outstandingRequests_.erase(request->reqid());
                delete request;
            }
        }
    }
}