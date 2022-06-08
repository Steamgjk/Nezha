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
#include <fcntl.h>
#include <fstream>
#include <glog/logging.h>
#include <junction/ConcurrentMap_Leapfrog.h>
#include <yaml-cpp/yaml.h>
#include "nezha/nezha-proto.pb.h"
#include "lib/utils.h"
#include "lib/concurrentqueue.hpp"
#include "lib/udp_socket_endpoint.h"
#include "lib/zipfian.h"

namespace nezha {
    using namespace nezha::proto;
    template<typename T1> using ConcurrentQueue = moodycamel::ConcurrentQueue<T1>;
    template<typename T1, typename T2> using ConcurrentMap = junction::ConcurrentMap_Leapfrog<T1, T2>;
    struct LogInfo {
        uint32_t reqId;
        uint64_t sendTime;
        uint64_t commitTime;
        uint32_t commitType;
    };
    class Client
    {
    private:
        /* data */
        std::map<std::string, std::thread*>threadPool_;
        YAML::Node clientConfig_;
        UDPSocketEndpoint* requestEP_;
        struct MsgHandlerStruct* replyHandler_;
        struct TimerStruct* monitorTimer_;
        std::atomic<bool> running_;
        std::atomic<bool> suspending_;
        int clientId_;
        int poissonRate_;
        std::atomic<uint32_t>nextReqId_;
        std::atomic<uint32_t> committedReqId_;
        std::atomic<uint32_t> reclaimedReqId_;
        std::vector<uint32_t> poissonTrace_;
        ConcurrentQueue<Request*> retryQu_;
        std::vector<std::vector<Address*>> proxyAddrs_;
        std::vector<uint32_t> zipfianKeys_;
        ConcurrentMap<uint32_t, Request*> outstandingRequests_;
        ConcurrentMap<uint32_t, uint64_t> outstandingRequestSendTime_;
        ConcurrentQueue<LogInfo*> logQu_; // for log
        uint32_t retryTimeoutus_;

        void LaunchThreads();
        void ProcessReplyTd();
        void OpenLoopSubmissionTd();
        void CloseLoopSubmissionTd();
        void LogTd();
        void ReceiveReply(char* msgBuffer, int bufferLen, Address* sender);

    public:
        Client(const std::string& configFile = std::string("../configs/nezha-client-config.yaml"));
        void Run();
        ~Client();
    };




}