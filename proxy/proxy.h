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
#include <concurrentqueue.h>
#include "proto/nezha-proto.pb.h"
#include "lib/utils.h"
#include "lib/address.h"

namespace nezha {
    using namespace nezha::proto;
    template<typename T1> using ConcurrentQueue = moodycamel::ConcurrentQueue<T1>;
    template<typename T1, typename T2> using ConcurrentMap = junction::ConcurrentMap_Leapfrog<T1, T2>;
    class Proxy
    {
    private:
        std::map<std::string, std::thread*>threadPool_;
        YAML::Node proxyConfig_;
        void PrintConfig();
        void LaunchThreads();
        void CheckQuorum(const int id = -1);
        void ForwardRequests(const int id = -1);
        void CreateContext();
        void CalcLatencyBound();
        Reply* QuorumReady(std::map<uint32_t, Reply>& quorum);
        int CreateSocketFd(const std::string& sip = std::string(""), const int sport = -1);
        std::atomic<bool> stopForwarding_;
        std::atomic<bool> running_;
        std::vector<int> replyFds_;
        std::vector<int> forwardFds_;
        std::vector<int> requestReceiveFds_;
        std::vector<uint64_t> proxyIds_;
        std::atomic<uint32_t> latencyBound_;
        uint32_t maxOWD_; // Upper bound of the estimated latencyBound_
        ConcurrentQueue<std::pair<uint32_t, uint32_t>> owdQu_; // <replicaId, owd>


        int replicaNum_;
        int f_;
        int fastQuorum_;
        std::vector<std::vector<struct sockaddr_in*>> replicaAddrs_;
        ConcurrentMap<uint32_t, struct sockaddr_in*> clientAddrs_;
        ConcurrentMap<uint64_t, Reply*> committedReply_; // used as cache


    public:
        Proxy(const std::string& configFile = std::string("../configs/nezha-proxy-config.yaml"));
        ~Proxy();
        void Run();
        void Terminate();
    };

}