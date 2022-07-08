#include <arpa/inet.h>
#include <concurrentqueue.h>
#include <ev.h>
#include <fcntl.h>
#include <glog/logging.h>
#include <junction/ConcurrentMap_Leapfrog.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <unistd.h>
#include <yaml-cpp/yaml.h>
#include <chrono>
#include <fstream>
#include <iostream>
#include <thread>
#include <vector>
#include "lib/utils.h"
#include "lib/zipfian.h"
#include "proto/nezha_proto.pb.h"

namespace nezha {
using namespace nezha::proto;
template <typename T1>
using ConcurrentQueue = moodycamel::ConcurrentQueue<T1>;
template <typename T1, typename T2>
using ConcurrentMap = junction::ConcurrentMap_Leapfrog<T1, T2>;
struct LogInfo {
  uint32_t reqId;
  uint64_t sendTime;
  uint64_t commitTime;
  uint32_t commitType;
  std::string toString() {
    std::string ret =
        (std::to_string(reqId) + "," + std::to_string(sendTime) + "," +
         std::to_string(commitTime) + "," + std::to_string(commitType));
    return ret;
  }
};
class Client {
 private:
  std::map<std::string, std::thread*> threadPool_;
  YAML::Node clientConfig_;
  /** The endpoint to send requests */
  int endPointType_;
  Endpoint* requestEP_;
  struct EndpointMsgHandler* replyHandler_;
  struct EndpointTimer* monitorTimer_;
  std::atomic<bool> running_;
  std::atomic<bool> suspending_;
  int clientId_;
  int poissonRate_;
  std::atomic<uint32_t> nextReqId_;
  std::atomic<uint32_t> committedReqId_;
  std::atomic<uint32_t> reclaimedReqId_;
  std::vector<uint32_t> poissonTrace_;
  ConcurrentQueue<Request*> retryQu_;
  std::vector<std::vector<Address*>> proxyAddrs_;
  std::vector<uint32_t> zipfianKeys_;
  ConcurrentMap<uint32_t, Request*> outstandingRequests_;
  ConcurrentMap<uint32_t, uint64_t> outstandingRequestSendTime_;
  /** To communicate between ProcessReplyTd and LogTd */
  ConcurrentQueue<LogInfo*> logQu_;
  uint32_t retryTimeoutus_;
  uint32_t retryNumber_;
  uint32_t committedNum_;

  void LaunchThreads();
  void ProcessReplyTd();
  void OpenLoopSubmissionTd();
  void CloseLoopSubmissionTd();
  void LogTd();
  void ReceiveReply(MessageHeader* msgHdr, char* msgBuffer, Address* sender);
  void PrintConfig();

 public:
  Client(const std::string& configFile =
             std::string("../configs/nezha-client-config.yaml"));
  void Run();
  void Terminate();
  ~Client();
};

}  // namespace nezha