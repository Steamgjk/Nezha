#include <yaml-cpp/yaml.h>
#include <fstream>
#include <iostream>
#include "client_config.h"
#include "lib/utils.h"
#include "lib/zipfian.h"
#include "proto/nezha_proto.pb.h"

namespace nezha {
using namespace nezha::proto;
/** LogInfo is used to dump some performance stats, which can be extended to
 * include more metrics */
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

/**
 * Refer to client_run.cc, the runnable program only needs to instantiate a
 * client object with a configuration file. Then it calls Run() method to run
 * and calls Terminate() method to stop
 */
class Client {
 private:
  /** All the configuration parameters for client are included in
   * clientConfig_*/
  ClientConfig clientConfig_;
  /** Each thread is given a unique name (key) and stored in the pool */
  std::map<std::string, std::thread*> threadPool_;
  /** The endpoint uses to submit request to proxies */
  Endpoint* requestEP_;

  /** The message handler used to handle replies (from proxies) */
  struct MessageHandler* replyHandler_;
  /** The timer periodically monitor the status of the client, and break the
   * blocking endpoint when the client is about to terminate */
  struct Timer* monitorTimer_;

  /** Flag to Run/Terminate threads */
  std::atomic<bool> running_;

  /** Each client is assigned with a unqiue id */
  int clientId_;

  /** Open-Loop submission related: the client's submission rate follows a
   * poisson distribution. We use 10ms as the basic interval and generate random
   * numbers with reference to poissonRate_, stored in poissonTrace_. Then the
   * open-loop clients submit poissonTrace_[i] requests in the ith interval.
   *
   * Regarding the definition of open-loop and closed-loop submission, refer to
   * ``evaluation method`` para of Sec 7.1 in our paper
   * */
  int poissonRate_;

  /** The next requestId to be submitted */
  std::atomic<uint32_t> nextReqId_;

  /** Requests whose requestId less or equal to committedReqId_ have been
   * committed */
  std::atomic<uint32_t> committedReqId_;

  /** Requests whose requestId less or equal to reclaimedReqId_ have been
   * reclaimed (memory freed) */
  std::atomic<uint32_t> reclaimedReqId_;
  std::vector<uint32_t> poissonTrace_;

  /** To communicate between OpenLoopSubmissionTd/CloseLoopSubmissionTd and
   * LogTd The LogTd monitors the outstanding requests (i.e. which have been
   * submitted but have not been committed). If some request has not been
   * committed after a certain time, the LogTd will enqueue the request to
   * retryQu, so that the OpenLoopSubmissionTd/CloseLoopSubmissionTd will
   * retry them */
  ConcurrentQueue<Request*> retryQu_;

  /** The addresses of proxies. Since we can have multiple proxies, and each
   * proxies can have multiple shards, we use a two-dimensional vector to store
   * the addresses, i.e., proxyAddrs[i][j] indicates the address of the jth
   * shard of the ith proxy */
  std::vector<std::vector<Address*>> proxyAddrs_;

  /** To test commutativity, we generate different zipfian workloads and write
   * ratios, i.e., we generate random numbers following the zipfian
   * distribution. These random numbers are stored in zipfianKeys_ and serve as
   * the keys that will be written/read by requests */
  std::vector<uint32_t> zipfianKeys_;

  float writeRatio_;

  /** Those requests which have been submitted but not yet committed (key is the
   * requestId)*/
  ConcurrentMap<uint32_t, Request*> outstandingRequests_;

  /** Record the send time of the requests, together with retryTimeoutus_, to
   * decide whether the request needes to be retried*/
  ConcurrentMap<uint32_t, uint64_t> outstandingRequestSendTime_;

  /** Used by LogTd to monitor outstanding reuqests. If they cannot be committed
   * within retryTimeoutUs_ (measured in macro-seconds), they should be retried
   * **/
  uint32_t retryTimeoutUs_;

  /** To communicate between ProcessReplyTd and LogTd */
  ConcurrentQueue<LogInfo*> logQu_;

  /** Performance counters, to show how many requests are retried/committed */
  uint32_t retryNumber_;
  uint32_t committedNum_;
  uint32_t fastCommitNum_;
  uint32_t fastWriteNum_;

  /** Stats */
  std::vector<uint32_t> hop3s;
  std::vector<uint32_t> hop4s;
  std::vector<uint32_t> totals;

  /** Launch all the threads, only called once during the lifetime of the
   * client*/
  void LaunchThreads();

  /** Functions whose names are ended with ``Td`` will be used to instantiate
   * threads.
   *
   * For the client, there are mainly three worker threads running:
   *
   * (1) OpenLoopSubmissionTd/CloseLoopSubmissionTd submits requests. A client
   * can be either open-loop client or closed-loop client, but cannot be both.
   *
   * (2) ProcessReplyTd receives and processes the reply messages, and handle
   * the log information to LogTd
   *
   * (3) LogTd dumps logs and also monitors the oustanding requests. If the
   * requests have not been committed after a certain time (retryTimeoutus_),
   * then LogTd will ask OpenLoopSubmissionTd/CloseLoopSubmissionTd to resubmit
   * this reuqest to proxies
   * */
  void ProcessReplyTd();
  void OpenLoopSubmissionTd();
  void CloseLoopSubmissionTd();
  void LogTd();

  /** The message handler to handle messages from proxies. The function is used
   * to instantiate a replyHandler_ and registered to requestEP_ */
  void ReceiveReply(MessageHeader* msgHdr, char* msgBuffer, Address* sender);

 public:
  /** Client accepts a config file, which contains all the necessary information
   * to instantiate the object, then it can call Run method
   *  */
  Client(const std::string& configFile = "../configs/nezha-client-config.yaml");
  void Run();
  void Terminate();
  ~Client();

  /** For debug */
  uint64_t lastCommittedReqId_;
  std::vector<uint32_t> ls;
};

}  // namespace nezha