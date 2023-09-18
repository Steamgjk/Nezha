#include <yaml-cpp/yaml.h>
#include <fstream>
#include "lib/utils.h"
#include "proto/nezha_proto.pb.h"
#include "proxy_config.h"

namespace nezha {
using namespace nezha::proto;

/**
 * Refer to proxy_run.cc, the runnable program only needs to instantiate a
 * Proxy object with a configuration file. Then it calls Run() method to run
 * and calls Terminate() method to stop
 */

class Proxy {
 private:
  /** All the configuration parameters for this proxy are included in
   * proxyConfig_*/
  ProxyConfig proxyConfig_;
  /** Each thread is given a unique name (key) */
  std::map<std::string, std::thread*> threadPool_;

  /** Launch all the threads, these threads are mainly categorized into three
   * classes:
   * (1) ForwardRequestsTd, which receives client requests and
   * multicast to replicas;
   * (2) CheckQuorumTd, which receives replica replies and
   * check whether the corresponding request has been committed (use
   * isQuorumReady), if so, send a reply to the client;
   * (3) CalculateLatencyBoundTd, which caluldates the latency bound
   *
   * (1) and (2) handles most workload and is parallelized, and the parallism
   * degree is decided by the parameter defined in proxyConfig_ (i.e.,
   * shard-num).
   *
   * (1) and (2) are paired, i.e., we launch equal number of
   * ForwardRequestsTds and CheckQuorumTds. The requests multicast by
   * ForwardRequestsTd-i will be tracked and quorum-checked by CheckQuorumTd-i
   */
  void LaunchThreads();
  void ForwardRequestsTd(const int id = -1);
  void CheckQuorumTd(const int id = -1);
  void CalculateLatencyBoundTd();

  /** LogTd is just used to collect some performance stats. It is not necessary
   * in the release version */
  void LogTd();

  /** Create/Initialize all the necessary variables */
  void CreateContext();

  /** Check whether a quorum has been formed for the request to be committed.
   * If the request has been committed, it returns the reply message, which will
   * be delievered to the client; otherwise, it returns NULL
   */
  Reply* isQuorumReady(std::vector<uint64_t>& repliedSyncPoint,
                       std::map<uint32_t, Reply>& quorum);

  /** Tools function: given ip and port, create a socket fd. If ip is not empty,
   * the socket will be binded to the <ip:port>   */
  int CreateSocketFd(const std::string& ip = "", const int port = -1);

  /** Flag to Run/Terminate threads */
  std::atomic<bool> running_;

  /** Each CheckQuorumTd thread uses the socket fd in replyFds_, based on its
   * id, to send reply to clients
   */
  std::vector<int> replyFds_;

  /** Each ForwardRequestsTd thread uses the socket fd in forwardFds_, based on
   * its id, to multicast requests to replicas
   */
  std::vector<int> forwardFds_;

  /** Each ForwardRequestsTd thread uses the socket fd in requestReceiveFds_,
   * based on its id, to receive requests from clients
   */
  std::vector<int> requestReceiveFds_;

  /** We create a unique id for each ForwardRequestsTd, so that replicas can
   * derive which CheckQuorumTd should receive the reply messages */
  std::vector<uint64_t> proxyIds_;

  /** CalculateLatencyBoundTd updates latencyBound_ and concurrently
   * ForwardRequestsTds read it and included in request messages */
  std::atomic<uint32_t> latencyBound_;

  /** Upper bound of the estimated latencyBound_, used to clamp the bound,
   * details in ``Adapative Latency Bound`` para of Sec 4 of our paper */
  uint32_t maxOWD_;

  /** CheckQuorumTd threads pass <replicaId, owd> samples to
   * CalculateLatencyBoundTd */
  ConcurrentQueue<std::pair<uint32_t, uint32_t>> owdQu_;  //

  int replicaNum_;
  int f_;          /** replicaNum_ =2f_+1 */
  int fastQuorum_; /** fastQuorum_ = f_+ceiling(f_/2)+1 */

  /** Just used to collect logs, can be deleted in the release version*/
  struct Log {
    uint32_t replicaId_;
    uint32_t clientId_;
    uint32_t reqId_;
    uint64_t clientTime_;
    uint64_t proxyTime_;
    uint64_t proxyEndProcessTime_;
    uint64_t recvTime_;
    uint64_t deadline_;
    uint64_t fastReplyTime_;
    uint64_t slowReplyTime_;
    uint64_t proxyRecvTime_;
    uint32_t commitType_;

    Log(uint32_t rid = 0, uint32_t cId = 0, uint32_t reqId = 0,
        uint64_t ctime = 0, uint64_t ptime = 0, uint64_t pedtime = 0,
        uint64_t rtime = 0, uint64_t ddl = 0, uint64_t fttime = 0,
        uint64_t swtime = 0, uint64_t prcvt = 0, uint32_t cmtt = 0)
        : replicaId_(rid),
          clientId_(cId),
          reqId_(reqId),
          clientTime_(ctime),
          proxyTime_(ptime),
          recvTime_(rtime),
          deadline_(ddl),
          fastReplyTime_(fttime),
          slowReplyTime_(swtime),
          proxyRecvTime_(prcvt),
          commitType_(cmtt) {}
    std::string ToString() {
      return std::to_string(replicaId_) + "," + std::to_string(clientId_) +
             "," + std::to_string(reqId_) + "," + std::to_string(clientTime_) +
             "," + std::to_string(proxyTime_) + "," +
             std::to_string(proxyEndProcessTime_) + "," +
             std::to_string(recvTime_) + "," + std::to_string(deadline_) + "," +
             std::to_string(fastReplyTime_) + "," +
             std::to_string(slowReplyTime_) + "," +
             std::to_string(proxyRecvTime_) + "," + std::to_string(commitType_);
    }
  };
  ConcurrentQueue<Log> logQu_;

  /** Vector of replica's addresses
   * Since replicas can have multiple receiver shards, we use a two-dimensional
   * vector.
   *
   * replicaAddrs_[i] records the addresses of replica-i, which can receive
   * requests replicaAddrs_[i][j] is the address of the jth receiver shard of
   * replica-i.
   */
  std::vector<std::vector<struct sockaddr_in*>> replicaAddrs_;

  /**
   * After ForwardRequestTd receives client request, it records the address of
   * the client, so that later the correspoinding CheckQuorumTd can know which
   * address should recieve the commit reply.
   */
  ConcurrentMap<uint32_t, struct sockaddr_in*> clientAddrs_;

  /**
   * As an optimization, proxies also mantain a cache to record the commit reply
   * messages for those already-commited requests. In this way, when clients
   * retry the request which has already been committed, the proxy can direct
   * resend the reply, instead of adding additional burden to the replicas
   */

  std::vector<std::unordered_map<uint64_t, Reply*>> committedReplyMap_;

  std::vector<ConcurrentMap<uint64_t, uint64_t>> sendTimeMap_;

  std::vector<ConcurrentMap<uint64_t, Log*>> logMap_;

 public:
  /** Proxy accept a config file, which contains all the necessary information
   * to instantiate the object, then it can call Run method
   *  */
  Proxy(const std::string& configFile = "../configs/nezha-proxy-config.yaml");
  ~Proxy();
  void Run();
  void Terminate();

  /** Tentative */
  std::vector<std::vector<uint64_t>> replicaSyncedPoints_;
};

}  // namespace nezha