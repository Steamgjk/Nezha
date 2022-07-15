#include "client/client.h"

namespace nezha {
Client::Client(const std::string& configFile) {
  hop3s.reserve(500000);
  hop4s.reserve(500000);
  totals.reserve(500000);

  LOG(INFO) << "Loading config information from " << configFile;
  clientConfig_ = YAML::LoadFile(configFile);
  PrintConfig();
  clientId_ = clientConfig_["client-info"]["client-id"].as<int>();
  LOG(INFO) << "clientId=" << clientId_;
  std::string clientIP =
      clientConfig_["client-info"]["client-ip"].as<std::string>();
  LOG(INFO) << "clientIP=" << clientIP;
  int requestPort = clientConfig_["client-info"]["request-port"].as<int>();
  LOG(INFO) << "requestPort=" << requestPort;
  endPointType_ = clientConfig_["client-info"]["endpoint-type"].as<int>();
  LOG(INFO) << "endPointType=" << endPointType_;
  requestEP_ = CreateEndpoint(endPointType_, clientIP, requestPort, true);
  replyHandler_ = CreateMsgHandler(
      endPointType_,
      [](MessageHeader* msgHdr, char* msgBuffer, Address* sender, void* ctx) {
        ((Client*)ctx)->ReceiveReply(msgHdr, msgBuffer, sender);
      },
      this);

  monitorTimer_ = new Timer(
      [](void* ctx, void* receiverEP) {
        // LOG(INFO) << "Monitor running " << ((Client*)ctx)->running_;
        if (((Client*)ctx)->running_ == false) {
          ((Endpoint*)receiverEP)->LoopBreak();
        }
      },
      10 /*Checks the status every 10ms*/, this);

  /** Fetch the addreses of all proxies and organize them as a two-dimensional
   * vector */
  proxyAddrs_.resize(clientConfig_["proxy-info"]["proxy-ips"].size());
  for (uint32_t i = 0; i < proxyAddrs_.size(); i++) {
    proxyAddrs_[i].resize(
        clientConfig_["proxy-info"]["proxy-shards"].as<int>());
    for (uint32_t j = 0; j < proxyAddrs_[i].size(); j++) {
      proxyAddrs_[i][j] = new Address(
          clientConfig_["proxy-info"]["proxy-ips"][i].as<std::string>(),
          clientConfig_["proxy-info"]["request-port-base"].as<int>() + j);
    }
  }

  /** If the client is a open-loop client, generate the poission trace for the
   * client */
  if (clientConfig_["client-info"]["is-openloop"].as<bool>()) {
    poissonRate_ = clientConfig_["client-info"]["poisson-rate"].as<int>();
    LOG(INFO) << "OpenLoop Client rate=" << poissonRate_;
    poissonTrace_.resize(1000, 0);
    std::default_random_engine generator(clientId_);  // clientId as the seed
    std::poisson_distribution<int> distribution(poissonRate_);
    for (int i = 0; i < 1000; i++) {
      int reqNum = distribution(generator);
      if (reqNum < 0) {
        poissonTrace_[i] = 0;
      } else {
        poissonTrace_[i] = reqNum;
      }
    }
  }
  /** Generate zipfian workload */
  int keyNum = clientConfig_["client-info"]["key-num"].as<int>();
  float skewFactor = clientConfig_["client-info"]["skew-factor"].as<float>();
  LOG(INFO) << "keyNum=" << keyNum << "\tskewFactor=" << skewFactor;
  zipfianKeys_.resize(1000000, 0);
  retryTimeoutus_ =
      clientConfig_["client-info"]["request-retry-time-us"].as<uint32_t>();
  if (keyNum > 1) {
    std::default_random_engine generator(clientId_);  // clientId as the seed
    zipfian_int_distribution<uint32_t> zipfianDistribution(0, keyNum - 1,
                                                           skewFactor);
    for (uint32_t i = 0; i < zipfianKeys_.size(); i++) {
      zipfianKeys_[i] = zipfianDistribution(generator);
    }
  }

  /** Initialize */
  committedReqId_ = 0;
  reclaimedReqId_ = 0;
  nextReqId_ = 1;
  retryNumber_ = 0;
  committedNum_ = 0;
}

void Client::Run() {
  running_ = true;
  LaunchThreads();
  for (auto& kv : threadPool_) {
    LOG(INFO) << "Join " << kv.first;
    kv.second->join();
    LOG(INFO) << "Join Complete " << kv.first;
  }
  LOG(INFO) << "Run Terminated ";
}

void Client::PrintConfig() {
  if (clientConfig_["print-config"].as<bool>()) {
    LOG(INFO) << "Print configs as follows";
    LOG(INFO) << "Proxy Information";
    YAML::Node proxyConfig = clientConfig_["proxy-info"];
    LOG(INFO) << "\t"
              << "Proxy IPs";
    for (uint32_t i = 0; i < proxyConfig["proxy-ips"].size(); i++) {
      LOG(INFO) << "\t\t" << proxyConfig["proxy-ips"][i].as<std::string>();
    }
    LOG(INFO) << "\t"
              << "Proxy Shards:" << proxyConfig["proxy-shards"].as<int>();
    LOG(INFO) << "\t"
              << "Request Port Base:"
              << proxyConfig["request-port-base"].as<int>();

    YAML::Node clientConfig = clientConfig_["client-info"];
    LOG(INFO) << "Client Information";
    LOG(INFO) << "Client EndPoint Type: "
              << clientConfig["endpoint-type"].as<int>();
    LOG(INFO) << "\t"
              << "Client ID:" << clientConfig["client-id"].as<int>();
    LOG(INFO) << "\t"
              << "Client IP:" << clientConfig["client-ip"].as<std::string>();
    LOG(INFO) << "\t"
              << "Request(Reply) Port:"
              << clientConfig["request-port"].as<int>();
    LOG(INFO) << "\t"
              << "Is OpenLoop?" << clientConfig["is-openloop"].as<bool>();
    LOG(INFO) << "\t"
              << "Poisson Rate:" << clientConfig["poisson-rate"].as<int>();
    LOG(INFO) << "\t"
              << "Duration (sec):" << clientConfig["duration-sec"].as<int>();
    LOG(INFO) << "\t"
              << "Key Num:" << clientConfig["key-num"].as<int>();
    LOG(INFO) << "\t"
              << "Skew Factor (0-0.99):"
              << clientConfig["skew-factor"].as<float>();
    LOG(INFO) << "\t"
              << "Request Retry Time (us):"
              << clientConfig["request-retry-time-us"].as<int>();
  }
}

void Client::LaunchThreads() {
  threadPool_["LogTd"] = new std::thread(&Client::LogTd, this);
  threadPool_["ProcessReplyTd"] =
      new std::thread(&Client::ProcessReplyTd, this);
  if (clientConfig_["client-info"]["is-openloop"].as<bool>()) {
    LOG(INFO) << "OpenLoop Client";
    threadPool_["OpenLoopSubmissionTd"] =
        new std::thread(&Client::OpenLoopSubmissionTd, this);
  } else {
    LOG(INFO) << "ClosedLoop Client";
    threadPool_["CloseLoopSubmissionTd"] =
        new std::thread(&Client::CloseLoopSubmissionTd, this);
  }
}

void Client::ProcessReplyTd() {
  /** Register the message handler and timer. Then this thread will run in an
   * event-driven mode, i.e, when message comes, it calls the registered message
   * handler */
  requestEP_->RegisterMsgHandler(replyHandler_);
  requestEP_->RegisterTimer(monitorTimer_);
  LOG(INFO) << "Loop Run ";
  requestEP_->LoopRun();
  LOG(INFO) << "Loop Run Exit ";
}

void Client::ReceiveReply(MessageHeader* msgHdr, char* msgBuffer,
                          Address* sender) {
  if (msgHdr->msgLen < 0) {
    return;
  }
  Reply reply;
  if (msgHdr->msgType == MessageType::COMMIT_REPLY &&
      reply.ParseFromArray(msgBuffer, msgHdr->msgLen)) {
    committedNum_++;
    uint64_t sendTime = outstandingRequestSendTime_.get(reply.reqid());
    if (sendTime > 0) {
      /** The corresponding request has not been committed, because it is still
       * in outstandingRequestSendTime_, so we wan to mark it as committed,
       * i.e., erase from outstandingRequestSendTime_
       */

      /**
       * Generate log information and pass to logQu_, which will be handled by
       * LogTd
       * */
      uint64_t recvTime = GetMicrosecondTimestamp();
      LogInfo* log = new LogInfo();
      lastCommittedReqId_ = reply.reqid();
      *log = {reply.reqid(), sendTime, recvTime, reply.replytype()};
      outstandingRequestSendTime_.erase(reply.reqid());
      logQu_.enqueue(log);
    }
  }
}

void Client::OpenLoopSubmissionTd() {
  int roundRobinIdx = 0;
  uint64_t startTime = GetMicrosecondTimestamp();
  uint64_t endTime =
      startTime +
      clientConfig_["client-info"]["duration-sec"].as<uint64_t>() * 1000000;

  endTime += 10 * 1000ul * 1000ul;
  LOG(INFO) << "Expected to end at " << endTime;
  // Poisson rate is ``10ms as one unit''
  for (uint32_t i = 0;
       i < clientConfig_["client-info"]["duration-sec"].as<uint32_t>() * 100;
       i++) {
    if (!running_) {
      return;
    }
    if (GetMicrosecondTimestamp() >= endTime) {
      // Client has executed long enough, should terminate
      LOG(INFO) << "Terminating soon...";
      running_ = false;
      return;
    }
    uint32_t reqNum = poissonTrace_[i % poissonTrace_.size()];
    if (reqNum <= 0) {
      usleep(10000);
      continue;
    }
    uint32_t intval = 10000 / reqNum;
    uint64_t startTime = GetMicrosecondTimestamp();
    for (uint32_t j = 0; j < reqNum; j++) {
      while (GetMicrosecondTimestamp() < startTime + j * intval) {
      }
      // Send the request
      uint32_t mapIdx =
          roundRobinIdx % (proxyAddrs_.size() * proxyAddrs_[0].size());
      Request* request = NULL;
      if (retryQu_.try_dequeue(request)) {
        request->set_clienttime(GetMicrosecondTimestamp());  // To Delete
        // Retry this request
        Address* roundRobinAddr = proxyAddrs_[mapIdx % proxyAddrs_.size()]
                                             [mapIdx / proxyAddrs_.size()];
        // LOG(INFO) << "Resend " << request->reqid() << "to "
        //           << mapIdx % proxyAddrs_.size() << "\t"
        //           << mapIdx / proxyAddrs_.size();
        requestEP_->SendMsgTo(*roundRobinAddr, *request,
                              MessageType::CLIENT_REQUEST);
        outstandingRequestSendTime_.assign(request->reqid(),
                                           GetMicrosecondTimestamp());
        roundRobinIdx++;
      } else {
        // submit new requests
        request = new Request();
        request->set_clientid(clientId_);
        request->set_reqid(nextReqId_);
        request->set_command("");
        request->set_key(zipfianKeys_[nextReqId_ % zipfianKeys_.size()]);
        // request->set_key(nextReqId_ % 100000 + 100000 * (clientId_ - 1));
        request->set_clienttime(GetMicrosecondTimestamp());  // To Delete
        Address* roundRobinAddr = proxyAddrs_[mapIdx % proxyAddrs_.size()]
                                             [mapIdx / proxyAddrs_.size()];
        // LOG(INFO) << "Sed " << request->reqid() << "to "
        //           << mapIdx % proxyAddrs_.size() << "\t"
        //           << mapIdx / proxyAddrs_.size();
        requestEP_->SendMsgTo(*roundRobinAddr, *request,
                              MessageType::CLIENT_REQUEST);
        outstandingRequests_.assign(request->reqid(), request);
        outstandingRequestSendTime_.assign(request->reqid(),
                                           GetMicrosecondTimestamp());
        nextReqId_++;
        roundRobinIdx++;
      }
    }
  }

  LOG(INFO) << "Terminating soon... after "
            << (endTime - GetMicrosecondTimestamp()) * 1e-6 << " seconds";
  while (GetMicrosecondTimestamp() < endTime) {
    // Client has executed long enough, should terminate
    usleep(1000);
  }
  running_ = false;
}

void Client::CloseLoopSubmissionTd() {
  int roundRobinIdx = 0;
  uint64_t startTime = GetMicrosecondTimestamp();
  uint64_t endTime =
      startTime +
      clientConfig_["client-info"]["duration-sec"].as<uint64_t>() * 1000000;
  endTime += 10 * 1000ul * 1000ul;
  LOG(INFO) << "Expected to end at " << endTime;
  while (running_) {
    if (GetMicrosecondTimestamp() >= endTime) {
      // Client has executed long enough, should terminate
      LOG(INFO) << "Terminating soon...";
      running_ = false;
      return;
    }
    Request* request = NULL;
    uint32_t mapIdx =
        roundRobinIdx % (proxyAddrs_.size() * proxyAddrs_[0].size());
    if (nextReqId_ == committedReqId_ + 1) {
      // submit new request
      request = new Request();
      request->set_clientid(clientId_);
      request->set_reqid(nextReqId_);
      request->set_command("");
      request->set_key(zipfianKeys_[nextReqId_ % zipfianKeys_.size()]);
      Address* roundRobinAddr =
          proxyAddrs_[mapIdx % proxyAddrs_.size()][mapIdx / proxyAddrs_.size()];
      requestEP_->SendMsgTo(*roundRobinAddr, *request,
                            MessageType::CLIENT_REQUEST);
      outstandingRequests_.assign(request->reqid(), request);
      outstandingRequestSendTime_.assign(request->reqid(),
                                         GetMicrosecondTimestamp());
      nextReqId_++;
      roundRobinIdx++;
    } else {
      if (retryQu_.try_dequeue(request)) {
        // have some requests to retry
        Address* roundRobinAddr = proxyAddrs_[mapIdx % proxyAddrs_.size()]
                                             [mapIdx / proxyAddrs_.size()];
        requestEP_->SendMsgTo(*roundRobinAddr, *request,
                              MessageType::CLIENT_REQUEST);
        outstandingRequestSendTime_.assign(request->reqid(),
                                           GetMicrosecondTimestamp());
        roundRobinIdx++;
      }
    }
  }
  LOG(INFO) << "Terminating soon... after "
            << (endTime - GetMicrosecondTimestamp()) * 1e-6 << " seconds";
  while (GetMicrosecondTimestamp() < endTime) {
    // Client has executed long enough, should terminate
    usleep(1000);
  }
  running_ = false;
}

void Client::LogTd() {
  LogInfo* log = NULL;
  uint64_t startTime, endTime;
  uint32_t lastSubmitteddReqId = 0;
  uint32_t lastCountCommitedReq = 0;
  uint32_t latencySample = 0;

  std::ofstream ofs("Client-Stats-" + std::to_string(clientId_));
  ofs << "ReqId,SendTime,CommitTime,CommitType" << std::endl;

  startTime = GetMicrosecondTimestamp();
  while (running_) {
    endTime = GetMicrosecondTimestamp();
    if (endTime - startTime >= 2000000) {
      float duration = (endTime - startTime) * 1e-6;
      uint32_t submittedReqNum = nextReqId_ - 1 - lastSubmitteddReqId;
      uint32_t committedReqNum = committedNum_ - lastCountCommitedReq;
      float submissionRate = submittedReqNum / duration;
      float commitRate = committedReqNum / duration;
      lastSubmitteddReqId = nextReqId_ - 1;
      lastCountCommitedReq = committedNum_;
      startTime = endTime;
      LOG(INFO) << "endTime=" << endTime << "\t"
                << "committedNum_ = " << committedNum_ << "\t"
                << "logQuLen =" << logQu_.size_approx() << "\t"
                << "committedReqId_=" << committedReqId_ << "\t"
                << "nextReqId_=" << nextReqId_ << "\t"
                << "lastCommittedReqId_=" << lastCommittedReqId_ << "\t"
                << "submissionRate=" << submissionRate << " req/sec\t"
                << "commitRate=" << commitRate << " req/sec"
                << "\t"
                << "latency(Sample)=" << latencySample << " us"
                << "\t"
                << "retryNum=" << retryNumber_;

      ofs.flush();
    }
    if (logQu_.try_dequeue(log)) {
      // LOG(INFO) << "committedReqId_=" << committedReqId_ << "\t" << "reqId="
      // << log->reqId;
      while (committedReqId_ + 1 <= log->reqId) {
        if (outstandingRequestSendTime_.get(committedReqId_ + 1) == 0) {
          // this reqId has also been committed (i.e. cannot find its footprint)
          // advance committedReqId;
          committedReqId_++;
        } else {
          break;
        }
      }

      latencySample = log->commitTime - log->sendTime;

      // log stats
      ofs << log->toString() << std::endl;
      delete log;
    }

    // Check whether any requests need retry
    for (uint32_t reqId = committedReqId_ + 1; reqId < nextReqId_; reqId++) {
      uint64_t sendTime = outstandingRequestSendTime_.get(reqId);
      if (sendTime > 0) {
        // Find it
        if (false && GetMicrosecondTimestamp() - sendTime > retryTimeoutus_) {
          // timeout, should retry
          Request* request = outstandingRequests_.get(reqId);
          VLOG(1) << "Timeout Retry " << request->reqid();
          outstandingRequestSendTime_.erase(reqId);
          retryQu_.enqueue(request);
          retryNumber_++;
        }
      }
    }

    while (reclaimedReqId_ + 1000 < committedReqId_) {
      // do not reclaim request too aggressive
      // If we reclaim too aggressive, there can be some edge case of dangling
      // request pointer
      Request* request = outstandingRequests_.get(reclaimedReqId_);
      if (request) {
        outstandingRequests_.erase(request->reqid());
        delete request;
      }
      reclaimedReqId_++;
    }
  }

  while (logQu_.try_dequeue(log)) {
    // log stats
    ofs << log->toString() << std::endl;
    delete log;
  }
  ofs.flush();
  LOG(INFO) << "Dump Finished";
}

void Client::Terminate() {
  LOG(INFO) << "Terminating...";
  running_ = false;
}

Client::~Client() {
  for (auto& kv : threadPool_) {
    delete kv.second;
  }
  while (reclaimedReqId_ <= nextReqId_) {
    Request* request = outstandingRequests_.get(reclaimedReqId_);
    if (request) {
      outstandingRequests_.erase(request->reqid());
      delete request;
    }
    reclaimedReqId_++;
  }
}
}  // namespace nezha