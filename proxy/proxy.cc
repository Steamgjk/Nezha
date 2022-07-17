#include "proxy/proxy.h"

namespace nezha {
Proxy::Proxy(const std::string& configFile) {
  proxyConfig_ = YAML::LoadFile(configFile);
  PrintConfig();
  CreateContext();
}

void Proxy::PrintConfig() {
  if (proxyConfig_["print-config"].as<bool>()) {
    LOG(INFO) << "Print configs as follows";
    LOG(INFO) << "Replica Information";
    YAML::Node replicaConfig = proxyConfig_["replica-info"];
    LOG(INFO) << "\t"
              << "Replica IPs";
    for (uint32_t i = 0; i < replicaConfig["replica-ips"].size(); i++) {
      LOG(INFO) << "\t\t" << replicaConfig["replica-ips"][i].as<std::string>();
    }
    LOG(INFO) << "\t"
              << "Replica Receiver Shards:"
              << replicaConfig["receiver-shards"].as<int>();
    LOG(INFO) << "\t"
              << "Replica Receiver Port:"
              << replicaConfig["receiver-port"].as<int>();
    LOG(INFO) << "\t"
              << "Initial One-Way Delay(us):"
              << replicaConfig["initial-owd"].as<uint32_t>();

    YAML::Node proxyConfig = proxyConfig_["proxy-info"];
    LOG(INFO) << "Proxy Information";
    LOG(INFO) << "\t"
              << "Proxy ID:" << proxyConfig["proxy-id"].as<int>();
    LOG(INFO) << "\t"
              << "Proxy IP:" << proxyConfig["proxy-ip"].as<std::string>();
    LOG(INFO) << "\t"
              << "Shard Number:" << proxyConfig["shard-num"].as<int>();
    LOG(INFO) << "\t"
              << "Max OWD:" << proxyConfig["max-owd"].as<uint32_t>();
    LOG(INFO) << "\t"
              << "Request Port Base:"
              << proxyConfig["request-port-base"].as<int>();
    LOG(INFO) << "\t"
              << "Reply Port Base:" << proxyConfig["reply-port-base"].as<int>();
  }
}

void Proxy::Terminate() {
  LOG(INFO) << "Terminating...";
  running_ = false;
}

void Proxy::Run() {
  running_ = true;
  LaunchThreads();
  for (auto& kv : threadPool_) {
    LOG(INFO) << "Join " << kv.first;
    kv.second->join();
    LOG(INFO) << "Join Complete " << kv.first;
  }
  LOG(INFO) << "Run Terminated ";
}

Proxy::~Proxy() {
  for (auto& kv : threadPool_) {
    delete kv.second;
  }

  for (uint32_t i = 0; i < replicaAddrs_.size(); i++) {
    for (uint32_t j = 0; j < replicaAddrs_[0].size(); j++) {
      if (replicaAddrs_[i][j]) {
        delete replicaAddrs_[i][j];
      }
    }
  }

  // Clear Context (free memory)
  ConcurrentMap<uint32_t, struct sockaddr_in*>::Iterator clientIter(
      clientAddrs_);
  while (clientIter.isValid()) {
    if (clientIter.getValue()) {
      delete clientIter.getValue();
    }
    clientIter.next();
  }

  for (uint32_t i = 0; i < committedReplyMap_.size(); i++) {
    ConcurrentMap<uint64_t, Reply*>& committedReply = committedReplyMap_[i];
    ConcurrentMap<uint64_t, Reply*>::Iterator iter(committedReply);
    while (iter.isValid()) {
      Reply* reply = iter.getValue();
      if (reply) {
        delete reply;
      }
      iter.next();
    }
  }
}

int Proxy::CreateSocketFd(const std::string& sip, const int sport) {
  int fd = socket(PF_INET, SOCK_DGRAM, 0);
  if (fd < 0) {
    LOG(ERROR) << "Receiver Fd fail ";
    return -1;
  }
  // Set Non-Blocking
  int status = fcntl(fd, F_SETFL, fcntl(fd, F_GETFL, 0) | O_NONBLOCK);
  if (status < 0) {
    LOG(ERROR) << " Set NonBlocking Fail";
    return -1;
  }

  if (sip != "") {
    struct sockaddr_in addr;
    bzero(&addr, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(sport);
    addr.sin_addr.s_addr = inet_addr(sip.c_str());
    // Bind socket to Address
    int bindRet = bind(fd, (struct sockaddr*)&addr, sizeof(addr));
    if (bindRet != 0) {
      LOG(ERROR) << "bind error\t" << bindRet;
      return -1;
    }
  }
  return fd;
}

void Proxy::LaunchThreads() {
  int shardNum = proxyConfig_["proxy-info"]["shard-num"].as<int>();

  threadPool_["CalcLatencyBound"] =
      new std::thread(&Proxy::CalculateLatencyBoundTd, this);
  for (int i = 0; i < shardNum; i++) {
    std::string key = "CheckQuorumTd-" + std::to_string(i);
    threadPool_[key] = new std::thread(&Proxy::CheckQuorumTd, this, i);
  }

  for (int i = 0; i < shardNum; i++) {
    std::string key = "ForwardRequestsTd-" + std::to_string(i);
    threadPool_[key] = new std::thread(&Proxy::ForwardRequestsTd, this, i);
  }

  std::string key = "LogTd";
  threadPool_[key] = new std::thread(&Proxy::LogTd, this);
}

void Proxy::CalculateLatencyBoundTd() {
  std::pair<uint32_t, uint32_t> owdSample;
  std::vector<uint32_t> replicaOWDs;
  replicaOWDs.resize(
      proxyConfig_["replica-info"]["replica-ips"].size(),
      proxyConfig_["replica-info"]["initial-owd"].as<uint32_t>());
  for (uint32_t i = 0; i < replicaOWDs.size(); i++) {
    LOG(INFO) << "replicaOWD " << i << "\t" << replicaOWDs[i];
  }
  while (running_) {
    if (owdQu_.try_dequeue(owdSample)) {
      VLOG(1) << "replica=" << owdSample.first << "\towd=" << owdSample.second;
      replicaOWDs[owdSample.first] = owdSample.second;
      // Update latency bound
      uint32_t estimatedOWD = 0;
      for (uint32_t i = 0; i < replicaOWDs.size(); i++) {
        if (estimatedOWD < replicaOWDs[i]) {
          estimatedOWD = replicaOWDs[i];
        }
      }
      if (estimatedOWD > maxOWD_) {
        estimatedOWD = maxOWD_;
      }
      latencyBound_.store(estimatedOWD);
      VLOG(1) << "Update bound " << latencyBound_;
    }
  }
}

void Proxy::LogTd() {
  Log litem;
  int proxyId = proxyConfig_["proxy-info"]["proxy-id"].as<int>();
  std::ofstream ofs("Proxy-Stats-" + std::to_string(proxyId) + ".csv");
  ofs << "ReplicaId,ClientTime,ProxyTime,RecvTime,Deadline,FastReplyTime,"
         "SlowReplyTime,"
         "ProxyRecvTime,CommitType"
      << std::endl;
  uint32_t logCnt = 0;
  while (running_) {
    if (logQu_.try_dequeue(litem)) {
      ofs << litem.ToString() << std::endl;
      logCnt++;
      if (logCnt % 10000 == 0) {
        ofs.flush();
      }
    }
  }
}

void Proxy::CheckQuorumTd(const int id) {
  ConcurrentMap<uint64_t, Reply*>& committedReply = committedReplyMap_[id];
  ConcurrentMap<uint64_t, Log*>& logs = logMap_[id];
  std::map<uint64_t, std::map<uint32_t, Reply>> replyQuorum;
  int sz = 0;
  char buffer[UDP_BUFFER_SIZE];
  MessageHeader* msgHdr = (MessageHeader*)(void*)buffer;
  struct sockaddr_in recvAddr;
  socklen_t sockLen = sizeof(recvAddr);
  Reply reply;
  Reply* committedAck = NULL;
  uint32_t replyNum = 0;
  uint64_t startTime, endTime;
  while (running_) {
    if ((sz = recvfrom(forwardFds_[id], buffer, UDP_BUFFER_SIZE, 0,
                       (struct sockaddr*)(&recvAddr), &sockLen)) > 0) {
      if ((uint32_t)sz < sizeof(MessageHeader) ||
          (uint32_t)sz < msgHdr->msgLen + sizeof(MessageHeader)) {
        continue;
      }

      if (reply.ParseFromArray(buffer + sizeof(MessageHeader),
                               msgHdr->msgLen)) {
        uint64_t reqKey = CONCAT_UINT32(reply.clientid(), reply.reqid());
        if (reply.owd() > 0) {
          owdQu_.enqueue(
              std::pair<uint32_t, uint32_t>(reply.replicaid(), reply.owd()));
        }

        uint64_t syncPoint = CONCAT_UINT32(reply.view(), reply.syncedlogid());
        if (replicaSyncedPoints_[reply.replicaid()] < syncPoint) {
          replicaSyncedPoints_[reply.replicaid()] = syncPoint;
        }

        committedAck = committedReply.get(reqKey);
        if (committedAck != NULL) {
          // already committed;  ignore
          continue;
        }

        // LOG(INFO) << reply.DebugString();
        if (reply.replytype() == (uint32_t)MessageType::COMMIT_REPLY) {
          committedAck = new Reply(reply);
          committedReply.assign(reqKey, committedAck);
        } else if (replyQuorum[reqKey].find(reply.replicaid()) ==
                   replyQuorum[reqKey].end()) {
          replyQuorum[reqKey][reply.replicaid()] = reply;
          committedAck = isQuorumReady(replyQuorum[reqKey]);
        } else if (reply.view() > replyQuorum[reqKey].begin()->second.view()) {
          // New view has come, clear existing replies for this request
          replyQuorum[reqKey].clear();
          replyQuorum[reqKey][reply.replicaid()] = reply;
          committedAck = isQuorumReady(replyQuorum[reqKey]);
        } else if (reply.view() == replyQuorum[reqKey].begin()->second.view()) {
          const Reply& existedReply = replyQuorum[reqKey][reply.replicaid()];
          if (existedReply.view() < reply.view()) {
            replyQuorum[reqKey][reply.replicaid()] = reply;
          } else if (existedReply.view() == reply.view() &&
                     existedReply.replytype() < reply.replytype()) {
            // FAST_REPLY < SLOW_REPLY < COMMIT_REPLY
            replyQuorum[reqKey][reply.replicaid()] = reply;
          }
          committedAck = isQuorumReady(replyQuorum[reqKey]);
        }  // else: reply.view()< replyQuorum[reqKey].begin()->second.view(),
           // ignore it

        if (committedAck != NULL) {
          // Ack to client
          struct sockaddr_in* clientAddr = clientAddrs_.get(reply.clientid());
          std::string replyMsg = committedAck->SerializeAsString();
          msgHdr->msgType = MessageType::COMMIT_REPLY;
          msgHdr->msgLen = replyMsg.length();
          memcpy(buffer + sizeof(MessageHeader), replyMsg.c_str(),
                 replyMsg.length());
          sendto(replyFds_[id], buffer,
                 replyMsg.length() + sizeof(MessageHeader), 0,
                 (struct sockaddr*)clientAddr, sizeof(sockaddr));

          Log* litem = logs.get(reqKey);
          if (litem) {
            litem->proxyRecvTime_ = GetMicrosecondTimestamp();
            litem->commitType_ = committedAck->replytype();
            logQu_.enqueue(*litem);
          }

          // Add to cache
          committedReply.assign(reqKey, committedAck);
          replyQuorum.erase(reqKey);
          // LOG(INFO) << "reqId=" << committedAck->reqid()
          //           << "\t type=" << committedAck->replytype();
          // replyNum++;
          // if (replyNum == 1) {
          //   startTime = GetMicrosecondTimestamp();
          // } else if (replyNum % 100 == 0) {
          //   endTime = GetMicrosecondTimestamp();
          //   float rate = 100 / ((endTime - startTime) * 1e-6);
          //   LOG(INFO) << "id=" << id << "\t"
          //             << "replyNum=" << replyNum << "\t"
          //             << "rate = " << rate;
          //   startTime = endTime;
          // }
        }
      }
    }
  }
}

// Reply* Proxy::isQuorumReady(std::map<uint32_t, Reply>& quorum) {
//   // These replies are of the same view for sure (we have previously
//   forbidden
//   // inconsistency)
//   uint32_t view = quorum.begin()->second.view();
//   uint32_t leaderId = view % replicaNum_;
//   if (quorum.find(leaderId) == quorum.end()) {
//     return NULL;
//   }

//   Reply& leaderReply = quorum[leaderId];

//   uint32_t fastOrSlowReplyNum = 0;  // slowReply can be used as fastReply
//   uint32_t slowReplyNum = 0;        // But fastReply cannot be used as
//   slowReply for (const auto& kv : quorum) {
//     bool fastSatisfied = (kv.second.replytype() == MessageType::FAST_REPLY &&
//                           kv.second.view() == leaderReply.view() &&
//                           kv.second.hash() == leaderReply.hash());
//     bool slowSatisfied =
//         (HIGH_32BIT(replicaSyncedPoints_[kv.first]) == leaderReply.view() &&
//          LOW_32BIT(replicaSyncedPoints_[kv.first]) >=
//              leaderReply.syncedlogid());

//     if (fastSatisfied || slowSatisfied) {
//       fastOrSlowReplyNum++;
//     }
//     if (slowSatisfied) {
//       slowReplyNum++;
//     }
//   }

//   if (fastOrSlowReplyNum >= (uint32_t)fastQuorum_) {
//     // Fast Commit
//     Reply* committedReply = new Reply(leaderReply);
//     committedReply->set_replytype(MessageType::FAST_REPLY);
//     return committedReply;
//   }

//   if (slowReplyNum >= (uint32_t)f_) {
//     // together with leader's fast reply, it forms the quorum of f+1
//     Reply* committedReply = new Reply(leaderReply);
//     committedReply->set_replytype(MessageType::SLOW_REPLY);
//     return committedReply;
//   }

//   return NULL;
// }

Reply* Proxy::isQuorumReady(std::map<uint32_t, Reply>& quorum) {
  // These replies are of the same view for sure (we have previously forbidden
  // inconsistency)
  uint32_t view = quorum.begin()->second.view();
  uint32_t leaderId = view % replicaNum_;
  if (quorum.find(leaderId) == quorum.end()) {
    return NULL;
  }

  Reply& leaderReply = quorum[leaderId];

  uint32_t fastOrSlowReplyNum = 0;  // slowReply can be used as fastReply
  uint32_t slowReplyNum = 0;        // But fastReply cannot be used as slowReply
  for (const auto& kv : quorum) {
    if (kv.second.replytype() == MessageType::FAST_REPLY &&
            kv.second.hash() == leaderReply.hash() ||
        kv.second.replytype() == MessageType::SLOW_REPLY) {
      fastOrSlowReplyNum++;
    }
    if (kv.second.replytype() == MessageType::SLOW_REPLY) {
      slowReplyNum++;
    }
  }

  if (fastOrSlowReplyNum >= (uint32_t)fastQuorum_) {
    // Fast Commit
    Reply* committedReply = new Reply(leaderReply);
    committedReply->set_replytype(MessageType::FAST_REPLY);
    return committedReply;
  }

  if (slowReplyNum >= (uint32_t)f_) {
    // together with leader's fast reply, it forms the quorum of f+1
    Reply* committedReply = new Reply(leaderReply);
    committedReply->set_replytype(MessageType::SLOW_REPLY);
    return committedReply;
  }

  return NULL;
}

void Proxy::ForwardRequestsTd(const int id) {
  ConcurrentMap<uint64_t, Reply*>& committedReply = committedReplyMap_[id];
  ConcurrentMap<uint64_t, Log*>& logs = logMap_[id];
  char buffer[UDP_BUFFER_SIZE];
  MessageHeader* msgHdr = (MessageHeader*)(void*)buffer;
  int sz = -1;
  struct sockaddr_in receiverAddr;
  socklen_t len = sizeof(receiverAddr);
  Request request;
  uint32_t forwardCnt = 0;
  uint64_t startTime, endTime;

  while (running_) {
    if ((sz = recvfrom(requestReceiveFds_[id], buffer, UDP_BUFFER_SIZE, 0,
                       (struct sockaddr*)&receiverAddr, &len)) > 0) {
      if ((uint32_t)sz < sizeof(MessageHeader) ||
          (uint32_t)sz < msgHdr->msgLen + sizeof(MessageHeader)) {
        continue;
      }
      if (msgHdr->msgType == MessageType::CLIENT_REQUEST &&
          request.ParseFromArray(buffer + sizeof(MessageHeader),
                                 msgHdr->msgLen)) {
        uint64_t reqKey = CONCAT_UINT32(request.clientid(), request.reqid());
        Reply* commitAck = committedReply.get(reqKey);
        if (commitAck != NULL) {
          std::string replyStr = commitAck->SerializeAsString();
          msgHdr->msgType = MessageType::COMMIT_REPLY;
          msgHdr->msgLen = replyStr.length();
          memcpy(buffer + sizeof(MessageHeader), replyStr.c_str(),
                 replyStr.length());
          sendto(requestReceiveFds_[id], buffer,
                 replyStr.length() + sizeof(MessageHeader), 0,
                 (struct sockaddr*)&receiverAddr, len);
          continue;
        }

        request.set_bound(latencyBound_);
        request.set_proxyid(proxyIds_[id]);
        request.set_sendtime(GetMicrosecondTimestamp());

        std::string msg = request.SerializeAsString();
        msgHdr->msgType = MessageType::CLIENT_REQUEST;
        msgHdr->msgLen = msg.length();
        memcpy(buffer + sizeof(MessageHeader), msg.c_str(), msg.length());
        if (clientAddrs_.get(request.clientid()) == NULL) {
          struct sockaddr_in* addr = new sockaddr_in(receiverAddr);
          clientAddrs_.assign(request.clientid(), addr);
        }

        // Send to every replica
        for (int i = 0; i < replicaNum_; i++) {
          // uint32_t generateProxyId = (uint32_t)(proxyIds_[id] >> 32u);
          // struct sockaddr_in* replicaAddr =
          //     replicaAddrs_[i][generateProxyId % replicaAddrs_[i].size()];
          struct sockaddr_in* replicaAddr =
              replicaAddrs_[i][proxyIds_[id] % replicaAddrs_[i].size()];

          sendto(forwardFds_[id], buffer,
                 msgHdr->msgLen + sizeof(MessageHeader), 0,
                 (struct sockaddr*)replicaAddr, sizeof(sockaddr_in));
        }

        Log* litem = new Log();
        litem->proxyTime_ = request.sendtime();
        litem->deadline_ = request.sendtime() + request.bound();
        logs.assign(reqKey, litem);
        // LOG(INFO) << "id=" << id << "\t"
        //           << "cid=" << request.clientid() << "\t" << request.reqid();

        // forwardCnt++;
        // if (forwardCnt == 1) {
        //   startTime = GetMicrosecondTimestamp();
        // } else if (forwardCnt % 100 == 0) {
        //   endTime = GetMicrosecondTimestamp();
        //   float rate = 100 / ((endTime - startTime) * 1e-6);
        //   LOG(INFO) << "Forward-Id=" << id << "\t"
        //             << "count =" << forwardCnt << "\t"
        //             << "rate=" << rate << " req/sec"
        //             << "\t"
        //             << "req is <" << request.clientid() << ","
        //             << request.reqid() << ">";
        //   startTime = endTime;
        // }
      }
    }
  }
}

void Proxy::CreateContext() {
  running_ = true;
  int shardNum = proxyConfig_["proxy-info"]["shard-num"].as<int>();
  std::string ip = proxyConfig_["proxy-info"]["proxy-ip"].as<std::string>();
  int requestPortBase =
      proxyConfig_["proxy-info"]["request-port-base"].as<int>();
  int replyPortBase = proxyConfig_["proxy-info"]["reply-port-base"].as<int>();
  int replicaReceiverPortBase =
      proxyConfig_["replica-info"]["receiver-port"].as<int>();
  int replicaReceiverShard =
      proxyConfig_["replica-info"]["receiver-shards"].as<int>();
  uint32_t proxyId = proxyConfig_["proxy-info"]["proxy-id"].as<uint32_t>();
  forwardFds_.resize(shardNum, -1);
  requestReceiveFds_.resize(shardNum, -1);
  replyFds_.resize(shardNum, -1);
  proxyIds_.resize(shardNum, proxyId);
  latencyBound_ = proxyConfig_["replica-info"]["initial-owd"].as<uint32_t>();
  maxOWD_ = proxyConfig_["proxy-info"]["max-owd"].as<uint32_t>();
  for (int i = 0; i < shardNum; i++) {
    forwardFds_[i] = CreateSocketFd(ip, replyPortBase + i);
    requestReceiveFds_[i] = CreateSocketFd(ip, requestPortBase + i);
    replyFds_[i] = CreateSocketFd("", -1);
    proxyIds_[i] = ((proxyIds_[i] << 32) | (uint32_t)i);
  }
  committedReplyMap_.resize(shardNum);
  logMap_.resize(shardNum);

  replicaNum_ = proxyConfig_["replica-info"]["replica-ips"].size();
  assert(replicaNum_ % 2 == 1);
  f_ = replicaNum_ / 2;
  replicaSyncedPoints_.assign(replicaNum_, CONCURRENT_MAP_START_INDEX);

  fastQuorum_ = (f_ % 2 == 1) ? (f_ + (f_ + 1) / 2 + 1) : (f_ + f_ / 2 + 1);
  replicaAddrs_.resize(replicaNum_);
  for (int i = 0; i < replicaNum_; i++) {
    std::string replicaIP =
        proxyConfig_["replica-info"]["replica-ips"][i].as<std::string>();
    for (int j = 0; j < replicaReceiverShard; j++) {
      struct sockaddr_in* addr = new sockaddr_in();
      bzero(addr, sizeof(struct sockaddr_in));
      addr->sin_family = AF_INET;
      addr->sin_port = htons(replicaReceiverPortBase + j);
      addr->sin_addr.s_addr = inet_addr(replicaIP.c_str());
      replicaAddrs_[i].push_back(addr);
    }
  }
}

}  // namespace nezha
