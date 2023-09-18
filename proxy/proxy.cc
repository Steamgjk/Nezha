#include "proxy/proxy.h"

namespace nezha {
Proxy::Proxy(const std::string& configFile) {
  std::string error = proxyConfig_.parseConfig(configFile);
  if (error != "") {
    LOG(ERROR) << "Error parsing proxy config: " << error << "Exiting.";
    exit(1);
  }
  CreateContext();
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

  // for (uint32_t i = 0; i < committedReplyMap_.size(); i++) {
  //   ConcurrentMap<uint64_t, Reply*>& committedReply = committedReplyMap_[i];
  //   ConcurrentMap<uint64_t, Reply*>::Iterator iter(committedReply);
  //   while (iter.isValid()) {
  //     Reply* reply = iter.getValue();
  //     if (reply) {
  //       delete reply;
  //     }
  //     iter.next();
  //   }
  // }
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
  int shardNum = proxyConfig_.proxyShardNum;

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

  // std::string key = "LogTd";
  // threadPool_[key] = new std::thread(&Proxy::LogTd, this);
}

void Proxy::CalculateLatencyBoundTd() {
  std::pair<uint32_t, uint32_t> owdSample;
  std::vector<uint32_t> replicaOWDs;
  replicaOWDs.resize(proxyConfig_.replicaIps.size(),
                     proxyConfig_.replicaInitialOwd);
  for (uint32_t i = 0; i < replicaOWDs.size(); i++) {
    LOG(INFO) << "replicaOWD " << i << "\t" << replicaOWDs[i];
  }
  while (running_) {
    while (owdQu_.try_dequeue(owdSample)) {
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
    usleep(5000);
  }
}

void Proxy::LogTd() {
  Log litem;
  std::ofstream ofs("Proxy-Stats-" + std::to_string(proxyConfig_.proxyId) +
                    ".csv");
  ofs << "ReplicaId,ClientId,RequestId,ClientTime,ProxyTime,"
         "ProxyEndProcessTime,RecvTime,Deadline,"
         "FastReplyTime,"
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
  // ConcurrentMap<uint64_t, Reply*>& committedReply = committedReplyMap_[id];
  std::unordered_map<uint64_t, Reply*>& committedReply = committedReplyMap_[id];
  ConcurrentMap<uint64_t, Log*>& logs = logMap_[id];
  std::map<uint64_t, std::map<uint32_t, Reply>> replyQuorum;
  std::map<uint32_t, Reply*> uncommittedReply;  // Key: logId, value: reqKey
  uint32_t currentView = 0;
  int sz = 0;
  char buffer[UDP_BUFFER_SIZE];
  MessageHeader* msgHdr = (MessageHeader*)(void*)buffer;
  struct sockaddr_in recvAddr;
  socklen_t sockLen = sizeof(recvAddr);
  Reply reply;
  Reply* committedAck = NULL;
  uint32_t replyNum = 0;
  uint64_t startTime, endTime;
  std::vector<uint64_t>& replicaSyncedPoint = replicaSyncedPoints_[id];
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

        uint64_t syncPoint =
            CONCAT_UINT32(reply.view(), reply.maxsyncedlogid());

        if (replicaSyncedPoint[reply.replicaid()] < syncPoint) {
          replicaSyncedPoint[reply.replicaid()] = syncPoint;
        }

        if (reply.clientid() == 0 && reply.reqid() == 0) {
          // Dummy reply, just used to update
          continue;
        }

        // committedAck = committedReply.get(reqKey);
        // if (committedAck != NULL) {
        //   // already committed;  ignore
        //   continue;
        // }
        auto iter = committedReply.find(reqKey);
        if (iter != committedReply.end()) {
          // already committed; ignore
          continue;
        }

        if (reply.view() < currentView) {
          LOG(INFO) << "Replied from old view";
          continue;
        }

        if (currentView < reply.view()) {
          // Replicas have upgraded to a new view
          // Reset current state
          currentView = reply.view();
          uncommittedReply.clear();
          replyQuorum.clear();
          for (int i = 0; i < replicaNum_; i++) {
            replicaSyncedPoint[i] = replicaSyncedPoint[reply.replicaid()];
          }
          currentView = reply.view();
        }

        // LOG(INFO) << reply.DebugString();
        if (reply.replytype() == (uint32_t)MessageType::COMMIT_REPLY) {
          committedAck = new Reply(reply);
          // committedReply.assign(reqKey, committedAck);
        } else if (replyQuorum[reqKey].find(reply.replicaid()) ==
                   replyQuorum[reqKey].end()) {
          replyQuorum[reqKey][reply.replicaid()] = reply;
          committedAck = isQuorumReady(replicaSyncedPoint, replyQuorum[reqKey]);
        } else if (reply.view() > replyQuorum[reqKey].begin()->second.view()) {
          // New view has come, clear existing replies for this request
          uncommittedReply.clear();
          replyQuorum[reqKey].clear();
          replyQuorum[reqKey][reply.replicaid()] = reply;
          for (int i = 0; i < replicaNum_; i++) {
            replicaSyncedPoint[i] = replicaSyncedPoint[reply.replicaid()];
          }
          committedAck = isQuorumReady(replicaSyncedPoint, replyQuorum[reqKey]);
        } else if (reply.view() == replyQuorum[reqKey].begin()->second.view()) {
          const Reply& existedReply = replyQuorum[reqKey][reply.replicaid()];
          if (existedReply.view() < reply.view()) {
            replyQuorum[reqKey][reply.replicaid()] = reply;
          } else if (existedReply.view() == reply.view() &&
                     existedReply.replytype() < reply.replytype()) {
            // FAST_REPLY < SLOW_REPLY < COMMIT_REPLY
            replyQuorum[reqKey][reply.replicaid()] = reply;
          }
          committedAck = isQuorumReady(replicaSyncedPoint, replyQuorum[reqKey]);
        }  // else: reply.view()< replyQuorum[reqKey].begin()->second.view(),
           // ignore it

        if (committedAck != NULL && committedAck->replytype() > 0) {
          // Ack to client
          struct sockaddr_in* clientAddr =
              clientAddrs_.get(committedAck->clientid());
          std::string replyMsg = committedAck->SerializeAsString();
          msgHdr->msgType = MessageType::COMMIT_REPLY;
          msgHdr->msgLen = replyMsg.length();
          memcpy(buffer + sizeof(MessageHeader), replyMsg.c_str(),
                 replyMsg.length());
          sendto(replyFds_[id], buffer,
                 replyMsg.length() + sizeof(MessageHeader), 0,
                 (struct sockaddr*)clientAddr, sizeof(sockaddr));

          // Add to cache
          // committedReply.assign(reqKey, committedAck);
          committedReply[reqKey] = committedAck;
          replyQuorum.erase(reqKey);

          // Disable Log
          // Log* litem = logs.get(reqKey);
          // if (litem) {
          //   litem->proxyRecvTime_ = GetMicrosecondTimestamp();
          //   litem->commitType_ = committedAck->replytype();
          //   logQu_.enqueue(*litem);
          // }

          // Check whether some uncommittedReply can be committed
          while ((!uncommittedReply.empty()) &&
                 uncommittedReply.begin()->first <= committedAck->logid()) {
            Reply* ack = uncommittedReply.begin()->second;
            ack->set_replytype(MessageType::COMMIT_REPLY);
            if (uncommittedReply.begin()->first < committedAck->logid()) {
              const Reply* ack = uncommittedReply.begin()->second;
              struct sockaddr_in* clientAddr =
                  clientAddrs_.get(ack->clientid());
              std::string replyMsg = ack->SerializeAsString();
              msgHdr->msgType = MessageType::COMMIT_REPLY;
              msgHdr->msgLen = replyMsg.length();
              memcpy(buffer + sizeof(MessageHeader), replyMsg.c_str(),
                     replyMsg.length());
              sendto(replyFds_[id], buffer,
                     replyMsg.length() + sizeof(MessageHeader), 0,
                     (struct sockaddr*)clientAddr, sizeof(sockaddr));
            }
            uint64_t reqKey = CONCAT_UINT32(ack->clientid(), ack->reqid());
            // committedReply.assign(reqKey, ack);
            committedReply[reqKey] = ack;
            replyQuorum.erase(reqKey);
            uncommittedReply.erase(uncommittedReply.begin());
            delete ack;
          }

          // LOG(INFO) << "reqId=" << committedAck->reqid()
          //           << "\t type=" << committedAck->replytype();
          // replyNum++;
          // if (replyNum == 1) {
          //   startTime = GetMicrosecondTimestamp();
          // } else if (replyNum % 100000 == 0) {
          //   endTime = GetMicrosecondTimestamp();
          //   float rate = 100000 / ((endTime - startTime) * 1e-6);
          //   LOG(INFO) << "id=" << id << "\t"
          //             << "replyNum=" << replyNum << "\t"
          //             << "rate = " << rate << "\t"
          //             << "uncommittedLen = " << uncommittedReply.size();
          //   startTime = endTime;
          // }
        } else if (committedAck != NULL && committedAck->replytype() == 0) {
          // record in uncommittedRequests
          if (committedAck->replicaid() == currentView % replicaNum_) {
            // This is a leader's reply, cache it
            if (uncommittedReply.find(committedAck->logid()) ==
                uncommittedReply.end()) {
              uncommittedReply[committedAck->logid()] = committedAck;
            }

          } else {
            delete committedAck;
          }
        }
      }
    }
  }
}

Reply* Proxy::isQuorumReady(std::vector<uint64_t>& replicaSyncedPoint,
                            std::map<uint32_t, Reply>& quorum) {
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
    bool fastSatisfied = (kv.second.replytype() == MessageType::FAST_REPLY &&
                          kv.second.view() == leaderReply.view() &&
                          kv.second.hash() == leaderReply.hash());
    bool slowSatisfied =
        (HIGH_32BIT(replicaSyncedPoint[kv.first]) == leaderReply.view() &&
         LOW_32BIT(replicaSyncedPoint[kv.first]) >= leaderReply.logid());

    // if (kv.second.replytype() == MessageType::FAST_REPLY &&
    //     kv.second.hash() != leaderReply.hash()) {
    //   LOG(INFO) << kv.second.DebugString()
    //             << "\t\t\nLeader: " << leaderReply.DebugString();
    // }
    if (fastSatisfied || slowSatisfied) {
      fastOrSlowReplyNum++;
    }
    if (slowSatisfied) {
      slowReplyNum++;
    }
    // if( (!fastSatisfied) && (!slowSatisfied) && quorum.size()==3) {
    //   LOG(INFO) <<"Wrong "<<kv.second.DebugString();
    //   LOG(INFO) <<"leader "<<leaderReply.DebugString();
    //   LOG(INFO)
    //   <<"kv.first="<<kv.first<<"\t"<<replicaSyncedPoint[kv.first]<<"\t"
    //   <<(HIGH_32BIT(replicaSyncedPoint[kv.first]) ==
    //   leaderReply.view())<<"\t"
    //   <<(LOW_32BIT(replicaSyncedPoint[kv.first]) >= leaderReply.logid()) ;
    // }
  }

  Reply* committedReply = new Reply(leaderReply);
  if (fastOrSlowReplyNum >= (uint32_t)fastQuorum_) {
    // Fast Commit
    committedReply->set_replytype(MessageType::FAST_REPLY);
  } else if (slowReplyNum >= (uint32_t)f_ + 1) {
    // Slow Commit: Together with the leader reply, it forms the simple quorum
    // of f+1
    committedReply->set_replytype(MessageType::SLOW_REPLY);
  } else {
    // Uncommitted
    // if(quorum.size()==3) {

    //   LOG(INFO) <<"fastOrSlowReplyNum="<<fastOrSlowReplyNum;
    //   LOG(INFO) <<quorum[0].DebugString();
    //   LOG(INFO) <<quorum[1].DebugString();
    //   LOG(INFO) <<quorum[2].DebugString();
    // }
    committedReply->set_replytype(0);
  }
  return committedReply;
}

void Proxy::ForwardRequestsTd(const int id) {
  // ConcurrentMap<uint64_t, Reply*>& committedReply = committedReplyMap_[id];
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
        // Log* litem = new Log();
        // litem->clientId_ = request.clientid();
        // litem->reqId_ = request.reqid();
        // litem->clientTime_ = request.clienttime();
        // litem->proxyTime_ = request.sendtime();
        // litem->deadline_ = request.sendtime() + request.bound();
        // logs.assign(reqKey, litem);
        // litem->proxyEndProcessTime_ = GetMicrosecondTimestamp();
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
  int shardNum = proxyConfig_.proxyShardNum;
  uint32_t proxyId = proxyConfig_.proxyId;
  forwardFds_.resize(shardNum, -1);
  requestReceiveFds_.resize(shardNum, -1);
  replyFds_.resize(shardNum, -1);
  proxyIds_.resize(shardNum, proxyId);
  latencyBound_ = proxyConfig_.replicaInitialOwd;
  maxOWD_ = proxyConfig_.proxyMaxOwd;
  for (int i = 0; i < shardNum; i++) {
    forwardFds_[i] = CreateSocketFd(proxyConfig_.proxyIp,
                                    proxyConfig_.proxyReplyPortBase + i);
    requestReceiveFds_[i] = CreateSocketFd(
        proxyConfig_.proxyIp, proxyConfig_.proxyRequestPortBase + i);
    replyFds_[i] = CreateSocketFd("", -1);
    proxyIds_[i] = ((proxyIds_[i] << 32) | (uint32_t)i);
  }
  committedReplyMap_.resize(shardNum);
  logMap_.resize(shardNum);

  replicaNum_ = proxyConfig_.replicaIps.size();
  assert(replicaNum_ % 2 == 1);
  f_ = replicaNum_ / 2;

  replicaSyncedPoints_.resize(shardNum);
  for (int i = 0; i < shardNum; i++) {
    replicaSyncedPoints_[i].assign(replicaNum_, CONCURRENT_MAP_START_INDEX);
  }

  fastQuorum_ = (f_ % 2 == 1) ? (f_ + (f_ + 1) / 2 + 1) : (f_ + f_ / 2 + 1);
  replicaAddrs_.resize(replicaNum_);
  for (int i = 0; i < replicaNum_; i++) {
    std::string replicaIP = proxyConfig_.replicaIps[i];
    for (int j = 0; j < proxyConfig_.replicaReceiverShards; j++) {
      struct sockaddr_in* addr = new sockaddr_in();
      bzero(addr, sizeof(struct sockaddr_in));
      addr->sin_family = AF_INET;
      addr->sin_port = htons(proxyConfig_.replicaReceiverPort + j);
      addr->sin_addr.s_addr = inet_addr(replicaIP.c_str());
      replicaAddrs_[i].push_back(addr);
    }
  }
}

}  // namespace nezha
