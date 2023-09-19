#include "replica/replica.h"

namespace nezha {
// #define GJK_DEBUG
#ifdef GJK_DEBUG
#define ASSERT(x) assert(x)
#else
#define ASSERT(x) \
  {}
#endif

Replica::Replica(const std::string& configFile, bool isRecovering)
    : viewId_(0), lastNormalView_(0) {
  repliedSyncPoint_ = new std::atomic<uint32_t>[maxProxyNum_];
  for (uint32_t i = 0; i < maxProxyNum_; i++) {
    repliedSyncPoint_[i] = CONCURRENT_MAP_START_INDEX - 1;
  }
  LOG(INFO) << maxProxyNum_ << " proxy replied sync point has been initialized";

  lastAskMissedIndexTime_ = 0;
  lastAskMissedRequestTime_ = 0;
  syncedLogEntryHead_ = new LogEntry();
  syncedLogEntryHead_->logId = CONCURRENT_MAP_START_INDEX - 1;
  syncedLogEntryHead_->body.deadline = 0;
  syncedLogEntryHead_->body.reqKey = 0;
  unSyncedLogEntryHead_ = new LogEntry();
  unSyncedLogEntryHead_->logId = CONCURRENT_MAP_START_INDEX - 1;
  unSyncedLogEntryHead_->body.deadline = 0;
  unSyncedLogEntryHead_->body.reqKey = 0;

  // Load Config
  std::string error = replicaConfig_.parseConfig(configFile);
  if (error != "") {
    LOG(ERROR) << "Error loading replica config. " << error << " Exiting";
    exit(1);
  }
  if (isRecovering) {
    status_ = ReplicaStatus::RECOVERING;
    LOG(INFO) << "Recovering ...";
  } else {
    status_ = ReplicaStatus::NORMAL;
  }
  LOG(INFO) << "Replica Status " << status_;
  CreateContext();
  LOG(INFO) << "viewId_=" << viewId_ << "\treplicaId=" << replicaId_
            << "\treplicaNum=" << replicaNum_ << "\tkeyNum=" << keyNum_;
}

Replica::~Replica() {
  status_ = ReplicaStatus::TERMINATED;
  for (auto& kv : threadPool_) {
    delete kv.second;
    VLOG(2) << "Deleted\t" << kv.first;
  }

  // TODO: A more elegant way is to reclaim or dump all logs before exit
  // For now, it is fine because all the memory is freed after the process is
  // terminated
}

void Replica::Run() {
  // Master thread run
  masterContext_->Register(endPointType_);
  if (status_ == ReplicaStatus::RECOVERING) {
    masterContext_->endPoint_->RegisterTimer(crashVectorRequestTimer_);
  } else if (status_ == ReplicaStatus::NORMAL) {
    if (!AmLeader()) {
      masterContext_->endPoint_->RegisterTimer(heartbeatCheckTimer_);
    }
    masterContext_->endPoint_->RegisterTimer(periodicSyncTimer_);
  }
  // Launch worker threads (based on config)
  LaunchThreads();

  masterContext_->endPoint_->LoopRun();
  VLOG(2) << "Break LoopRun";

  // Wait until all threads return
  for (auto& kv : threadPool_) {
    VLOG(2) << "Joining " << kv.first;
    kv.second->join();
    VLOG(2) << "Join Complete \t" << kv.first;
  }
}

void Replica::Terminate() {
  do {
    status_ = ReplicaStatus::TERMINATED;
    waitVar_.notify_all();
    // LOG(INFO) << "activeWorkerNum_=" << activeWorkerNum_;
  } while (activeWorkerNum_ > 0);
}

void Replica::CreateContext() {
  endPointType_ = replicaConfig_.endpointType;
  replicaId_ = replicaConfig_.replicaId;
  replicaNum_ = replicaConfig_.replicaIps.size();
  keyNum_ = replicaConfig_.keyNum;
  lastReleasedEntryByKeys_.assign(keyNum_, {0ul, 0ul});
  // Since ConcurrentMap reserves 0 and 1, log-id starts from from 2
  // So these variables are initialized as 2-1=1

  maxSyncedLogEntry_ = syncedLogEntryHead_;
  maxUnSyncedLogEntry_ = unSyncedLogEntryHead_;
  minUnSyncedLogEntry_ = unSyncedLogEntryHead_;

  maxSyncedLogEntryByKey_.assign(keyNum_, NULL);
  maxUnSyncedLogEntryByKey_.assign(keyNum_, NULL);
  minUnSyncedLogEntryByKey_.assign(keyNum_, NULL);

  committedLogId_ = CONCURRENT_MAP_START_INDEX - 1;
  toCommitLogId_ = CONCURRENT_MAP_START_INDEX - 1;

  // Create master endpoints and context
  std::string ip = replicaConfig_.replicaIps[replicaId_.load()];
  int port = replicaConfig_.masterPort;
  int monitorPeriodMs = replicaConfig_.monitorPeriodMs;
  Endpoint* masterEP = CreateEndpoint(endPointType_, ip, port, true);
  auto masterCallBack = [](MessageHeader* msgHeader, char* msgBuffer,
                           Address* sender, void* ctx) {
    ((Replica*)ctx)->ReceiveMasterMessage(msgHeader, msgBuffer);
  };
  // Register a timer to monitor replica status
  Timer* masterMonitorTimer = new Timer(
      [](void* ctx, void* receiverEP) {
        if (((Replica*)ctx)->status_ == ReplicaStatus::TERMINATED) {
          // Master thread will only break its loop when status comes to
          // TERMINATED
          ((Endpoint*)receiverEP)->LoopBreak();
        }
      },
      monitorPeriodMs, this);
  masterContext_ =
      new ReceiverContext(masterEP, this, masterCallBack, masterMonitorTimer);

  LOG(INFO) << "Master Created";
  // Create request-receiver endpoints and context
  requestContext_.resize(replicaConfig_.receiverShards);
  for (int i = 0; i < replicaConfig_.receiverShards; i++) {
    int port = replicaConfig_.receiverPort + i;
    Endpoint* requestEP = CreateEndpoint(endPointType_, ip, port);
    // Register a request handler to this endpoint
    auto requestHandlerFunc = [](MessageHeader* msgHeader, char* msgBuffer,
                                 Address* sender, void* ctx) {
      ((Replica*)ctx)->ReceiveClientRequest(msgHeader, msgBuffer, sender);
    };
    // Register a timer to monitor replica status
    Timer* requestEPMonitorTimer = new Timer(
        [](void* ctx, void* receiverEP) {
          if (((Replica*)ctx)->status_ != ReplicaStatus::NORMAL) {
            ((Endpoint*)receiverEP)->LoopBreak();
          }
        },
        monitorPeriodMs, this);
    requestContext_[i] = new ReceiverContext(
        requestEP, this, requestHandlerFunc, requestEPMonitorTimer);
  }

  LOG(INFO) << "requestContext_ Created";
  // (Leader) Use these endpoints to broadcast indices to followers
  for (int i = 0; i < replicaConfig_.indexSyncShards; i++) {
    indexSender_.push_back(new UDPSocketEndpoint());
  }
  indexAcker_ = CreateEndpoint(endPointType_);
  indexRequester_ = CreateEndpoint(endPointType_);
  reqRequester_ = CreateEndpoint(endPointType_);
  for (uint32_t i = 0; i < replicaNum_; i++) {
    std::string ip = replicaConfig_.replicaIps[i];
    int indexPort = replicaConfig_.indexSyncPort;
    indexReceiver_.push_back(new Address(ip, indexPort));
    int indexAskPort = replicaConfig_.indexAskPort;
    indexAskReceiver_.push_back(new Address(ip, indexAskPort));
    int requestAskPort = replicaConfig_.requestAskPort;
    requestAskReceiver_.push_back(new Address(ip, requestAskPort));
    int masterPort = replicaConfig_.masterPort;
    masterReceiver_.push_back(new Address(ip, masterPort));
  }
  // (Followers:) Create index-sync endpoint to receive indices
  port = replicaConfig_.indexSyncPort;
  Endpoint* idxSyncEP = CreateEndpoint(endPointType_, ip, port);
  // Register a msg handler to this endpoint to handle index sync messages
  auto idxHandleFunc = [](MessageHeader* msgHeader, char* msgBuffer,
                          Address* sender, void* ctx) {
    ((Replica*)ctx)->ReceiveIndexSyncMessage(msgHeader, msgBuffer);
  };

  // Register a timer to monitor replica status
  Timer* idxSyncMonitorTimer = new Timer(
      [](void* ctx, void* receiverEP) {
        if (((Replica*)ctx)->status_ != ReplicaStatus::NORMAL) {
          ((Endpoint*)receiverEP)->LoopBreak();
        }
      },
      monitorPeriodMs, this);

  indexSyncContext_ =
      new ReceiverContext(idxSyncEP, this, idxHandleFunc, idxSyncMonitorTimer);

  LOG(INFO) << "indexSyncContext_ Created";

  // Create an endpoint to handle others' requests for missed index
  port = replicaConfig_.indexAskPort;
  Endpoint* missedIdxEP = CreateEndpoint(endPointType_, ip, port);
  // Register message handler
  auto missedIdxHandleFunc = [](MessageHeader* msgHeader, char* msgBuffer,
                                Address* sender, void* ctx) {
    ((Replica*)ctx)->ReceiveAskMissedIdx(msgHeader, msgBuffer);
  };

  // Register a timer to monitor replica status
  Timer* missedIdxAckMonitorTimer = new Timer(
      [](void* ctx, void* receiverEP) {
        if (((Replica*)ctx)->status_ != ReplicaStatus::NORMAL) {
          ((Endpoint*)receiverEP)->LoopBreak();
        }
      },
      monitorPeriodMs, this);

  missedIndexAckContext_ = new ReceiverContext(
      missedIdxEP, this, missedIdxHandleFunc, missedIdxAckMonitorTimer);

  LOG(INFO) << "missedIndexAckContext_ Created";

  // Create an endpoint to handle others' requests for missed req
  port = replicaConfig_.requestAskPort;
  Endpoint* missedReqAckEP = CreateEndpoint(endPointType_, ip, port);
  // Register message handler
  auto missedReqAckHandleFunc = [](MessageHeader* msgHeader, char* msgBuffer,
                                   Address* sender, void* ctx) {
    ((Replica*)ctx)->ReceiveAskMissedReq(msgHeader, msgBuffer);
  };
  // Register a timer to monitor replica status
  Timer* missedReqAckMonitorTimer = new Timer(
      [](void* ctx, void* receiverEP) {
        if (((Replica*)ctx)->status_ != ReplicaStatus::NORMAL) {
          ((Endpoint*)receiverEP)->LoopBreak();
        }
      },
      monitorPeriodMs, this);

  missedReqAckContext_ = new ReceiverContext(
      missedReqAckEP, this, missedReqAckHandleFunc, missedReqAckMonitorTimer);

  LOG(INFO) << "missedReqAckContext_ Created";

  // Create Record Qus and Maps
  recordMap_.resize(replicaConfig_.recordShards);
  recordQu_.resize(replicaConfig_.recordShards);

  // Create track entry for trackThread
  trackedEntry_.assign(replicaConfig_.trackShards, maxSyncedLogEntry_);

  // Create reply endpoints
  int replyShardNum = replicaConfig_.replyShards;
  for (int i = 0; i < replyShardNum; i++) {
    fastReplySender_.push_back(CreateEndpoint(endPointType_));
    slowReplySender_.push_back(CreateEndpoint(endPointType_));
  }
  // Create reply queues (one queue per fast/slow reply thread)
  fastReplyQu_.resize(replyShardNum);
  slowReplyQu_.resize(replyShardNum);

  // Create CrashVector Context
  std::vector<uint32_t> cvVec(replicaNum_, 0);
  CrashVectorStruct* cv = new CrashVectorStruct(cvVec, 2);
  crashVector_.assign(cv->version_, cv);
  /** Thw related threads using crash vectors are:
   * (1) master (1 thread)
   * (2) FastReplyThread(s) (replyShardNum threads) */
  crashVectorVecSize_ = 1 + replyShardNum;
  crashVectorInUse_ = new std::atomic<CrashVectorStruct*>[crashVectorVecSize_];
  for (uint32_t i = 0; i < crashVectorVecSize_; i++) {
    crashVectorInUse_[i] = cv;
  }

  // Create other useful timers
  heartbeatCheckTimer_ = new Timer(
      [](void* ctx, void* receiverEP) {
        // Followers use this timer to check leader's heartbeat
        ((Replica*)ctx)->CheckHeartBeat();
      },
      monitorPeriodMs, this);

  indexAskTimer_ = new Timer(
      [](void* ctx, void* receiverEP) { ((Replica*)ctx)->AskMissedIndex(); },
      replicaConfig_.indexAskPeriodMs, this);
  roundRobinIndexAskIdx_ = 0;
  // Initially, no missed indices, so we make first > second
  missedIndices_ = {1, 0};

  requestAskTimer_ = new Timer(
      [](void* ctx, void* receiverEP) { ((Replica*)ctx)->AskMissedRequest(); },
      replicaConfig_.requestAskPeriodMs, this);
  roundRobinRequestAskIdx_ = 0;
  missedReqKeys_.clear();

  viewChangeTimer_ = new Timer(
      [](void* ctx, void* receiverEP) {
        ((Replica*)ctx)->BroadcastViewChange();
      },
      replicaConfig_.viewChangePeriodMs, this);
  roundRobinProcessIdx_ = 0;

  periodicSyncTimer_ = new Timer(
      [](void* ctx, void* receiverEP) {
        ((Replica*)ctx)->SendSyncStatusReport();
      },
      replicaConfig_.syncReportPeriodMs, this);

  requestTrasnferBatch_ = replicaConfig_.requestTransferBatch;
  indexTransferBatch_ = replicaConfig_.indexTransferBatch;
  requestKeyTransferBatch_ = replicaConfig_.requestKeyTransferBatch;

  stateTransferTimer_ = new Timer(
      [](void* ctx, void* receiverEP) {
        ((Replica*)ctx)->SendStateTransferRequest();
      },
      replicaConfig_.stateTransferPeriodMs, this);

  stateTransferTimeout_ = replicaConfig_.stateTransferTimeoutMs;

  crashVectorRequestTimer_ = new Timer(
      [](void* ctx, void* receiverEP) {
        ((Replica*)ctx)->BroadcastCrashVectorRequest();
      },
      replicaConfig_.crashVectorRequestPeriodMs, this);

  recoveryRequestTimer_ = new Timer(
      [](void* ctx, void* receiverEP) {
        ((Replica*)ctx)->BroadcastRecoveryRequest();
      },
      replicaConfig_.recoveryRequestPeriodMs, this);

  movingPercentile_ = replicaConfig_.movingPercentile;
  slidingWindowLen_ = replicaConfig_.owdEstimationWindow;

  // Signal variable for garbage collection (of followers)
  reclaimTimeout_ = replicaConfig_.reclaimTimeoutMs;
  safeToClearUnSyncedLogId_ = new std::atomic<uint32_t>[replyShardNum + 1];
  safeToClearLateBufferLogId_ = CONCURRENT_MAP_START_INDEX - 1;
  for (int i = 0; i <= replyShardNum; i++) {
    safeToClearUnSyncedLogId_[i] = CONCURRENT_MAP_START_INDEX - 1;
  }
  prepareToClearLateBufferLogId_ = CONCURRENT_MAP_START_INDEX - 1;
  prepareToClearUnSyncedLogId_ = CONCURRENT_MAP_START_INDEX - 1;
}

void Replica::ResetContext() {
  // Clear queues
  for (uint32_t i = 0; i < fastReplyQu_.size(); i++) {
    LogEntry* entry;
    while (fastReplyQu_[i].try_dequeue(entry)) {
    }
    while (slowReplyQu_[i].try_dequeue(entry)) {
    }
    // Don't worry about memory leakage, the memory pointed by these in-queue
    // pointers have already been cleaned or will be cleaned according to their
    // Conucurrent maps
  }
  LogEntry* entry;
  while (processQu_.try_dequeue(entry)) {
    delete entry;
  }
  for (uint32_t i = 0; i < recordQu_.size(); i++) {
    RequestBody* rb;
    while (recordQu_[i].try_dequeue(rb)) {
      delete rb;
    }
  }

  // TODO: Clear LateBuffer

  // Clear Early Buffer
  while (earlyBuffer_.empty() == false) {
    LogEntry* entry = earlyBuffer_.begin()->second;
    delete entry;
    earlyBuffer_.erase(earlyBuffer_.begin());
  }

  // Reset lastReleasedEntryByKeys_, no need to care about UnSyncedLogs, because
  // they are all cleared
  for (uint32_t key = 0; key < keyNum_; key++) {
    if (maxSyncedLogEntryByKey_[key]) {
      lastReleasedEntryByKeys_[key] = {
          maxSyncedLogEntryByKey_[key]->body.deadline,
          maxSyncedLogEntryByKey_[key]->body.reqKey};

    } else {
      lastReleasedEntryByKeys_[key] = {0ul, 0ul};
    }
  }

  // Clear UnSyncedLogs
  minUnSyncedLogEntry_ = unSyncedLogEntryHead_;
  maxUnSyncedLogEntry_ = unSyncedLogEntryHead_;
  minUnSyncedLogEntryByKey_.clear();
  maxUnSyncedLogEntryByKey_.clear();
  minUnSyncedLogEntryByKey_.assign(keyNum_, NULL);
  maxUnSyncedLogEntryByKey_.assign(keyNum_, NULL);

  // Reset Index-Sync related stuff
  roundRobinIndexAskIdx_ = 0;
  missedIndices_ = {1, 0};
  roundRobinRequestAskIdx_ = 0;
  missedReqKeys_.clear();
  roundRobinProcessIdx_ = 0;
  pendingIndexSync_.clear();

  // Reset stateTransfer related stuff
  stateTransferIndices_.clear();
  viewChangeSet_.clear();
  crashVectorReplySet_.clear();
  recoveryReplySet_.clear();
  syncStatusSet_.clear();

  // Reset trackedEntry
  trackedEntry_.assign(trackedEntry_.size(), maxSyncedLogEntry_);

  // Reset OWD-Calc Related stuff
  slidingWindow_.clear();
  owdSampleNum_.clear();

  // Reset Master's timers
  // No need to worry about other timers: worker thread will unregister their
  // timers and msg handlers during LoopBreak
  masterContext_->endPoint_->UnRegisterAllTimers();
  masterContext_->endPoint_->RegisterTimer(masterContext_->monitorTimer_);
  if (!AmLeader()) {
    // Start checking leader's heartbeat from now on
    lastHeartBeatTime_ = GetMicrosecondTimestamp();
    masterContext_->endPoint_->RegisterTimer(heartbeatCheckTimer_);
  }
  masterContext_->endPoint_->RegisterTimer(periodicSyncTimer_);

  // Reset signal variable for garbage collection (of followers)
  safeToClearLateBufferLogId_ = CONCURRENT_MAP_START_INDEX - 1;
  for (uint32_t i = 0; i <= fastReplyQu_.size(); i++) {
    // The number of such counters is number of FastReplyThread_ + 1 (IndexRecv)
    safeToClearUnSyncedLogId_[i] = CONCURRENT_MAP_START_INDEX - 1;
  }
  prepareToClearLateBufferLogId_ = CONCURRENT_MAP_START_INDEX - 1;
  prepareToClearUnSyncedLogId_ = CONCURRENT_MAP_START_INDEX - 1;
}

void Replica::LaunchThreads() {
  activeWorkerNum_ = 0;  // Dynamic variable, used as semaphore
  totalWorkerNum_ = 0;   // Static variable to count number of workers
  // RequestReceive
  for (int i = 0; i < replicaConfig_.receiverShards; i++) {
    totalWorkerNum_++;
    std::thread* td = new std::thread(&Replica::ReceiveThread, this, i);
    std::string key("ReceiveThread-" + std::to_string(i));
    threadPool_[key] = td;
    LOG(INFO) << "Launched " << key << "\t" << td->native_handle();
  }

  // RequestRecord
  for (int i = 0; i < replicaConfig_.recordShards; i++) {
    totalWorkerNum_++;
    std::thread* td = new std::thread(&Replica::RecordThread, this, i);
    std::string key("RecordThread-" + std::to_string(i));
    threadPool_[key] = td;
    LOG(INFO) << "Launched " << key << "\t" << td->native_handle();
  }

  // RequestProcess
  if (replicaConfig_.processShards != 1) {
    LOG(ERROR) << "ProcessThread parallelization is not supported. "
                  "replicaConfig_->processShards must be 1.";
    exit(1);
  }
  for (int i = 0; i < replicaConfig_.processShards; i++) {
    totalWorkerNum_++;
    std::thread* td = new std::thread(&Replica::ProcessThread, this, i);
    std::string key("ProcessThread-" + std::to_string(i));
    threadPool_[key] = td;
    LOG(INFO) << "Launched " << key << "\t" << td->native_handle();
  }

  // RequestReply
  int replyShardNum = replicaConfig_.replyShards;
  for (int i = 0; i < replyShardNum; i++) {
    totalWorkerNum_++;
    std::thread* td =
        new std::thread(&Replica::FastReplyThread, this, i, i + 1);
    std::string key("FastReplyThread-" + std::to_string(i));
    threadPool_[key] = td;
    LOG(INFO) << "Launched " << key << "\t" << td->native_handle();
  }

  for (int i = 0; i < replyShardNum; i++) {
    totalWorkerNum_++;
    std::thread* td = new std::thread(&Replica::SlowReplyThread, this, i);
    std::string key("SlowReplyThread-" + std::to_string(i));
    threadPool_[key] = td;
    LOG(INFO) << "Launched " << key << "\t" << td->native_handle();
  }

  // Track
  for (int i = 0; i < replicaConfig_.trackShards; i++) {
    totalWorkerNum_++;
    std::thread* td = new std::thread(&Replica::TrackThread, this, i);
    std::string key("TrackThread-" + std::to_string(i));
    threadPool_[key] = td;
    LOG(INFO) << "Launched " << key << "\t" << td->native_handle();
  }

  // IndexSync
  for (int i = 0; i < replicaConfig_.indexSyncShards; i++) {
    totalWorkerNum_++;
    std::thread* td = new std::thread(&Replica::IndexSendThread, this, i,
                                      i + replyShardNum + 1);
    std::string key("IndexSendThread-" + std::to_string(i));
    threadPool_[key] = td;
    LOG(INFO) << "Launched " << key << "\t" << td->native_handle();
    if (!AmLeader()) {
      // follower only needs one sync thread
      break;
    }
  }

  totalWorkerNum_++;
  threadPool_["IndexRecvThread"] =
      new std::thread(&Replica::IndexRecvThread, this);
  LOG(INFO) << "Launched IndexRecvThread\t"
            << threadPool_["IndexRecvThread"]->native_handle();

  totalWorkerNum_++;
  threadPool_["IndexProcessThread"] =
      new std::thread(&Replica::IndexProcessThread, this);
  LOG(INFO) << "Launched IndexProcessThread\t"
            << threadPool_["IndexProcessThread"]->native_handle();

  totalWorkerNum_++;
  threadPool_["MissedIndexAckThread"] =
      new std::thread(&Replica::MissedIndexAckThread, this);
  LOG(INFO) << "Launched MissedIndexAckThread\t"
            << threadPool_["MissedIndexAckThread"]->native_handle();

  totalWorkerNum_++;
  threadPool_["MissedReqAckThread"] =
      new std::thread(&Replica::MissedReqAckThread, this);
  LOG(INFO) << "Launched MissedReqAckThread\t"
            << threadPool_["MissedReqAckThread"]->native_handle();

  // totalWorkerNum_++;
  // threadPool_["GarbageCollectThread"] =
  //     new std::thread(&Replica::GarbageCollectThread, this);
  // LOG(INFO) << "Launch  GarbageCollectThread "
  //           << threadPool_["GarbageCollectThread"]->native_handle();

  totalWorkerNum_++;
  threadPool_["OWDCalcThread"] = new std::thread(&Replica::OWDCalcThread, this);
  LOG(INFO) << "Launch  OWDCalcThread "
            << threadPool_["OWDCalcThread"]->native_handle();

  // totalWorkerNum_++;
  // threadPool_["LogHash"] = new std::thread(&Replica::LogHash, this);
  // LOG(INFO) << "Launched IndexRecvThread\t"
  //           << threadPool_["LogHash"]->native_handle();

  LOG(INFO) << "Master Thread " << pthread_self();

  LOG(INFO) << "totalWorkerNum_=" << totalWorkerNum_;
}

void Replica::ReceiveClientRequest(MessageHeader* msgHdr, char* msgBuffer,
                                   Address* sender) {
  if (msgHdr->msgType == MessageType::CLIENT_REQUEST) {
    Request request;
    if (request.ParseFromArray(msgBuffer, msgHdr->msgLen)) {
      // tagQu_.enqueue(request.tagid());
      // Collect OWD sample
      uint64_t recvTime = GetMicrosecondTimestamp();
      if (recvTime > request.sendtime()) {
        owdQu_.enqueue(std::pair<uint64_t, uint32_t>(
            request.proxyid(), GetMicrosecondTimestamp() - request.sendtime()));
      }

      if (proxyAddressMap_.get(request.proxyid()) == 0) {
        Address* addr = new Address(*sender);
        /**  When one proxy sends the request, it needs to specify a proper
         **unique* proxyid related to one specific receiver thread on the
         *replica, so that this replica's different receiver threads will not
         *insert the same entry concurrently (otherwise, it may cause memory
         *leakage)
         *
         * In our proxy Implemention, each proxy machine has a unique id,
         with multiple shard. The machine-id concats shard-id becomes a unqiue
         *proxy-id, modulo replica-shard-num and then send to the replica
         *receiver
         **/
        proxyAddressMap_.assign(request.proxyid(), addr);
      }
      uint64_t reqKey = CONCAT_UINT32(request.clientid(), request.reqid());
      uint64_t deadline = request.sendtime() + request.bound();
      RequestBody* rb =
          new RequestBody(deadline, reqKey, request.key(), request.proxyid(),
                          request.command(), request.iswrite());
      uint32_t quId = (reqKey) % recordQu_.size();
      recordQu_[quId].enqueue(rb);

    } else {
      LOG(WARNING) << "Parse request fail";
    }

  } else {
    LOG(WARNING) << "Invalid Message Type " << (uint32_t)(msgHdr->msgType);
  }
}

void Replica::BlockWhenStatusIsNot(char targetStatus) {
  if (status_ != targetStatus) {
    activeWorkerNum_.fetch_sub(1);
    std::unique_lock<std::mutex> lk(waitMutext_);
    waitVar_.wait(lk, [this, targetStatus] {
      if (status_ == ReplicaStatus::TERMINATED || status_ == targetStatus) {
        // Unblock
        activeWorkerNum_.fetch_add(1);
        return true;
      } else {
        return false;
      }
    });
  }
}

void Replica::OWDCalcThread() {
  activeWorkerNum_.fetch_add(1);
  std::pair<uint64_t, uint32_t> owdSample;
  // uint32_t logCnt = 0;
  while (status_ != ReplicaStatus::TERMINATED) {
    BlockWhenStatusIsNot(ReplicaStatus::NORMAL);
    while (owdQu_.try_dequeue(owdSample)) {
      uint64_t proxyId = owdSample.first;
      uint32_t owd = owdSample.second;
      owdSampleNum_[proxyId]++;
      if (slidingWindow_[proxyId].size() < slidingWindowLen_) {
        slidingWindow_[proxyId].push_back(owd);
      } else {
        slidingWindow_[proxyId][owdSampleNum_[proxyId] % slidingWindowLen_] =
            owd;
      }
      if (owdSampleNum_[proxyId] >= slidingWindowLen_) {
        std::vector<uint32_t> tmpSamples(slidingWindow_[proxyId]);
        sort(tmpSamples.begin(), tmpSamples.end());
        uint32_t movingEstimate =
            tmpSamples[slidingWindowLen_ * movingPercentile_];
        owdMap_.assign(proxyId, movingEstimate);
      }
    }
    // reduce CPU cost
    nanosleep((const struct timespec[]){{0, 1000000L}}, NULL);
  }
  uint32_t preVal = activeWorkerNum_.fetch_sub(1);
  LOG(INFO) << "OWDCalcThread Terminated: " << preVal - 1
            << " worker remaining";
}

void Replica::ReceiveThread(int id) {
  activeWorkerNum_.fetch_add(1);
  while (status_ != ReplicaStatus::TERMINATED) {
    BlockWhenStatusIsNot(ReplicaStatus::NORMAL);
    requestContext_[id]->Register(endPointType_);
    requestContext_[id]->endPoint_->LoopRun();
  }
  uint32_t preVal = activeWorkerNum_.fetch_sub(1);
  LOG(INFO) << "ReceiveThread Terminated:" << preVal - 1 << " worker remaining";
}

void Replica::RecordThread(int id) {
  activeWorkerNum_.fetch_add(1);
  RequestBody* rb;
  // uint64_t sta, ed, cnt;
  // cnt = 0;
  while (status_ != ReplicaStatus::TERMINATED) {
    BlockWhenStatusIsNot(ReplicaStatus::NORMAL);
    if (recordQu_[id].try_dequeue(rb)) {
      // cnt++;
      // if (cnt == 1) {
      //   sta = GetMicrosecondTimestamp();
      // }
      // if (cnt % 100000 == 0) {
      //   ed = GetMicrosecondTimestamp();
      //   float rate = 100000.0 / ((ed - sta) * 1e-6);
      //   sta = ed;
      //   LOG(INFO) << "id=" << id << "  record rate = " << rate << "\t"
      //             << "recordQuLen=" << recordQu_[id].size_approx() << "\t"
      //             << "processQuLen=" << processQu_.size_approx() << "\t"
      //             << "gap sample =" << ed - rb->deadline
      //             << " \t deadline=" << rb->deadline;
      // }
      /** The map is sharded by reqKey */
      LogEntry* duplicate = recordMap_[id].get(rb->reqKey);
      if (duplicate == NULL) {
        SHA_HASH dummy;
        LogEntry* newEntry = new LogEntry(*rb, dummy, dummy);
        recordMap_[id].assign(rb->reqKey, newEntry);
        processQu_.enqueue(newEntry);

      } else {
        // Duplicate requests
        processQu_.enqueue(duplicate);
      }
      delete rb;
    }
  }
  uint32_t preVal = activeWorkerNum_.fetch_sub(1);
  LOG(INFO) << "RecordThread-" << id << " Terminated: " << preVal - 1
            << " worker remaining";
}

void Replica::TrackThread(int id) {
  activeWorkerNum_.fetch_add(1);
  while (status_ != ReplicaStatus::TERMINATED) {
    BlockWhenStatusIsNot(ReplicaStatus::NORMAL);
    if (trackedEntry_[id]->next) {
      LogEntry* next = trackedEntry_[id]->next;
      // LOG(INFO) << "next logId = " << next->logId;
      if (next->logId % trackedEntry_.size() == (uint32_t)id) {
        if (trackedEntry_[id]->logId >= CONCURRENT_MAP_START_INDEX) {
          uint32_t a = trackedEntry_[id]->logId;
          uint32_t b = next->logId;
          if (a + trackedEntry_.size() != b) {
            LOG(ERROR) << "myId = " << trackedEntry_[id]->logId << "\t"
                       << "sz = " << trackedEntry_.size() << "\t"
                       << "next=" << next->logId << "\t"
                       << trackedEntry_[id]->logId + trackedEntry_.size()
                       << "\t"
                       << (trackedEntry_[id]->logId + trackedEntry_.size() !=
                           next->logId)
                       << "\t"
                       << "a=" << a << "\t"
                       << "b=" << b;
          }
          ASSERT(trackedEntry_[id]->logId + trackedEntry_.size() ==
                 next->logId);
        }
        syncedLogEntryByLogId_.assign(next->logId, next);
        syncedLogEntryByReqKey_.assign(next->body.reqKey, next);
      }
      trackedEntry_[id] = next;
    }
    if (status_ == ReplicaStatus::TERMINATED) {
      LOG(INFO) << "Track Thread terminate ";
    }
  }
  uint32_t preVal = activeWorkerNum_.fetch_sub(1);
  LOG(INFO) << "TrackThread-" << id << " Terminated: " << preVal - 1
            << " worker remaining";
}

void Replica::ProcessThread(int id) {
  activeWorkerNum_.fetch_add(1);
  LogEntry* entry;
  std::set<uint64_t> tags;
  while (status_ != ReplicaStatus::TERMINATED) {
    BlockWhenStatusIsNot(ReplicaStatus::NORMAL);
    bool amLeader = AmLeader();

    if (processQu_.try_dequeue(entry)) {
      if (entry->status == EntryStatus::INITIAL) {
        std::pair<uint64_t, uint64_t> earlyBufferRank(entry->body.deadline,
                                                      entry->body.reqKey);
        if (earlyBufferRank > lastReleasedEntryByKeys_[entry->body.opKey]) {
          earlyBuffer_[earlyBufferRank] = entry;
          entry->status = EntryStatus::IN_PROCESS;
        } else {
          // LOG(INFO) <<"Abnormal "<<entry->body.opKey
          //         <<"\t<"<<rankKey.first<<","<<rankKey.second<<">\t"
          //          <<"\t<"<<lastReleasedEntryByKeys_[entry->body.opKey].first
          //          <<","<<lastReleasedEntryByKeys_[entry->body.opKey].second
          //          <<">";
          // This entry cannot enter early buffer
          if (amLeader) {
            // Leader modifies its deadline
            entry->body.deadline =
                lastReleasedEntryByKeys_[entry->body.opKey].first + 1;
            earlyBufferRank.first = entry->body.deadline;
            earlyBuffer_[earlyBufferRank] = entry;
            entry->status = EntryStatus::IN_PROCESS;
          } else {
            // Followers leave it in late buffer
            entry->status = EntryStatus::IN_LATEBUFFER;
          }
        }
      } else if (entry->status == EntryStatus::IN_PROCESS ||
                 entry->status == EntryStatus::IN_LATEBUFFER) {
        continue;
      } else if (entry->status == EntryStatus::PROCESSED) {
        uint32_t quId = (entry->body.reqKey) % fastReplyQu_.size();
        fastReplyQu_[quId].enqueue(entry);
      } else if (entry->status == EntryStatus::TO_SLOW_REPLY) {
        uint32_t quId = (entry->body.reqKey) % slowReplyQu_.size();
        slowReplyQu_[quId].enqueue(entry);
      } else {
        LOG(WARNING) << "Unexpected Entry Status " << (int)(entry->status);
      }
    }

    // Polling early-buffer
    uint64_t nowTime = GetMicrosecondTimestamp();

    // This while loop is safe because there is only one processThread.
    // Parallelization of this thread is not supported.
    while (!earlyBuffer_.empty()) {
      LogEntry* nextEntry = earlyBuffer_.begin()->second;
      if (nowTime < nextEntry->body.deadline) {
        break;
      }
      if (nextEntry->body.isWrite) {
        lastReleasedEntryByKeys_[nextEntry->body.opKey] =
            earlyBuffer_.begin()->first;
      }
      ProcessRequest(nextEntry, amLeader, true, amLeader);
      earlyBuffer_.erase(earlyBuffer_.begin());
    }
  }
  uint32_t preVal = activeWorkerNum_.fetch_sub(1);
  LOG(INFO) << "ProcessThread Terminated: " << preVal - 1
            << " worker remaining";
}

void Replica::ProcessRequest(LogEntry* entry, const bool isSyncedReq,
                             const bool sendReply, const bool canExecute) {
  RequestBody& rb = entry->body;
  // Read Request do not contribute to hash
  entry->logHash = entry->entryHash =
      rb.isWrite ? CalculateHash(rb.deadline, rb.reqKey) : SHA_HASH();

  std::vector<LogEntry*>& maxEntryByKey =
      isSyncedReq ? maxSyncedLogEntryByKey_ : maxUnSyncedLogEntryByKey_;
  std::atomic<LogEntry*>& maxEntry =
      isSyncedReq ? maxSyncedLogEntry_ : maxUnSyncedLogEntry_;

  // The log id of the previous non-commutative entry in the synced logs
  entry->prevNonCommutative = maxEntryByKey[rb.opKey];
  if (entry->prevNonCommutative) {
    if (entry->prevNonCommutative->body.isWrite) {
      entry->prevNonCommutativeWrite = entry->prevNonCommutative;
    } else {
      entry->prevNonCommutativeWrite =
          entry->prevNonCommutative->prevNonCommutativeWrite;
    }
  }
  entry->prev = maxEntry;

  entry->result = (isSyncedReq && canExecute) ? ApplicationExecute(rb) : "";

  if (entry->prevNonCommutativeWrite) {
    entry->logHash.XOR(entry->prevNonCommutativeWrite->logHash);
  }
  ASSERT(entry->prev != NULL);
  entry->logId = entry->prev->logId + 1;
  entry->status = EntryStatus::PROCESSED;

  if (entry->prevNonCommutative) {
    entry->prevNonCommutative->nextNonCommutative = entry;
  }
  if (entry->prevNonCommutativeWrite && rb.isWrite) {
    entry->prevNonCommutativeWrite->nextNonCommutativeWrite = entry;
  }
  if (isSyncedReq == false && minUnSyncedLogEntryByKey_[rb.opKey] == NULL) {
    minUnSyncedLogEntryByKey_[rb.opKey] = entry;
  }
  entry->prev->next = entry;

  maxEntryByKey[rb.opKey] = entry;
  maxEntry = entry;

  if (sendReply) {
    uint32_t quId = (entry->body.reqKey) % fastReplyQu_.size();
    fastReplyQu_[quId].enqueue(entry);
  }
}

void Replica::FastReplyThread(int id, int cvId) {
  activeWorkerNum_.fetch_add(1);
  Reply reply;
  reply.set_replytype(MessageType::FAST_REPLY);
  reply.set_replicaid(replicaId_);
  CrashVectorStruct* cv = crashVectorInUse_[cvId];
  uint32_t replyNum = 0;
  // uint64_t startTime, endTime;
  while (status_ != ReplicaStatus::TERMINATED) {
    BlockWhenStatusIsNot(ReplicaStatus::NORMAL);
    bool amLeader = AmLeader();
    safeToClearUnSyncedLogId_[id].store(prepareToClearUnSyncedLogId_.load());
    // Before encoding crashVector into hash, check whether the crashVector
    // (cv) is the freshest one
    CrashVectorStruct* masterCV = crashVectorInUse_[0].load();
    if (cv->version_ < masterCV->version_) {
      // My crash vector is stale, update it
      crashVectorInUse_[cvId] = masterCV;
      cv = masterCV;
    }
    LogEntry* entry = NULL;
    if (fastReplyQu_[id].try_dequeue(entry)) {
      reply.set_iswrite(entry->body.isWrite);
      reply.set_opkey(entry->body.opKey);
      replyNum++;
      // if (replyNum % 500000 == 0) {
      //   LOG(INFO) << id << "QuLen=" << fastReplyQu_[id].size_approx();
      // }
      Address* addr = proxyAddressMap_.get(entry->body.proxyId);
      if (!addr) {
        // The replica cannot find the address to send reply
        // This can happen in very trivial edge cases, e.g.,
        // Step 1: This replica misses the entry
        // Step 2: The other replica gives this replica the missing entry
        // Step 3: This replica has not received any entries from that proxy,
        // so it does not have any addr info Step 4: This replica wants to
        // send reply for this entry
        LOG(ERROR) << "Cannot find the address of the proxy "
                   << HIGH_32BIT(entry->body.proxyId) << "-"
                   << LOW_32BIT(entry->body.proxyId);
        continue;
      }
      reply.set_view(viewId_);
      reply.set_clientid(HIGH_32BIT(entry->body.reqKey));
      reply.set_reqid(LOW_32BIT(entry->body.reqKey));
      reply.set_result(entry->result);
      // If the owdMap_ does not have the proxyId (i.e. the owd for this
      // proxyId has not been estimated), it will return 0 (0 happens to be
      // the dummy value of protobuf, and the proxy will not consider it as an
      // estimated owd)
      reply.set_owd(owdMap_.get(entry->body.proxyId));

      SHA_HASH hash(entry->logHash);
      hash.XOR(cv->cvHash_);
      if (amLeader) {
        // Leader's logic is very easy: after XORing the crashVector and the
        // log entry hash together, it can directly reply
        reply.set_hash(hash.hash, SHA_DIGEST_LENGTH);
        reply.set_logid(entry->logId);
        reply.set_maxsyncedlogid(maxSyncedLogEntry_.load()->logId);

        uint32_t proxyMachineId = HIGH_32BIT(entry->body.proxyId);
        repliedSyncPoint_[proxyMachineId] = reply.maxsyncedlogid();
        fastReplySender_[id]->SendMsgTo(*addr, reply, MessageType::FAST_REPLY);

        // replyLogQu_.enqueue(reply);
        // LOG(INFO) << "Leader reply=" << reply.reqid() << "\t"
        //     << "opKey=" << entry->opKey << "\t"
        //     << "hash=" << hash.toString();
      } else {
        // But follower's hash is a bit complicated, because it needs to
        // consider both synced entries and unsynced entries, i.e. We need to
        // (1) eliminate the part to the left of sync-point and (2) use the
        // remaining part (to the right of sync-point) to XOR the part that
        // has already been synced

        // Let's first get the boundary, i.e. minUnSyncedLogId_ and
        // maxSyncedLogId_ maxSynced is always updated earlier than
        // minUnSynced, so we first get minUnSynced, and then get maxSynced,
        // this ensures minUnSynced is no fresher than maxSynced By contrast,
        // if we get the two variables in the reverse order, then we cannot be
        // sure which variable is fresher, that can lead to the missing of
        // some entries during hash calculation

        LogEntry* unsyncedEntry = minUnSyncedLogEntryByKey_[entry->body.opKey];
        LogEntry* syncedEntry = maxSyncedLogEntryByKey_[entry->body.opKey];

        if (syncedEntry && syncedEntry->body.isWrite == false) {
          // Only Write matters
          syncedEntry = syncedEntry->prevNonCommutativeWrite;
          assert(syncedEntry == NULL || syncedEntry->body.isWrite);
        }
        if (syncedEntry == NULL) {
          // The index sync process may have not been started, or may have not
          // catch up; Or the unsynced logs have been reclaimed by
          // GarbageCollectionThread (we have advanced
          // safeToClearUnSyncedLogId_) We cannot decide the sync-point, so
          // we directly reply with the XORed hash (similar to the leader)
          reply.set_hash(hash.hash, SHA_DIGEST_LENGTH);
          reply.set_maxsyncedlogid(maxSyncedLogEntry_.load()->logId);
          uint32_t proxyMachineId = HIGH_32BIT(entry->body.proxyId);
          repliedSyncPoint_[proxyMachineId] = reply.maxsyncedlogid();
          fastReplySender_[id]->SendMsgTo(*addr, reply,
                                          MessageType::FAST_REPLY);
          // replyLogQu_.enqueue(reply);
        } else {
          // The follower already gets some synced non-commutative logs (via
          // index sync process)

          // Log entries up to syncedEntry are all synced
          // syncedEntry->hash represents them
          if (entry->LessOrEqual(*syncedEntry)) {
            // No need to send fast replies, because this entry has already
            // been covered by index sync process, just give it a dummy reply,
            // which includes the max-synced-log-id
            uint32_t proxyMachineId = HIGH_32BIT(entry->body.proxyId);
            if (repliedSyncPoint_[proxyMachineId] <
                maxSyncedLogEntry_.load()->logId) {
              reply.set_clientid(0);
              reply.set_reqid(0);
              reply.set_logid(0);
              reply.set_maxsyncedlogid(maxSyncedLogEntry_.load()->logId);
              fastReplySender_[id]->SendMsgTo(*addr, reply,
                                              MessageType::FAST_REPLY);
            }
          } else {
            // Beyond syncedEntry, we need to find the boundary in the unsynced
            // logs
            // TODO: Check the following
            // Since unsyncedLogId is no fresher (maybe older) than syncedLogId,
            // then unsyncedEntry may have already been surpasssed by
            // syncedEntry, we need to remove the (potential) overlap

            while (unsyncedEntry->LessOrEqual(*syncedEntry)) {
              if (unsyncedEntry->body.isWrite) {
                if (unsyncedEntry->nextNonCommutative) {
                  unsyncedEntry = unsyncedEntry->nextNonCommutative;
                } else {
                  break;
                }
              } else {
                if (unsyncedEntry->nextNonCommutative) {
                  unsyncedEntry = unsyncedEntry->nextNonCommutative;
                } else {
                  break;
                }
              }
            }
            // LogStruct log;
            // log.originalHash = hash.toString();
            // hash encodes all the (unsynced) entries up to entry
            hash.XOR(unsyncedEntry->logHash);  // Remove all previous hash
            // before unsyncedEntry [included]
            // log.unsynced = unsyncedEntry;
            // log.addback = false;
            if (syncedEntry->LessThan(*unsyncedEntry)) {
              // add itself back (read request is 0)
              hash.XOR(unsyncedEntry->entryHash);
              // log.addback = true;
            }
            // Now hash only encodes [unsyncedEntry, entry]
            // Let's add the synced part
            // log.synced = syncedEntry;
            hash.XOR(syncedEntry->logHash);
            // log.finalE = entry;
            // entryQu_.enqueue(log);
            reply.set_hash(hash.hash, SHA_DIGEST_LENGTH);
            reply.set_maxsyncedlogid(maxSyncedLogEntry_.load()->logId);
            uint32_t proxyMachineId = HIGH_32BIT(entry->body.proxyId);
            repliedSyncPoint_[proxyMachineId] = reply.maxsyncedlogid();
            fastReplySender_[id]->SendMsgTo(*addr, reply,
                                            MessageType::FAST_REPLY);
            // replyLogQu_.enqueue(reply);
          }
        }
      }
    }
  }
  uint32_t preVal = activeWorkerNum_.fetch_sub(1);
  LOG(INFO) << "Fast Reply Terminated " << preVal - 1 << " worker remaining";
}

void Replica::SlowReplyThread(int id) {
  activeWorkerNum_.fetch_add(1);
  Reply reply;
  reply.set_replicaid(replicaId_);
  reply.set_hash("");
  // uint32_t replyNum = 0;
  // uint64_t startTime, endTime;
  while (status_ != ReplicaStatus::TERMINATED) {
    BlockWhenStatusIsNot(ReplicaStatus::NORMAL);
    if (AmLeader()) {
      // Leader does not send slow replies
      nanosleep((const struct timespec[]){{0, 1000000L}}, NULL);
      continue;
    }
    LogEntry* entry = NULL;
    if (slowReplyQu_[id].try_dequeue(entry)) {
      uint32_t logId = entry->logId;
      reply.set_view(viewId_);
      reply.set_clientid((entry->body.reqKey) >> 32);
      reply.set_reqid((uint32_t)(entry->body.reqKey));
      // Optimize: SLOW_REPLY => COMMIT_REPLY
      if (logId <= committedLogId_) {
        reply.set_replytype(MessageType::COMMIT_REPLY);
        reply.set_result(entry->result);
      } else {
        reply.set_replytype(MessageType::SLOW_REPLY);
        reply.set_result("");
      }
      reply.set_owd(owdMap_.get(entry->body.proxyId));
      reply.set_maxsyncedlogid(maxSyncedLogEntry_.load()->logId);

      Address* addr = proxyAddressMap_.get(entry->body.proxyId);
      if (addr) {
        slowReplySender_[id]->SendMsgTo(*addr, reply, MessageType::SLOW_REPLY);
      }
      // replyNum++;
      // if (replyNum == 1) {
      //   startTime = GetMicrosecondTimestamp();
      // } else if (replyNum % 100000 == 0) {
      //   endTime = GetMicrosecondTimestamp();
      //   float rate = 100000 / ((endTime - startTime) * 1e-6);
      //   LOG(INFO) << "id=" << id << "\t Slow Reply Rate=" << rate
      //             << "\t QuLen=" << slowReplyQu_[id].size_approx() << "\t"
      //             << "pendingIndexSync_ qu =" << pendingIndexSync_.size();
      //   startTime = endTime;
      // }
    }
  }
  uint32_t preVal = activeWorkerNum_.fetch_sub(1);
  LOG(INFO) << "SlowReplyThread Terminated " << preVal - 1
            << " worker remaining ";
}

void Replica::IndexSendThread(int id, int cvId) {
  activeWorkerNum_.fetch_add(1);
  LogEntry* lastSyncedEntry = syncedLogEntryHead_;
  IndexSync indexSyncMsg;
  uint32_t syncPeriod = replicaConfig_.indexSyncPeriodUs;
  struct timespec sleepIntval({0, syncPeriod * 1000});
  while (status_ != ReplicaStatus::TERMINATED) {
    BlockWhenStatusIsNot(ReplicaStatus::NORMAL);
    if (!AmLeader()) {
      // Although this replica is not leader currently,
      // we still keep this thread. When it becomes the leader
      // we can immediately use the thread instead of launching extra threads
      // (slowly)
      nanosleep((const struct timespec[]){{0, 1000000L}}, NULL);
      continue;
    }
    if (maxSyncedLogEntry_ == NULL) {
      continue;
    }

    // (1) Leader has some indices to sync
    // (2) There is noting to send, but we still send an indexSync msg every
    // 10ms (to serve as leader's heartbeat)
    indexSyncMsg.set_view(viewId_);
    indexSyncMsg.set_logidbegin(lastSyncedEntry->logId + 1);
    uint32_t logEnd = maxSyncedLogEntry_.load()->logId;
    logEnd = std::min(indexSyncMsg.logidbegin() + indexTransferBatch_, logEnd);
    indexSyncMsg.set_logidend(logEnd);
    indexSyncMsg.clear_deadlines();
    indexSyncMsg.clear_reqkeys();
    for (uint32_t i = indexSyncMsg.logidbegin(); i <= indexSyncMsg.logidend();
         i++) {
      LogEntry* entry = lastSyncedEntry->next;
      ASSERT(entry != NULL);
      ASSERT(entry->logId == i);
      indexSyncMsg.add_deadlines(entry->body.deadline);
      indexSyncMsg.add_reqkeys(entry->body.reqKey);
      lastSyncedEntry = entry;
    }

    indexSyncMsg.set_sendtime(GetMicrosecondTimestamp());
    // Send to all followers
    for (uint32_t r = 0; r < replicaNum_; r++) {
      if (r != replicaId_) {
        indexSender_[id]->SendMsgTo(*(indexReceiver_[r]), indexSyncMsg,
                                    MessageType::SYNC_INDEX);
      }
    }

    nanosleep(&sleepIntval, NULL);
  }
  uint32_t preVal = activeWorkerNum_.fetch_sub(1);
  LOG(INFO) << "IndexSendThread Terminated " << preVal - 1
            << " worker remaining";
}

void Replica::IndexRecvThread() {
  activeWorkerNum_.fetch_add(1);
  while (status_ != ReplicaStatus::TERMINATED) {
    BlockWhenStatusIsNot(ReplicaStatus::NORMAL);
    indexSyncContext_->Register(endPointType_);
    indexSyncContext_->endPoint_->LoopRun();
  }
  uint32_t preVal = activeWorkerNum_.fetch_sub(1);
  LOG(INFO) << "IndexRecvThread Terminated " << preVal - 1
            << " worker remaining";
}

void Replica::ReceiveIndexSyncMessage(MessageHeader* msgHdr, char* msgBuffer) {
  // Promise to the GarbageCollectThread, that I will not use the data before
  // safeToClearLateBufferLogId_ and safeToClearUnSyncedLogId_, so that
  // GarbageCollectThread can safely reclaim them
  safeToClearLateBufferLogId_.store(prepareToClearLateBufferLogId_.load());
  safeToClearUnSyncedLogId_[fastReplyQu_.size()].store(
      prepareToClearUnSyncedLogId_.load());

  MessageHeader* newMsgHdr = new MessageHeader(msgHdr->msgType, msgHdr->msgLen);
  char* newBuffer = new char[msgHdr->msgLen];
  memcpy(newBuffer, msgBuffer, msgHdr->msgLen);

  indexQu_.enqueue({newMsgHdr, newBuffer});
}

void Replica::IndexProcessThread() {
  activeWorkerNum_.fetch_add(1);
  std::pair<MessageHeader*, char*> ele;
  while (status_ != ReplicaStatus::TERMINATED) {
    BlockWhenStatusIsNot(ReplicaStatus::NORMAL);
    while (indexQu_.try_dequeue(ele)) {
      MessageHeader* msgHdr = ele.first;
      char* msgBuffer = ele.second;
      if (msgHdr->msgType == MessageType::SYNC_INDEX) {
        IndexSync idxSyncMsg;
        if (idxSyncMsg.ParseFromArray(msgBuffer, msgHdr->msgLen)) {
          if (!CheckView(idxSyncMsg.view(), false)) {
            delete msgHdr;
            delete[] msgBuffer;
            break;
          }

          lastHeartBeatTime_ = GetMicrosecondTimestamp();
          if (idxSyncMsg.logidbegin() > idxSyncMsg.logidend()) {
            // Pure heart beat
            continue;
          }

          if (idxSyncMsg.logidend() > maxSyncedLogEntry_.load()->logId) {
            std::pair<uint32_t, uint32_t> key(idxSyncMsg.logidbegin(),
                                              idxSyncMsg.logidend());
            pendingIndexSync_[key] = idxSyncMsg;
          }
          // Process pendingIndexSync, if any
          while (!pendingIndexSync_.empty()) {
            if (ProcessIndexSync(pendingIndexSync_.begin()->second)) {
              pendingIndexSync_.erase(pendingIndexSync_.begin());
            } else {
              break;
            }
          }
        }
      } else if (msgHdr->msgType == MessageType::MISSED_REQ) {
        MissedReq missedReqMsg;
        if (missedReqMsg.ParseFromArray(msgBuffer, msgHdr->msgLen)) {
          for (int i = 0; i < missedReqMsg.reqs().size(); i++) {
            const RequestBodyMsg& rbMsg = missedReqMsg.reqs(i);
            if (missedReqKeys_.find(rbMsg.reqkey()) != missedReqKeys_.end()) {
              RequestBody* rb = new RequestBody(
                  rbMsg.deadline(), rbMsg.reqkey(), rbMsg.key(),
                  rbMsg.proxyid(), rbMsg.command(), rbMsg.iswrite());
              // We must handle it to ProcessThread instead of processing it
              // here, to avoid data race (and further memroy leakage), although
              // it is a trivial possibility
              uint32_t quId = rbMsg.reqkey() % recordQu_.size();
              recordQu_[quId].enqueue(rb);

              missedReqKeys_.erase(rbMsg.reqkey());
              fetchTime_.push_back(GetMicrosecondTimestamp() -
                                   askTimebyReqKey_[rbMsg.reqkey()]);
              askTimebyReqKey_.erase(rbMsg.reqkey());
            }
          }
        }
      } else {
        LOG(WARNING) << "Unexpected msg type " << (int)(msgHdr->msgType);
      }
      delete msgHdr;
      delete[] msgBuffer;
    }
  }
  uint32_t preVal = activeWorkerNum_.fetch_sub(1);
  LOG(INFO) << "IndexProcessThread Terminated: " << preVal - 1
            << " worker remaining";
}

bool Replica::ProcessIndexSync(const IndexSync& idxSyncMsg) {
  uint32_t maxSyncedLogId = maxSyncedLogEntry_.load()->logId;
  if (idxSyncMsg.logidend() <= maxSyncedLogId) {
    // This idxSyncMsg is useless
    return true;
  }
  if (idxSyncMsg.logidbegin() > maxSyncedLogId + 1) {
    // Missing some indices
    missedIndices_ = {maxSyncedLogId + 1, idxSyncMsg.logidbegin() - 1};
    AskMissedIndex();
    return false;
  }

  // Coming here means, no index is missing
  if (indexSyncContext_->endPoint_->isTimerRegistered(indexAskTimer_)) {
    indexSyncContext_->endPoint_->UnRegisterTimer(indexAskTimer_);
  }

  for (uint32_t logId = maxSyncedLogId + 1; logId <= idxSyncMsg.logidend();
       logId++) {
    uint32_t offset = logId - idxSyncMsg.logidbegin();
    uint64_t reqKey = idxSyncMsg.reqkeys(offset);
    uint64_t deadline = idxSyncMsg.deadlines(offset);
    uint32_t quId = reqKey % recordMap_.size();
    LogEntry* entry = recordMap_[quId].get(reqKey);
    if (entry && missedReqKeys_.empty()) {
      SHA_HASH myHash;
      SHA_HASH hash;
      if (entry->body.isWrite) {
        myHash = CalculateHash(deadline, reqKey);
        hash = myHash;
      }
      LogEntry* prevNonCommutative = maxSyncedLogEntryByKey_[entry->body.opKey];
      LogEntry* prevNonCommutativeWrite = NULL;
      if (prevNonCommutative) {
        if (prevNonCommutative->body.isWrite) {
          prevNonCommutativeWrite = prevNonCommutative;
        } else {
          prevNonCommutativeWrite = prevNonCommutative->prevNonCommutativeWrite;
        }
      }
      assert(prevNonCommutativeWrite == NULL ||
             prevNonCommutativeWrite->body.isWrite);
      if (prevNonCommutativeWrite) {
        // This request has some pre non-commutative ones
        // In that way, XOR the previous accumulated hash
        hash.XOR(prevNonCommutativeWrite->logHash);
      }
      LogEntry* newEntry =
          new LogEntry(entry->body, myHash, hash, prevNonCommutative, NULL,
                       prevNonCommutativeWrite, NULL, maxSyncedLogEntry_, NULL);
      newEntry->status = EntryStatus::TO_SLOW_REPLY;
      newEntry->logId = logId;
      ASSERT(logId == maxSyncedLogEntry_.load()->logId + 1);
      maxSyncedLogEntry_.load()->next = newEntry;
      if (prevNonCommutative) {
        prevNonCommutative->nextNonCommutative = newEntry;
      }
      if (newEntry->body.isWrite && prevNonCommutativeWrite) {
        prevNonCommutativeWrite->nextNonCommutativeWrite = newEntry;
      }
      // uint32_t prevMaxLogId = maxSyncedLogEntry_.load()->logId;
      maxSyncedLogEntry_ = newEntry;
      ASSERT(maxSyncedLogEntry_.load()->logId == logId);
      ASSERT(prevMaxLogId + 1 == logId);

      maxSyncedLogEntryByKey_[newEntry->body.opKey] = newEntry;
      uint32_t quId = (newEntry->body.reqKey) % slowReplyQu_.size();
      slowReplyQu_[quId].enqueue(newEntry);

      ASSERT(newEntry->prev->logId + 1 == newEntry->logId);
      // TODO： Think about the order above

      // Chunk UnSynced logs
      if (minUnSyncedLogEntryByKey_[newEntry->body.opKey]) {
        // Try to advance  minUnSyncedLogIdByKey_[opKey]
        LogEntry* unSyncedEntry =
            minUnSyncedLogEntryByKey_[newEntry->body.opKey];

        while (unSyncedEntry->LessOrEqual(*entry)) {
          if (unSyncedEntry->body.isWrite) {
            if (unSyncedEntry->nextNonCommutativeWrite) {
              unSyncedEntry = unSyncedEntry->nextNonCommutativeWrite;
            } else {
              break;
            }
          } else {
            if (unSyncedEntry->nextNonCommutative) {
              unSyncedEntry = unSyncedEntry->nextNonCommutative;
            } else {
              break;
            }
          }
        }
        minUnSyncedLogEntryByKey_[newEntry->body.opKey] = unSyncedEntry;
      }
    } else {
      missedReqKeys_.insert(reqKey);
    }
  }
  if (missedReqKeys_.empty()) {
    return true;
  } else {
    AskMissedRequest();
    return false;
  }
}

void Replica::MissedIndexAckThread() {
  activeWorkerNum_.fetch_add(1);
  while (status_ != ReplicaStatus::TERMINATED) {
    BlockWhenStatusIsNot(ReplicaStatus::NORMAL);
    missedIndexAckContext_->Register(endPointType_);
    missedIndexAckContext_->endPoint_->LoopRun();
  }
  uint32_t preVal = activeWorkerNum_.fetch_sub(1);
  LOG(INFO) << "MissedIndexAckThread Terminated " << preVal - 1
            << " worker remaining";
}

void Replica::ReceiveAskMissedIdx(MessageHeader* msgHdr, char* msgBuffer) {
  AskIndex askIndex;
  if (msgHdr->msgType == MessageType::MISSED_INDEX_ASK &&
      askIndex.ParseFromArray(msgBuffer, msgHdr->msgLen)) {
    uint32_t logBegin = askIndex.logidbegin();
    uint32_t logEnd =
        std::min(maxSyncedLogEntry_.load()->logId, askIndex.logidend());
    for (uint32_t i = logBegin; i <= logEnd; i += indexTransferBatch_) {
      IndexSync indexSyncMsg;
      indexSyncMsg.set_view(viewId_);
      indexSyncMsg.set_logidbegin(i);
      uint32_t end = std::min(i + indexTransferBatch_ - 1, logEnd);
      indexSyncMsg.set_logidend(end);
      uint32_t logid = i;
      LogEntry* entryStart = syncedLogEntryByLogId_.get(logid);
      if (!entryStart) {
        // Since the update of syncedLogEntryByLogId_ may lag a bit behind
        // maxSyncedLogEntry_. entryStart may be NULL. In that case, we
        // terminate here
        break;
      }

      ASSERT(entryStart->logId == logid);
      while (entryStart->logId <= end) {
        indexSyncMsg.add_deadlines(entryStart->body.deadline);
        indexSyncMsg.add_reqkeys(entryStart->body.reqKey);
        entryStart = entryStart->next;
      }
      indexAcker_->SendMsgTo(*(indexReceiver_[askIndex.replicaid()]),
                             indexSyncMsg, MessageType::SYNC_INDEX);
    }
  }
}

void Replica::MissedReqAckThread() {
  activeWorkerNum_.fetch_add(1);
  while (status_ != ReplicaStatus::TERMINATED) {
    BlockWhenStatusIsNot(ReplicaStatus::NORMAL);
    missedReqAckContext_->Register(endPointType_);
    missedReqAckContext_->endPoint_->LoopRun();
  }
  uint32_t preVal = activeWorkerNum_.fetch_sub(1);
  LOG(INFO) << "MissedReqAckThread Terminated " << preVal - 1
            << " worker remaining";
}

void Replica::ReceiveAskMissedReq(MessageHeader* msgHdr, char* msgBuffer) {
  AskReq askReqMsg;
  if (msgHdr->msgType == MessageType::MISSED_REQ_ASK &&
      askReqMsg.ParseFromArray(msgBuffer, msgHdr->msgLen)) {
    MissedReq missedReqMsg;
    missedReqMsg.set_replicaid(this->replicaId_);
    for (int i = 0; i < askReqMsg.missedreqkeys_size(); i++) {
      uint64_t reqKey = askReqMsg.missedreqkeys(i);
      uint32_t quId = reqKey % recordMap_.size();
      LogEntry* entry = recordMap_[quId].get(reqKey);
      if (entry) {
        RequestBodyToMessage(entry->body, missedReqMsg.add_reqs());
      }
      if ((uint32_t)(missedReqMsg.reqs_size()) >= requestTrasnferBatch_) {
        missedReqAckContext_->endPoint_->SendMsgTo(
            *(indexReceiver_[askReqMsg.replicaid()]), missedReqMsg,
            MessageType::MISSED_REQ);
        missedReqMsg.clear_reqs();
      }
    }

    if (missedReqMsg.reqs_size() > 0) {
      // This ack is useful because it really contains some missed requests,
      // so send it
      missedReqAckContext_->endPoint_->SendMsgTo(
          *(indexReceiver_[askReqMsg.replicaid()]), missedReqMsg,
          MessageType::MISSED_REQ);
    }
  }
}

void Replica::RequestBodyToMessage(const RequestBody& rb,
                                   RequestBodyMsg* rbMsg) {
  rbMsg->set_deadline(rb.deadline);
  rbMsg->set_reqkey(rb.reqKey);
  rbMsg->set_proxyid(rb.proxyId);
  rbMsg->set_command(rb.command);
  rbMsg->set_key(rb.opKey);
  rbMsg->set_iswrite(rb.isWrite);
}

void Replica::AskMissedIndex() {
  if (missedIndices_.first > missedIndices_.second) {
    // indexSyncContext_->endPoint_->UnRegisterTimer(indexAskTimer_);
    return;
  }
  uint64_t nowTime = GetMicrosecondTimestamp();
  if (lastAskMissedIndexTime_ + 50 > nowTime) {
    return;
  }
  AskIndex askIndexMsg;
  askIndexMsg.set_replicaid(this->replicaId_);
  askIndexMsg.set_logidbegin(missedIndices_.first);
  askIndexMsg.set_logidend(missedIndices_.second);

  // roundRobinIndexAskIdx_ = 0;// Debug

  // Do not ask leader every time, choose random replica to ask to avoid
  // leader bottleneck
  indexRequester_->SendMsgTo(
      *(indexAskReceiver_[roundRobinIndexAskIdx_ % replicaNum_]), askIndexMsg,
      MessageType::MISSED_INDEX_ASK);
  roundRobinIndexAskIdx_++;
  if (roundRobinIndexAskIdx_ % replicaNum_ == replicaId_) {
    roundRobinIndexAskIdx_++;
  }
  lastAskMissedIndexTime_ = GetMicrosecondTimestamp();
}

void Replica::AskMissedRequest() {
  if (missedReqKeys_.empty()) {
    // no need to start timer
    return;
  }
  uint64_t nowTime = GetMicrosecondTimestamp();
  if (lastAskMissedIndexTime_ + 50 > nowTime) {
    return;
  }
  AskReq askReqMsg;
  askReqMsg.set_replicaid(this->replicaId_);
  for (const uint64_t& reqKey : missedReqKeys_) {
    askReqMsg.add_missedreqkeys(reqKey);
    if ((uint32_t)(askReqMsg.missedreqkeys_size()) >=
        requestKeyTransferBatch_) {
      reqRequester_->SendMsgTo(
          *(requestAskReceiver_[roundRobinRequestAskIdx_ % replicaNum_]),
          askReqMsg, MessageType::MISSED_REQ_ASK);
      roundRobinRequestAskIdx_++;
      if (roundRobinRequestAskIdx_ % replicaNum_ == replicaId_) {
        roundRobinRequestAskIdx_++;
      }
      askReqMsg.clear_missedreqkeys();
    }
    askTimebyReqKey_[reqKey] = GetMicrosecondTimestamp();
  }
  if (askReqMsg.missedreqkeys_size() > 0) {
    reqRequester_->SendMsgTo(*(requestAskReceiver_[viewId_ % replicaNum_]),
                             askReqMsg, MessageType::MISSED_REQ_ASK);

    roundRobinRequestAskIdx_++;
    if (roundRobinRequestAskIdx_ % replicaNum_ == replicaId_) {
      roundRobinRequestAskIdx_++;
    }
    lastAskMissedRequestTime_ = GetMicrosecondTimestamp();
  }
}

void Replica::GarbageCollectThread() {
  activeWorkerNum_.fetch_add(1);
  while (status_ != ReplicaStatus::TERMINATED) {
    BlockWhenStatusIsNot(ReplicaStatus::NORMAL);
    // Reclaim stale crashVector
    ReclaimStaleCrashVector();
    // Reclaim (unsynced) stale logs
    ReclaimStaleLogs();
    // Check LateBuffer and UnSyncedLog items and try to advance
    // prepareToClearLateBufferLogId_ and prepareToClearUnSyncedLogId_
    PrepareNextReclaim();
  }
  uint32_t preVal = activeWorkerNum_.fetch_sub(1);
  LOG(INFO) << "GarbageCollectThread Terminated " << preVal - 1
            << " worker remaining";
}

void Replica::ReclaimStaleCrashVector() {
  uint32_t masterCVVersion = crashVectorInUse_[0].load()->version_;
  while (cvVersionToClear_ <= masterCVVersion) {
    bool canDelete = true;
    for (uint32_t i = 0; i < crashVectorVecSize_; i++) {
      if (crashVectorInUse_[i].load()->version_ <= cvVersionToClear_) {
        canDelete = false;
        break;
      }
    }
    if (canDelete) {
      CrashVectorStruct* cvToClear = crashVector_.get(cvVersionToClear_);
      crashVector_.erase(cvVersionToClear_);
      delete cvToClear;
      cvVersionToClear_++;
    } else {
      break;
    }
  }
}

void Replica::ReclaimStaleLogs() {
  uint32_t safePoint = prepareToClearUnSyncedLogId_;

  for (uint32_t shardIdx = 0; shardIdx < fastReplyQu_.size() + 1; shardIdx++) {
    safePoint = std::min(safePoint, safeToClearUnSyncedLogId_[shardIdx].load());
  }
  // Reclaim UnSynced Entries

  // Reclaim Entries in late-buffer
  safePoint = safeToClearLateBufferLogId_;
}

void Replica::PrepareNextReclaim() {}

void Replica::CheckHeartBeat() {
  if (status_ == ReplicaStatus::TERMINATED) {
    masterContext_->endPoint_->LoopBreak();
    return;
  }
  if (AmLeader()) {
    return;
  }
  if (status_ != ReplicaStatus::NORMAL) {
    // Some worker threads have detected viewchange and switch status_ to
    // VIEWCHANGE But workers have no priviledge to increment viewId_ and
    // initiate view change process, so the master will do that
    VLOG(2) << "InitiateViewChange-10";
    InitiateViewChange(viewId_ + 1);
    return;
  }
  uint64_t nowTime = GetMicrosecondTimestamp();
  uint64_t threashold = replicaConfig_.heartbeatThresholdMs * 1000;

  if (lastHeartBeatTime_ + threashold < nowTime) {
    // I haven't heard from the leader for too long, it probably has died
    // Before start view change, clear context
    VLOG(2) << "InitiateViewChange-1";
    InitiateViewChange(viewId_ + 1);
  }
}

void Replica::ReceiveMasterMessage(MessageHeader* msgHdr, char* msgBuffer) {
  VLOG(4) << "msgType " << (uint32_t)(msgHdr->msgType);

  if (msgHdr->msgType == MessageType::VIEWCHANGE_REQ) {
    ViewChangeRequest viewChangeReq;
    if (viewChangeReq.ParseFromArray(msgBuffer, msgHdr->msgLen)) {
      ProcessViewChangeReq(viewChangeReq);
    }

  } else if (msgHdr->msgType == MessageType::VIEWCHANGE_MSG) {
    ViewChange viewChangeMsg;
    if (viewChangeMsg.ParseFromArray(msgBuffer, msgHdr->msgLen)) {
      ProcessViewChange(viewChangeMsg);
    }

  } else if (msgHdr->msgType == MessageType::STATE_TRANSFER_REQUEST) {
    StateTransferRequest stateTransferReq;
    if (stateTransferReq.ParseFromArray(msgBuffer, msgHdr->msgLen)) {
      ProcessStateTransferRequest(stateTransferReq);
    }
  } else if (msgHdr->msgType == MessageType::STATE_TRANSFER_REPLY) {
    StateTransferReply stateTransferRep;
    if (stateTransferRep.ParseFromArray(msgBuffer, msgHdr->msgLen)) {
      ProcessStateTransferReply(stateTransferRep);
    }
  } else if (msgHdr->msgType == MessageType::START_VIEW) {
    StartView startView;
    if (startView.ParseFromArray(msgBuffer, msgHdr->msgLen)) {
      ProcessStartView(startView);
    }
  } else if (msgHdr->msgType == MessageType::CRASH_VECTOR_REQUEST) {
    CrashVectorRequest request;
    if (request.ParseFromArray(msgBuffer, msgHdr->msgLen)) {
      ProcessCrashVectorRequest(request);
    }
  } else if (msgHdr->msgType == MessageType::CRASH_VECTOR_REPLY) {
    CrashVectorReply reply;
    if (reply.ParseFromArray(msgBuffer, msgHdr->msgLen)) {
      VLOG(2) << "CrashVectorReply = " << reply.DebugString();
      ProcessCrashVectorReply(reply);
    }
  } else if (msgHdr->msgType == MessageType::RECOVERY_REQUEST) {
    RecoveryRequest request;
    if (request.ParseFromArray(msgBuffer, msgHdr->msgLen)) {
      ProcessRecoveryRequest(request);
    }
  } else if (msgHdr->msgType == MessageType::RECOVERY_REPLY) {
    RecoveryReply reply;
    if (reply.ParseFromArray(msgBuffer, msgHdr->msgLen)) {
      ProcessRecoveryReply(reply);
    }
  } else if (msgHdr->msgType == MessageType::SYNC_STATUS_REPORT) {
    SyncStatusReport report;
    if (report.ParseFromArray(msgBuffer, msgHdr->msgLen)) {
      ProcessSyncStatusReport(report);
    }
  } else if (msgHdr->msgType == MessageType::COMMIT_INSTRUCTION) {
    CommitInstruction commit;
    if (commit.ParseFromArray(msgBuffer, msgHdr->msgLen)) {
      ProcessCommitInstruction(commit);
    }
  }

  else {
    LOG(WARNING) << "Unexpected message type " << (int)msgBuffer[0];
  }
}

void Replica::SendViewChangeRequest(const int toReplicaId) {
  ViewChangeRequest viewChangeReq;
  viewChangeReq.set_view(viewId_);
  viewChangeReq.set_replicaid(replicaId_);
  CrashVectorStruct* cv = crashVectorInUse_[0].load();
  viewChangeReq.mutable_cv()->Add(cv->cv_.begin(), cv->cv_.end());

  if (toReplicaId < 0) {
    // send to all
    for (uint32_t i = 0; i < replicaNum_; i++) {
      if (i != replicaId_) {
        // no need to send to myself
        masterContext_->endPoint_->SendMsgTo(
            *(masterReceiver_[i]), viewChangeReq, MessageType::VIEWCHANGE_REQ);
      }
    }
  } else {
    masterContext_->endPoint_->SendMsgTo(*(masterReceiver_[toReplicaId]),
                                         viewChangeReq,
                                         MessageType::VIEWCHANGE_REQ);
  }
}

void Replica::SendViewChange() {
  if (AmLeader()) {
    // I am the leader of this new view, no need to send to myself
    return;
  }

  ViewChange viewChangeMsg;
  viewChangeMsg.set_view(viewId_);
  viewChangeMsg.set_replicaid(replicaId_);
  CrashVectorStruct* cv = crashVectorInUse_[0].load();
  viewChangeMsg.mutable_cv()->Add(cv->cv_.begin(), cv->cv_.end());
  viewChangeMsg.set_syncpoint(maxSyncedLogEntry_.load()->logId);
  if (filteredUnSyncedEntries_.size() > 1) {
    viewChangeMsg.set_unsynclogbegin(1);
    viewChangeMsg.set_unsynclogend(filteredUnSyncedEntries_.size() - 1);
  } else {
    viewChangeMsg.set_unsynclogbegin(0);
    viewChangeMsg.set_unsynclogend(0);
  }

  viewChangeMsg.set_lastnormalview(lastNormalView_);
  masterContext_->endPoint_->SendMsgTo(
      *(masterReceiver_[viewId_ % replicaNum_]), viewChangeMsg,
      MessageType::VIEWCHANGE_MSG);
}

void Replica::InitiateViewChange(const uint32_t view) {
  if (viewId_ > view) {
    LOG(ERROR) << "Invalid view change initiation currentView=" << viewId_
               << "\ttargetView=" << view;
    return;
  }

  if (viewId_ == view && status_ == ReplicaStatus::VIEWCHANGE) {
    // Already in viewchange
    return;
  }

  status_ = ReplicaStatus::VIEWCHANGE;
  LOG(INFO) << "status =" << (int)status_ << "\t"
            << " view=" << viewId_ << "\t"
            << " targeting view=" << view;

  // Wait until every worker stop
  while (activeWorkerNum_ > 0) {
    usleep(1000);
  }

  /** Since the update of syncedLogEntryByReqKey_ and syncedLogEntryByLogId_
   * may have not been completed when they encounter view change, let's first
   * complete (flush) them */
  LogEntry* minTrackedEntry = trackedEntry_[0];
  for (uint32_t i = 0; i < trackedEntry_.size(); i++) {
    if (minTrackedEntry->logId > trackedEntry_[i]->logId) {
      minTrackedEntry = trackedEntry_[i];
    }
  }
  while (minTrackedEntry->next) {
    LogEntry* next = minTrackedEntry->next;
    if (syncedLogEntryByLogId_.get(next->logId) == NULL) {
      syncedLogEntryByLogId_.assign(next->logId, next);
      syncedLogEntryByReqKey_.assign(next->body.reqKey, next);
    }
    minTrackedEntry = next;
  }
  trackedEntry_.assign(trackedEntry_.size(), minTrackedEntry);

  LogEntry* entryStart = minUnSyncedLogEntry_;
  if (entryStart->logId < CONCURRENT_MAP_START_INDEX) {
    // This is dummy, move to its next;
    entryStart = entryStart->next;
  }
  filteredUnSyncedEntries_.clear();
  filteredUnSyncedEntries_.resize(
      1);  // Reserve 1 slot as dummy value [because 0 has special use]
  while (entryStart) {
    LogEntry* entry = syncedLogEntryByReqKey_.get(entryStart->body.reqKey);
    if (!entry) {
      // Has not been synced
      filteredUnSyncedEntries_.push_back(entryStart);
    }
    entryStart = entryStart->next;
  }

  viewId_ = view;
  // Unregister all timers, except the monitorTimer (so as the master thread
  // can break when status=Terminated)
  masterContext_->endPoint_->UnRegisterAllTimers();
  masterContext_->endPoint_->RegisterTimer(masterContext_->monitorTimer_);
  LOG(INFO) << "Monitor Timer Registered "
            << "viewId=" << viewId_ << "\t"
            << "maxSyncedLogId=" << maxSyncedLogEntry_.load()->logId << "\t"
            << "committedLogId=" << committedLogId_ << "\t"
            << "filteredUnSyncedEntries_.size()="
            << filteredUnSyncedEntries_.size() << "\t"
            << "currentTime=" << GetMicrosecondTimestamp() << "\t";
  // Launch viewChange timer
  masterContext_->endPoint_->RegisterTimer(viewChangeTimer_);
}

void Replica::BroadcastViewChange() {
  if (status_ == ReplicaStatus::NORMAL) {
    // Can stop the timer
    masterContext_->endPoint_->UnRegisterTimer(viewChangeTimer_);
    return;
  }
  // Broadcast VIEW-CHANGE-REQ to all replicas
  SendViewChangeRequest(-1);
  // Send VIEW-CHANGE to the leader in this view
  SendViewChange();
}

void Replica::SendStartView(const int toReplicaId) {
  StartView startView;
  startView.set_replicaid(replicaId_);
  startView.set_view(viewId_);
  CrashVectorStruct* cv = crashVectorInUse_[0];
  startView.mutable_cv()->Add(cv->cv_.begin(), cv->cv_.end());
  // startView.set_syncedlogid(maxSyncedLogId_);
  startView.set_syncedlogid(maxSyncedLogEntry_.load()->logId);
  if (toReplicaId >= 0) {
    // send to one
    masterContext_->endPoint_->SendMsgTo(*(masterReceiver_[toReplicaId]),
                                         startView, MessageType::START_VIEW);
  } else {
    // send to all
    for (uint32_t i = 0; i < replicaNum_; i++) {
      if (i == replicaId_) {
        // No need to send to self
        continue;
      }
      masterContext_->endPoint_->SendMsgTo(*(masterReceiver_[i]), startView,
                                           MessageType::START_VIEW);
      VLOG(2) << "Send StartView to " << i << "\t"
              << masterReceiver_[i]->GetIPAsString() << ":"
              << masterReceiver_[i]->GetPortAsInt();
    }
  }
}

void Replica::SendSyncStatusReport() {
  SyncStatusReport report;
  report.set_view(viewId_);
  report.set_replicaid(replicaId_);
  CrashVectorStruct* cv = crashVectorInUse_[0].load();
  report.mutable_cv()->Add(cv->cv_.begin(), cv->cv_.end());
  // report.set_syncedlogid(maxSyncedLogId_);
  report.set_syncedlogid(maxSyncedLogEntry_.load()->logId);
  if (AmLeader()) {
    // leader directly process its own report
    ProcessSyncStatusReport(report);
  } else {
    // send to leader
    masterContext_->endPoint_->SendMsgTo(
        *(masterReceiver_[viewId_ % replicaNum_]), report,
        MessageType::SYNC_STATUS_REPORT);
  }
}

void Replica::SendCommit() {
  CommitInstruction commit;
  commit.set_view(viewId_);
  commit.set_replicaid(replicaId_);
  CrashVectorStruct* cv = crashVectorInUse_[0].load();
  commit.mutable_cv()->Add(cv->cv_.begin(), cv->cv_.end());
  commit.set_committedlogid(committedLogId_);
  // LOG(INFO) << "commit " << commit.DebugString();
  for (uint32_t i = 0; i < replicaNum_; i++) {
    if (i != replicaId_) {
      masterContext_->endPoint_->SendMsgTo(*(masterReceiver_[i]), commit,
                                           MessageType::COMMIT_INSTRUCTION);
    }
  }
}

void Replica::ProcessViewChangeReq(const ViewChangeRequest& viewChangeReq) {
  if (status_ == ReplicaStatus::RECOVERING) {
    // Recovering replicas do not participate in view change
    return;
  }
  if (!CheckCV(viewChangeReq.replicaid(), viewChangeReq.cv())) {
    // stray message
    return;
  }
  if (Aggregated(viewChangeReq.cv())) {
    // If cv is updated, then it is likely that some messages in
    // viewChangeSet_ become stray, so remove them
    for (uint32_t i = 0; i < replicaNum_; i++) {
      auto iter = viewChangeSet_.find(i);
      if (iter != viewChangeSet_.end() && (!CheckCV(i, iter->second.cv()))) {
        viewChangeSet_.erase(i);
      }
    }
  }
  if (viewChangeReq.view() > viewId_) {
    VLOG(2) << "InitiateViewChange-2";
    InitiateViewChange(viewChangeReq.view());
  } else {
    if (status_ == ReplicaStatus::NORMAL) {
      SendStartView(viewChangeReq.replicaid());
    } else {
      SendViewChange();
    }
  }
}

void Replica::ProcessViewChange(const ViewChange& viewChange) {
  // LOG(INFO) << "viewChange: " << viewChange.DebugString();
  if (status_ == ReplicaStatus::RECOVERING) {
    // Recovering replicas do not participate in view change
    return;
  }
  if (!CheckCV(viewChange.replicaid(), viewChange.cv())) {
    // stray message
    LOG(WARNING) << "Stray Message";
    return;
  }

  Aggregated(viewChange.cv());

  if (status_ == ReplicaStatus::NORMAL) {
    if (viewChange.view() > viewId_) {
      VLOG(2) << "InitiateViewChange-3";
      InitiateViewChange(viewChange.view());
    } else {
      // The sender lags behind
      SendStartView(viewChange.replicaid());
    }
  } else if (status_ == ReplicaStatus::VIEWCHANGE) {
    if (viewChange.view() > viewId_) {
      VLOG(2) << "InitiateViewChange-4";
      InitiateViewChange(viewChange.view());
    } else if (viewChange.view() < viewId_) {
      SendViewChangeRequest(viewChange.replicaid());
    }
    // viewChange.view() == viewId
    else if (viewChangeSet_.size() >= replicaNum_ / 2 + 1) {
      // We have got enough valid viewchange messages, no need for this one
      return;
    } else {
      ASSERT(AmLeader());
      viewChangeSet_[viewChange.replicaid()] = viewChange;
      VLOG(3) << "viewChangeSet Size=" << viewChangeSet_.size();
      // If cv is updated, then it is likely that some messages in
      // viewChangeSet_ become stray, so remove them
      for (uint32_t i = 0; i < replicaNum_; i++) {
        auto iter = viewChangeSet_.find(i);
        if (iter != viewChangeSet_.end() && (!CheckCV(i, iter->second.cv()))) {
          viewChangeSet_.erase(i);
        }
      }
      if (viewChangeSet_.size() >= replicaNum_ / 2) {
        ASSERT(viewChangeSet_.find(replicaId_) == viewChangeSet_.end());
        // Got f viewChange
        // Plus myself, got f+1 viewChange messages
        ViewChange myvc;
        CrashVectorStruct* masterCV = crashVectorInUse_[0].load();
        myvc.mutable_cv()->Add(masterCV->cv_.begin(), masterCV->cv_.end());
        myvc.set_view(viewId_);
        myvc.set_replicaid(replicaId_);
        // myvc.set_syncpoint(maxSyncedLogId_);
        // myvc.set_unsynclogbegin(minUnSyncedLogId_);
        // myvc.set_unsynclogend(maxUnSyncedLogId_);
        myvc.set_syncpoint(maxSyncedLogEntry_.load()->logId);
        if (filteredUnSyncedEntries_.size() > 1) {
          myvc.set_unsynclogbegin(1);
          myvc.set_unsynclogend(filteredUnSyncedEntries_.size() - 1);
        } else {
          myvc.set_unsynclogbegin(0);
          myvc.set_unsynclogend(0);
        }

        myvc.set_lastnormalview(lastNormalView_);
        viewChangeSet_[replicaId_] = myvc;
        // Has got enough viewChange messages, stop viewChangeTimer
        masterContext_->endPoint_->UnRegisterTimer(viewChangeTimer_);
        TransferSyncedLog();
      }
    }
  } else {
    LOG(WARNING) << "Unexpected Status " << status_;
  }
}

void Replica::TransferSyncedLog() {
  uint32_t largestNormalView = lastNormalView_;
  uint32_t maxSyncedLogId = maxSyncedLogEntry_.load()->logId;
  uint32_t largestSyncPoint = maxSyncedLogId;
  uint32_t targetReplicaId = replicaId_;
  transferSyncedEntry_ = true;
  for (auto& kv : viewChangeSet_) {
    if (largestNormalView < kv.second.lastnormalview()) {
      largestNormalView = kv.second.lastnormalview();
    }
  }
  for (auto& kv : viewChangeSet_) {
    if (kv.second.lastnormalview() == largestNormalView &&
        largestSyncPoint < kv.second.syncpoint()) {
      largestSyncPoint = kv.second.syncpoint();
      targetReplicaId = kv.second.replicaid();
    }
  }

  stateTransferIndices_.clear();
  VLOG(3) << "maxSyncedLogId_=" << maxSyncedLogId << "\t"
          << "largestSyncPoint=" << largestSyncPoint << "\t"
          << "largestNormalView = " << largestNormalView << "\t"
          << "lastNormalView_=" << lastNormalView_;
  // Directly copy the synced entries
  if (largestNormalView == lastNormalView_) {
    if (maxSyncedLogId < largestSyncPoint) {
      stateTransferIndices_[targetReplicaId] = {maxSyncedLogId + 1,
                                                largestSyncPoint};
    }
    // Else: no need to do state transfer, because this replica has all synced
    // entries
  } else {
    stateTransferIndices_[targetReplicaId] = {committedLogId_ + 1,
                                              largestSyncPoint};
  }

  if (!stateTransferIndices_.empty()) {
    // Start state transfer
    // After this state transfer has been completed, continue to execute the
    // callback (MergeUnsyncedLog)

    stateTransferCallback_ = std::bind(&Replica::TransferUnSyncedLog, this);
    stateTransferTerminateTime_ =
        GetMicrosecondTimestamp() + stateTransferTimeout_ * 1000;
    stateTransferTerminateCallback_ =
        std::bind(&Replica::RollbackToViewChange, this);
    LOG(INFO) << "Start state transfer targetReplica " << targetReplicaId
              << "\t"
              << "seg=" << stateTransferIndices_[targetReplicaId].first << "\t"
              << stateTransferIndices_[targetReplicaId].second;
    // Start the state tranfer timer
    masterContext_->endPoint_->RegisterTimer(stateTransferTimer_);
  } else {
    // Directly go to the second stage: transfer unsynced log
    TransferUnSyncedLog();
  }
}

void Replica::TransferUnSyncedLog() {
  // Get the unsynced logs from the f+1 remaining replicas
  // If this process cannot be completed, rollback to view change
  uint32_t largestNormalView = lastNormalView_;
  transferSyncedEntry_ = false;
  for (auto& kv : viewChangeSet_) {
    if (largestNormalView < kv.second.lastnormalview()) {
      largestNormalView = kv.second.lastnormalview();
    }
  }
  VLOG(3) << "TransferUnSyncedLog largestNormalView=" << largestNormalView;

  stateTransferIndices_.clear();
  for (auto& kv : viewChangeSet_) {
    if (kv.second.lastnormalview() < largestNormalView) {
      // No need to transfer log, this guy's unsynced logs do not contribute
      // to committed logs
      continue;
    }
    if (kv.first == replicaId_) {
      // No need to transfer log entries from self
      continue;
    }
    if (kv.second.unsynclogbegin() == 0 && kv.second.unsynclogend() == 0) {
      // This replica has no unsynced logs
      continue;
    }
    // request transfer of the filteredUnSyncedRequests vec
    stateTransferIndices_[kv.first] = {kv.second.unsynclogbegin(),
                                       kv.second.unsynclogend()};
  }
  if (stateTransferIndices_.empty()) {
    // No need to do state transfer for unsynced logs
    // Directly go to new view
    EnterNewView();
    return;
  }
  // After this state transfer is completed, this replica will enter the new
  // view
  stateTransferCallback_ = std::bind(&Replica::MergeUnSyncedLog, this);
  stateTransferTerminateTime_ =
      GetMicrosecondTimestamp() + stateTransferTimeout_ * 1000;
  stateTransferTerminateCallback_ =
      std::bind(&Replica::RollbackToViewChange, this);
  masterContext_->endPoint_->RegisterTimer(stateTransferTimer_);
}

void Replica::MergeUnSyncedLog() {
  int f = replicaNum_ / 2;
  int quorum = (f % 2 == 0) ? (f / 2 + 1) : (f / 2 + 2);
  SHA_HASH dummy;
  for (auto& kv : requestsToMerge_) {
    uint64_t reqKey = kv.first.second;
    LogEntry* entry = kv.second.first;
    int count = kv.second.second;
    if (count >= quorum) {
      if (syncedLogEntryByReqKey_.get(reqKey)) {
        // at-most once
        delete entry;
        continue;
      }
      ProcessRequest(entry, true, false, true);
      syncedLogEntryByReqKey_.assign(reqKey, entry);
      syncedLogEntryByLogId_.assign(entry->logId, entry);
    }
  }
  requestsToMerge_.clear();
  EnterNewView();
}

void Replica::EnterNewView() {
  LOG(INFO) << "Enter New View " << viewId_
            << " maxSyncedLog =" << maxSyncedLogEntry_.load()->logId << "\t"
            << GetMicrosecondTimestamp();
  // Leader sends StartView to all the others
  if (AmLeader()) {
    SendStartView(-1);
  }  // Else: followers directly start

  status_ = ReplicaStatus::NORMAL;
  lastNormalView_.store(viewId_);
  // Update crashVector, all synced with master
  CrashVectorStruct* masterCV = crashVectorInUse_[0].load();
  for (uint32_t i = 1; i < crashVectorVecSize_; i++) {
    crashVectorInUse_[i] = masterCV;
  }
  crashVector_.assign(masterCV->version_, masterCV);

  // More lightweight than CreateContext
  ResetContext();
  // Notify the blocking workers until all workers become active
  while (activeWorkerNum_ < totalWorkerNum_) {
    waitVar_.notify_all();
    usleep(1000);
  }

  LOG(INFO) << "View=" << viewId_
            << " Recovered worker number:" << activeWorkerNum_;
}

void Replica::SendStateTransferRequest() {
  if (GetMicrosecondTimestamp() >= stateTransferTerminateTime_) {
    // If statetransfer cannot be completed within a certain amount of time,
    // rollback to view change
    masterContext_->endPoint_->UnRegisterTimer(stateTransferTimer_);
    LOG(INFO)
        << "The state transfer takes too long, roll back to previous step ";
    stateTransferTerminateCallback_();
    return;
  }

  StateTransferRequest request;
  request.set_view(viewId_);
  request.set_issynced(transferSyncedEntry_);
  request.set_replicaid(replicaId_);
  for (auto& stateTransferInfo : stateTransferIndices_) {
    // Do not request too many entries at one time, otherwise, UDP packet
    // cannot handle that
    uint32_t targetReplica = stateTransferInfo.first;
    uint32_t logBegin = stateTransferInfo.second.first;
    uint32_t logEnd = stateTransferInfo.second.second;

    request.set_logbegin(logBegin);
    if (logBegin + requestTrasnferBatch_ <= logEnd) {
      request.set_logend(logBegin + requestTrasnferBatch_);
    } else {
      request.set_logend(logEnd);
    }

    VLOG(3) << "I am asking stateTransferRequest from " << targetReplica << "\t"
            << request.logbegin() << "\t" << request.logend() << "\t"
            << "\tisSynced=" << request.issynced();
    masterContext_->endPoint_->SendMsgTo(*(masterReceiver_[targetReplica]),
                                         request,
                                         MessageType::STATE_TRANSFER_REQUEST);
  }
}

void Replica::ProcessStateTransferRequest(
    const StateTransferRequest& stateTransferRequest) {
  VLOG(3) << "stateTransferRequest from Replica-"
          << stateTransferRequest.replicaid() << "\t||"
          << stateTransferRequest.logbegin() << "\t"
          << stateTransferRequest.logend() << "\tisSynced "
          << stateTransferRequest.issynced()
          << " view=" << stateTransferRequest.view();

  if (stateTransferRequest.view() != viewId_) {
    if (stateTransferRequest.view() > viewId_) {
      VLOG(2) << "InitiateViewChange-5";
      InitiateViewChange(stateTransferRequest.view());
    }
    return;
  }
  StateTransferReply reply;
  CrashVectorStruct* cv = crashVectorInUse_[0].load();
  const Address* requesterAddr =
      masterReceiver_[stateTransferRequest.replicaid()];
  reply.set_replicaid(replicaId_);
  reply.set_view(viewId_);
  reply.mutable_cv()->Add(cv->cv_.begin(), cv->cv_.end());
  reply.set_issynced(stateTransferRequest.issynced());

  if (reply.issynced()) {
    reply.set_logbegin(stateTransferRequest.logbegin());
    ASSERT(maxSyncedLogEntry_.load()->logId >= stateTransferRequest.logend());
    for (uint32_t j = stateTransferRequest.logbegin();
         j <= stateTransferRequest.logend(); j++) {
      LogEntry* entry = syncedLogEntryByLogId_.get(j);
      if (entry) {
        RequestBodyToMessage(entry->body, reply.add_reqs());
        reply.set_logend(j);
      } else {
        LOG(WARNING) << "Maybe just due to lag "
                     << stateTransferRequest.logend() << ">" << reply.logend();
        break;
      }
    }
    VLOG(3) << "State Reply " << reply.logbegin() << "--" << reply.logend();
  } else {
    reply.set_logbegin(stateTransferRequest.logbegin());
    reply.set_logend(stateTransferRequest.logend());
    ASSERT(filteredUnSyncedEntries_.size() > reply.logend());
    for (uint32_t j = reply.logbegin(); j <= reply.logend(); j++) {
      LogEntry* entry = filteredUnSyncedEntries_[j];
      ASSERT(entry != NULL);
      RequestBodyToMessage(entry->body, reply.add_reqs());
    }
    VLOG(3) << "Give " << reply.logbegin() << "-" << reply.logend();
  }
  if (reply.reqs_size() > 0) {
    masterContext_->endPoint_->SendMsgTo(*requesterAddr, reply,
                                         MessageType::STATE_TRANSFER_REPLY);
  }
}

void Replica::ProcessStateTransferReply(
    const StateTransferReply& stateTransferReply) {
  VLOG(3) << "Receive some state " << stateTransferReply.logbegin() << "--"
          << stateTransferReply.logend()
          << " view=" << stateTransferReply.view() << "--- "
          << transferSyncedEntry_ << "==" << stateTransferReply.issynced();
  if (status_ == ReplicaStatus::NORMAL) {
    // Normal replicas do not need state transfer
    return;
  }
  if (!CheckCV(stateTransferReply.replicaid(), stateTransferReply.cv())) {
    return;
  } else {
    Aggregated(stateTransferReply.cv());
  }

  if (!(masterContext_->endPoint_->isTimerRegistered(stateTransferTimer_))) {
    // We are not doing state transfer, so ignore this message
    return;
  }

  if (stateTransferReply.view() < viewId_) {
    // Old view: ignore
    return;
  } else if (stateTransferReply.view() > viewId_) {
    masterContext_->endPoint_->UnRegisterTimer(stateTransferTimer_);
    if (status_ == ReplicaStatus::RECOVERING) {
      // This state transfer is useless, stop it and restart recovery request
      masterContext_->endPoint_->RegisterTimer(recoveryRequestTimer_);
    } else if (status_ == ReplicaStatus::VIEWCHANGE) {
      VLOG(2) << "InitiateViewChange-6";
      InitiateViewChange(stateTransferReply.view());
    } else {
      LOG(ERROR) << "Unknown replica status " << (uint32_t)status_;
    }
    return;
  }

  // Else: Same view
  if (transferSyncedEntry_ != stateTransferReply.issynced()) {
    return;
  }

  const auto& iter = stateTransferIndices_.find(stateTransferReply.replicaid());
  if (iter == stateTransferIndices_.end() ||
      stateTransferReply.logend() < iter->second.first) {
    // We do not need these log entries
    return;
  }

  // So long as the state transfer is making progress, we should give it more
  // time instead of early termination
  // Only if the state transfer has not made progress within
  // stateTransferTimeout_. then we terminate it and rollback to some previous
  // function
  stateTransferTerminateTime_ =
      GetMicrosecondTimestamp() + +stateTransferTimeout_ * 1000;
  SHA_HASH dummy;
  if (stateTransferReply.issynced()) {
    // This is the state-transfer for synced requests
    for (uint32_t i = iter->second.first; i <= stateTransferReply.logend();
         i++) {
      const RequestBodyMsg& rbMsg =
          stateTransferReply.reqs(i - iter->second.first);
      LogEntry* entry = new LogEntry(
          rbMsg.deadline(), rbMsg.reqkey(), rbMsg.key(), rbMsg.proxyid(),
          rbMsg.command(), rbMsg.iswrite(), dummy, dummy);
      ProcessRequest(entry, true, false, false);
      // LOG(INFO) << "Processed " << entry->logId << "\t"
      //           << maxSyncedLogEntry_.load()->logId;
      // Register
      if (syncedLogEntryByReqKey_.get(entry->body.reqKey) == NULL) {
        syncedLogEntryByReqKey_.assign(entry->body.reqKey, entry);
        syncedLogEntryByLogId_.assign(entry->logId, entry);
        if (entry->logId > CONCURRENT_MAP_START_INDEX) {
          ASSERT(syncedLogEntryByLogId_.get(entry->logId - 1) != NULL);
          ASSERT(syncedLogEntryByLogId_.get(entry->logId - 1) == entry->prev);
        }
      }
    }
  } else {
    // This is the state-transfer for unsynced request (log merge)
    for (int i = 0; i < stateTransferReply.reqs_size(); i++) {
      const RequestBodyMsg& rbMsg = stateTransferReply.reqs(i);
      std::pair<uint64_t, uint64_t> key(rbMsg.deadline(), rbMsg.reqkey());
      if (requestsToMerge_.find(key) != requestsToMerge_.end()) {
        LogEntry* entry = new LogEntry(
            rbMsg.deadline(), rbMsg.reqkey(), rbMsg.key(), rbMsg.proxyid(),
            rbMsg.command(), rbMsg.iswrite(), dummy, dummy);

        requestsToMerge_[key] = {entry, 1};
      } else {
        requestsToMerge_[key].second++;
      }
    }
  }

  iter->second.first = stateTransferReply.logend() + 1;
  VLOG(2) << "Transfer Synced? " << stateTransferReply.issynced() << "\t"
          << " In Progress: " << iter->first << ":" << iter->second.first << "-"
          << iter->second.second;

  uint32_t remainingPercent =
      stateTransferIndicesRef_[stateTransferReply.replicaid()].second;
  if (remainingPercent > 10) {
    uint32_t previousGap =
        stateTransferIndicesRef_[stateTransferReply.replicaid()].first;
    uint32_t remainingGap = iter->second.second - iter->second.first;
    if (remainingGap * 100 / previousGap < remainingPercent) {
      LOG(INFO) << "State Tranfer from Replica "
                << stateTransferReply.replicaid() << "\t" << remainingPercent
                << "\% of progress (i.e., " << remainingGap
                << " logs) remaining\t"
                << "Current committedLogId_=" << committedLogId_
                << "\tmaxSyncedLogId=" << maxSyncedLogEntry_.load()->logId;
      ;
      stateTransferIndicesRef_[stateTransferReply.replicaid()].second -= 10;
    }
  }

  if (iter->second.first > iter->second.second) {
    // We have completed the state transfer for this target replica
    stateTransferIndices_.erase(iter->first);
  }

  if (stateTransferIndices_.empty()) {
    // This state transfer is completed, unregister the timer
    masterContext_->endPoint_->UnRegisterTimer(stateTransferTimer_);
    stateTransferIndices_.clear();
    stateTransferIndicesRef_.clear();
    // If we have a callback, then call it
    if (stateTransferCallback_) {
      stateTransferCallback_();
    }
  }
}

void Replica::RewindSyncedLogTo(uint32_t rewindPoint) {
  LOG(INFO) << "maxSyncedLogId_=" << maxSyncedLogEntry_.load()->logId << "\t"
            << "rewindPoint=" << rewindPoint;
  LogEntry* entryStart = maxSyncedLogEntry_;
  while (entryStart->logId > rewindPoint) {
    LogEntry* entryToDel = entryStart;
    if (entryToDel->prevNonCommutative) {
      entryToDel->prevNonCommutative->nextNonCommutative = NULL;
    }
    if (entryToDel->prev) {
      entryToDel->prev->next = NULL;
    }
    ASSERT(entryStart->prev != NULL);
    syncedLogEntryByReqKey_.erase(entryToDel->body.reqKey);
    syncedLogEntryByLogId_.erase(entryToDel->logId);
    entryStart = entryStart->prev;
    delete entryToDel;
  }
  entryStart->next = NULL;
  entryStart->nextNonCommutative = NULL;
  maxSyncedLogEntry_ = entryStart;
  trackedEntry_.assign(trackedEntry_.size(), maxSyncedLogEntry_);
}

void Replica::ProcessStartView(const StartView& startView) {
  VLOG(3) << startView.DebugString();

  if (!CheckCV(startView.replicaid(), startView.cv())) {
    return;
  } else {
    Aggregated(startView.cv());
  }

  if (status_ == ReplicaStatus::VIEWCHANGE) {
    if (startView.view() > viewId_) {
      VLOG(2) << "InitiateViewChange-7";
      InitiateViewChange(startView.view());
    } else if (startView.view() == viewId_) {
      if (committedLogId_ < startView.syncedlogid()) {
        // Start StateTransfer
        if (masterContext_->endPoint_->isTimerRegistered(stateTransferTimer_)) {
          // LOG(INFO) << "StateTransfer In Progress:"
          //           << stateTransferIndices_[startView.replicaid()].first
          //           << "--"
          //           << stateTransferIndices_[startView.replicaid()].second;
          return;
        }
        RewindSyncedLogTo(committedLogId_);
        stateTransferIndices_.clear();
        stateTransferIndicesRef_[startView.replicaid()] = {committedLogId_ + 1,
                                                           100};
        stateTransferIndices_[startView.replicaid()] = {
            committedLogId_ + 1, startView.syncedlogid()};
        stateTransferIndicesRef_[startView.replicaid()] = {committedLogId_ + 1,
                                                           100};
        stateTransferCallback_ = std::bind(&Replica::EnterNewView, this);
        stateTransferTerminateTime_ =
            GetMicrosecondTimestamp() + stateTransferTimeout_ * 1000;
        stateTransferTerminateCallback_ =
            std::bind(&Replica::RollbackToViewChange, this);

        transferSyncedEntry_ = true;
        masterContext_->endPoint_->RegisterTimer(stateTransferTimer_);
      } else {
        RewindSyncedLogTo(committedLogId_);
        EnterNewView();
      }

    }  // else: startView.view()<viewId_, old message, ignore it
  } else if (status_ == ReplicaStatus::NORMAL) {
    if (startView.view() > viewId_) {
      VLOG(2) << "InitiateViewChange-8";
      InitiateViewChange(startView.view());
    } else if (startView.view() < viewId_) {
      // My view is fresher
      SendStartView(startView.replicaid());
    }
    // Else: We are in the same view and this replica is normal, no need
    // startView
  }
  // If status == RECOVERING, it does not participate in view change
}

void Replica::BroadcastCrashVectorRequest() {
  CrashVectorRequest request;
  boost::uuids::random_generator generator;
  boost::uuids::uuid uuid = generator();
  nonce_ = boost::uuids::to_string(uuid);
  request.set_nonce(nonce_);
  request.set_replicaid(replicaId_);
  crashVectorReplySet_.clear();
  for (uint32_t i = 0; i < replicaNum_; i++) {
    if (i == replicaId_) {
      continue;
    }
    LOG(INFO) << "Ask CrashVector to Replica " << i;
    masterContext_->endPoint_->SendMsgTo(*(masterReceiver_[i]), request,
                                         MessageType::CRASH_VECTOR_REQUEST);
  }
}

void Replica::BroadcastRecoveryRequest() {
  RecoveryRequest request;
  CrashVectorStruct* cv = crashVectorInUse_[0].load();
  request.mutable_cv()->Add(cv->cv_.begin(), cv->cv_.end());
  request.set_replicaid(replicaId_);
  for (uint32_t i = 0; i < replicaNum_; i++) {
    if (i == replicaId_) {
      continue;
    }
    masterContext_->endPoint_->SendMsgTo(*(masterReceiver_[i]), request,
                                         MessageType::RECOVERY_REQUEST);
  }
}

void Replica::ProcessCrashVectorRequest(const CrashVectorRequest& request) {
  if (status_ != ReplicaStatus::NORMAL) {
    return;
  }

  CrashVectorReply reply;
  reply.set_nonce(request.nonce());
  reply.set_replicaid(replicaId_);
  CrashVectorStruct* cv = crashVectorInUse_[0].load();
  reply.mutable_cv()->Add(cv->cv_.begin(), cv->cv_.end());
  masterContext_->endPoint_->SendMsgTo(*(masterReceiver_[request.replicaid()]),
                                       reply, MessageType::CRASH_VECTOR_REPLY);
}

void Replica::ProcessCrashVectorReply(const CrashVectorReply& reply) {
  if (status_ != ReplicaStatus::RECOVERING) {
    LOG(INFO) << "nolong Recovering " << status_;
    return;
  }

  if (nonce_ != reply.nonce()) {
    LOG(INFO) << "nonce inconistent " << crashVectorReplySet_.size();
    return;
  }

  if (masterContext_->endPoint_->isTimerRegistered(crashVectorRequestTimer_) ==
      false) {
    // We no longer request crash vectors
    LOG(INFO) << "no longer register crashVectorRequest "
              << crashVectorReplySet_.size();
    return;
  }

  crashVectorReplySet_[reply.replicaid()] = reply;

  if (crashVectorReplySet_.size() >= replicaNum_ / 2 + 1) {
    // Got enough quorum
    CrashVectorStruct* oldCV = crashVectorInUse_[0].load();
    CrashVectorStruct* newCV = new CrashVectorStruct(*oldCV);
    newCV->version_++;
    for (const auto& kv : crashVectorReplySet_) {
      for (uint32_t i = 0; i < replicaNum_; i++) {
        if (kv.second.cv(i) > newCV->cv_[i]) {
          newCV->cv_[i] = kv.second.cv(i);
        }
      }
    }
    // Increment self counter
    newCV->cv_[replicaId_]++;
    crashVector_.assign(newCV->version_, newCV);
    for (uint32_t i = 0; i < crashVectorVecSize_; i++) {
      crashVectorInUse_[i] = newCV;
    }
    masterContext_->endPoint_->UnRegisterTimer(crashVectorRequestTimer_);
    crashVectorReplySet_.clear();

    // Start Recovery Request
    masterContext_->endPoint_->RegisterTimer(recoveryRequestTimer_);
  }
}

void Replica::ProcessRecoveryRequest(const RecoveryRequest& request) {
  if (status_ != ReplicaStatus::NORMAL) {
    return;
  }

  if (!CheckCV(request.replicaid(), request.cv())) {
    return;
  } else {
    Aggregated(request.cv());
  }

  RecoveryReply reply;
  CrashVectorStruct* cv = crashVectorInUse_[0].load();
  reply.set_replicaid(replicaId_);
  reply.set_view(viewId_);
  reply.mutable_cv()->Add(cv->cv_.begin(), cv->cv_.end());
  reply.set_syncedlogid(maxSyncedLogEntry_.load()->logId);
  masterContext_->endPoint_->SendMsgTo(*(masterReceiver_[request.replicaid()]),
                                       reply, MessageType::RECOVERY_REPLY);
}

void Replica::ProcessRecoveryReply(const RecoveryReply& reply) {
  if (!CheckCV(reply.replicaid(), reply.cv())) {
    return;
  } else {
    if (Aggregated(reply.cv())) {
      // If cv is updated, then it is likely that some messages in
      // recoveryReplySet_ become stray, so remove them
      for (uint32_t i = 0; i < replicaNum_; i++) {
        auto iter = recoveryReplySet_.find(i);
        if (iter != recoveryReplySet_.end() &&
            (!CheckCV(i, iter->second.cv()))) {
          recoveryReplySet_.erase(i);
        }
      }
    }
  }

  if (masterContext_->endPoint_->isTimerRegistered(recoveryRequestTimer_) ==
      false) {
    // We no longer request recovery reply
    return;
  }
  recoveryReplySet_[reply.replicaid()] = reply;
  if (recoveryReplySet_.size() >= replicaNum_ / 2 + 1) {
    // Got enough quorum
    masterContext_->endPoint_->UnRegisterTimer(recoveryRequestTimer_);
    uint32_t maxView = 0;
    uint32_t syncedLogId = 0;
    for (const auto& kv : recoveryReplySet_) {
      if (kv.second.view() > maxView) {
        maxView = kv.second.view();
        syncedLogId = kv.second.syncedlogid();
      }
    }
    // Get the maxView, launch state transfer with the corresponding leader
    viewId_ = maxView;
    recoveryReplySet_.clear();
    LOG(INFO) << "Replica intends to enter View " << viewId_
              << " after recovery; the number of logs to recover is:"
              << syncedLogId;
    if (AmLeader()) {
      LOG(INFO) << "The recovered replica will become the leader in this view, "
                   "skip it!";
      // If the recoverying replica happens to be the leader of the new view,
      // don't participate. Wait until the healthy replicas elect a new leader
      usleep(1000);  // sleep some time and restart the recovery process
      masterContext_->endPoint_->RegisterTimer(recoveryRequestTimer_);
    } else {
      // Launch state transfer for synced log entries
      stateTransferIndices_.clear();
      if (syncedLogId >= CONCURRENT_MAP_START_INDEX) {
        // There are some synced log entries that should be transferred
        transferSyncedEntry_ = true;
        stateTransferIndices_[maxView % replicaNum_] = {
            CONCURRENT_MAP_START_INDEX, syncedLogId};

        stateTransferIndicesRef_[maxView % replicaNum_] = {
            syncedLogId - CONCURRENT_MAP_START_INDEX + 1, 100};
        LOG(INFO) << "Recover Logs from " << CONCURRENT_MAP_START_INDEX
                  << "\t to\t" << syncedLogId;
        stateTransferCallback_ = std::bind(&Replica::EnterNewView, this);
        stateTransferTerminateTime_ =
            GetMicrosecondTimestamp() + stateTransferTimeout_ * 1000;
        stateTransferTerminateCallback_ =
            std::bind(&Replica::RollbackToRecovery, this);
        masterContext_->endPoint_->RegisterTimer(stateTransferTimer_);
      } else {
        // No log entries to recover, directly enter new view
        EnterNewView();
      }
    }
  }
}

void Replica::ProcessSyncStatusReport(const SyncStatusReport& report) {
  if (!CheckCV(report.replicaid(), report.cv())) {
    // Stray message
    return;
  } else {
    if (Aggregated(report.cv())) {
      // Possibly make existing msg become stray
      for (uint32_t i = 0; i < replicaId_; i++) {
        auto iter = syncStatusSet_.find(i);
        if (iter != syncStatusSet_.end() && (!CheckCV(i, iter->second.cv()))) {
          syncStatusSet_.erase(i);
        }
      }
    }
  }

  if (!CheckView(report.view())) {
    return;
  }

  auto iter = syncStatusSet_.find(report.replicaid());
  if (iter == syncStatusSet_.end() ||
      iter->second.syncedlogid() < report.syncedlogid()) {
    syncStatusSet_[report.replicaid()] = report;
  }

  // LOG(INFO) << "sync size=" << syncStatusSet_.size();
  if (syncStatusSet_.size() >= replicaNum_ / 2 + 1) {
    uint32_t minLogId = UINT32_MAX;
    for (const auto& kv : syncStatusSet_) {
      if (minLogId > kv.second.syncedlogid()) {
        minLogId = kv.second.syncedlogid();
      }
    }
    // LOG(INFO) << "minLogId=" << minLogId << "\t" << committedLogId_;
    if (minLogId >= committedLogId_) {
      committedLogId_ = minLogId;
      // LOG(INFO) << "syncStauts " << report.DebugString();
      SendCommit();
    }
  }
}

void Replica::ProcessCommitInstruction(const CommitInstruction& commit) {
  if (!CheckCV(commit.replicaid(), commit.cv())) {
    return;
  } else {
    Aggregated(commit.cv());
  }
  if (!CheckView(commit.view())) {
    return;
  }

  lastHeartBeatTime_ = GetMicrosecondTimestamp();
  // LOG(INFO) << "commit " << commit.DebugString();
  // Buggy: should compare with syncedLogId, to see whether log is missing
  if (commit.committedlogid() > committedLogId_) {
    // Don't assign committedLogId_ directly, because this replica may have
    // not get enough synced logs
    toCommitLogId_ = commit.committedlogid();
    // LOG(INFO) << "committedLogId_=" << committedLogId_;
  }

  uint32_t nextCommitId = maxSyncedLogEntry_.load()->logId;
  if (toCommitLogId_ < nextCommitId) {
    nextCommitId = toCommitLogId_;
  }
  while (committedLogId_ < nextCommitId) {
    if (committedLogId_ < CONCURRENT_MAP_START_INDEX) {
      committedLogId_++;
      continue;
    }
    uint32_t preFetchTrackedLogId = trackedEntry_[0]->logId;
    LogEntry* entry = syncedLogEntryByLogId_.get(committedLogId_);
    if (entry == NULL) {
      if (committedLogId_ <= preFetchTrackedLogId) {
        LOG(INFO) << "committedLogId_=" << committedLogId_ << "\t"
                  << "maxSyncedLogId_=" << maxSyncedLogEntry_.load()->logId
                  << "\ttrackedLogId =" << preFetchTrackedLogId;
        for (uint32_t i = CONCURRENT_MAP_START_INDEX;
             i <= trackedEntry_[0]->logId; i++) {
          if (syncedLogEntryByLogId_.get(i) == NULL) {
            LOG(INFO) << "log " << i << " not recorded";
          }
        }
        LOG(ERROR) << "abnormal exit";
        exit(0);
      }
      if (viewId_ == 1) {
        LOG(INFO) << "committedLogId_=" << committedLogId_ << "\t"
                  << "maxSyncedLogId_=" << maxSyncedLogEntry_.load()->logId
                  << "\t"
                  << "\ttrackedLogId =" << trackedEntry_[0]->logId;
      }

      break;
    }
    ASSERT(entry != NULL);
    entry->result = ApplicationExecute(entry->body);
    committedLogId_++;
    // if (committedLogId_ % 1000 == 0) {
    //   LOG(INFO) << "committedLogId_=" << committedLogId_ << "\t"
    //             << "maxSyncedLogId_=" << maxSyncedLogEntry_.load()->logId;
    // }
  }
}

bool Replica::CheckView(const uint32_t view, const bool isMaster) {
  if (view < viewId_) {
    // old message
    return false;
  }
  if (view > viewId_) {
    if (isMaster) {
      if (status_ != ReplicaStatus::RECOVERING) {
        // Recovering replicas do not participate in view change
        VLOG(2) << "InitiateViewChange-9: " << view
                << "\t currentView=" << viewId_ << "\t"
                << "td=" << pthread_self();
        InitiateViewChange(view);
      }
    } else {
      // new view, update status and wait for master thread to handle the
      // situation
      status_ = ReplicaStatus::VIEWCHANGE;
    }

    return false;
  }
  return true;
}

bool Replica::CheckCV(const uint32_t senderId,
                      const google::protobuf::RepeatedField<uint32_t>& cv) {
  CrashVectorStruct* masterCV = crashVectorInUse_[0].load();
  return (cv.at(senderId) >= masterCV->cv_[senderId]);
}

bool Replica::Aggregated(const google::protobuf::RepeatedField<uint32_t>& cv) {
  CrashVectorStruct* masterCV = crashVectorInUse_[0].load();
  std::vector<uint32_t> maxCV(masterCV->cv_);
  bool needAggregate = false;
  for (uint32_t i = 0; i < replicaNum_; i++) {
    if (maxCV[i] < cv.at(i)) {
      // The incoming cv has fresher elements
      needAggregate = true;
      maxCV[i] = cv.at(i);
    }
  }
  if (needAggregate) {
    CrashVectorStruct* newCV =
        new CrashVectorStruct(maxCV, masterCV->version_ + 1);
    crashVector_.assign(newCV->version_, newCV);
    crashVectorInUse_[0] = newCV;
    if (status_ == ReplicaStatus::NORMAL) {
      // Wait until the reply threads has known the new cv
      while (true) {
        bool ready = true;
        for (uint32_t i = 1; i <= fastReplyQu_.size(); i++) {
          if (crashVectorInUse_[i].load()->version_ < newCV->version_) {
            ready = false;
          }
        }
        if (ready) {
          break;
        } else {
          usleep(1000);
        }
      }
    }  // Else (status_=ViewChange), then there is only master thread alive,
       // no need to wait for reply thread
  }
  return needAggregate;
}

void Replica::RollbackToViewChange() {
  LOG(INFO) << "Rollback to restart view change";
  status_ = ReplicaStatus::VIEWCHANGE;
  viewChangeSet_.clear();
  if (false == masterContext_->endPoint_->isTimerRegistered(viewChangeTimer_)) {
    masterContext_->endPoint_->RegisterTimer(viewChangeTimer_);
  }
}

void Replica::RollbackToRecovery() {
  LOG(INFO) << "Rollback to restart recovery";
  status_ = ReplicaStatus::RECOVERING;
  recoveryReplySet_.clear();
  // Since we start a new round of recovery, the logs obtained from the
  // previous round (if any) will not count. Delete them (=clean state) and
  // restart
  LogEntry* entryStart = syncedLogEntryHead_->next;
  while (entryStart) {
    LogEntry* entryToDel = entryStart;
    entryStart = entryStart->next;
    delete entryToDel;
  }
  maxSyncedLogEntry_ = syncedLogEntryHead_;
  maxSyncedLogEntryByKey_.assign(keyNum_, NULL);

  if (false ==
      masterContext_->endPoint_->isTimerRegistered(recoveryRequestTimer_)) {
    masterContext_->endPoint_->RegisterTimer(recoveryRequestTimer_);
  }
}

std::string Replica::ApplicationExecute(const RequestBody& request) {
  return "";
}

bool Replica::AmLeader() { return (viewId_ % replicaNum_ == replicaId_); }

}  // namespace nezha
