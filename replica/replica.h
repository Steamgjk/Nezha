#ifndef NEZHA_REPLICA_H
#define NEZHA_REPLICA_H

#include <yaml-cpp/yaml.h>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <condition_variable>
#include <fstream>
#include "lib/utils.h"
#include "proto/nezha_proto.pb.h"
#include "replica_config.h"

namespace nezha {
using namespace nezha::proto;
/** Receiver is more complex than sender. A sender only needs an endpoint.
 * But A Receiver needs an endpoint (endPoint_) to receive messages, and the
 * message should be handled bu an already-registered handler (msgHandlerFunc_).
 * Besides, in order to unblock the endpoint during view change, there is also a
 *  timer (monitorTimer_) needed, to keep monitor the status of the replica.
 *
 * We package all the necessary components into ReceiverContext for brievity
 */
struct ReceiverContext {
  Endpoint* endPoint_;
  void* context_;
  MessageHandlerFunc msgHandlerFunc_;
  Timer* monitorTimer_;
  ReceiverContext(Endpoint* ep = NULL, void* ctx = NULL,
                  MessageHandlerFunc msgFunc = nullptr, Timer* t = NULL)
      : endPoint_(ep),
        context_(ctx),
        msgHandlerFunc_(msgFunc),
        monitorTimer_(t) {}
  void Register(int endpointType = EndpointType::UDP_ENDPOINT) {
    if (endpointType == EndpointType::UDP_ENDPOINT) {
      // UDP Endpoint
      UDPMsgHandler* udpMsgHandler =
          new UDPMsgHandler(msgHandlerFunc_, context_);
      ((UDPSocketEndpoint*)endPoint_)->RegisterMsgHandler(udpMsgHandler);
      ((UDPSocketEndpoint*)endPoint_)->RegisterTimer(monitorTimer_);
    } else {
      // To support other types of endpoints later
      LOG(ERROR) << "unknown endpoint type " << (int)endpointType;
    }
  }
};

/**
 * Refer to replica_run.cc, the runnable program only needs to instantiate a
 * Replica object with a configuration file. Then it calls Run() method to run
 * and calls Terminate() method to stop
 */

class Replica {
 private:
  /** All the configuration parameters for the replica are included in
   * replicaConfig_*/
  ReplicaConfig replicaConfig_;
  /** 1 for UDP, 2 for GRPC (not supported yet) */
  int endPointType_;
  /** viewId_ starts from 0 */
  std::atomic<uint32_t> viewId_;
  std::atomic<uint32_t> lastNormalView_;
  /** replicaId_ starts from 0 */
  std::atomic<uint32_t> replicaId_;
  std::atomic<uint32_t> replicaNum_;

  /** Worker threads check status_ to decide whether they should be blocked (for
   * view change) */
  std::atomic<char> status_;

  /** Every unique request, sharded across several maps for concurrency.
   * Before a request is processed, it is addded to one of these maps by
   * recordThread. Map from reqKey -> logEntry */
  std::vector<ConcurrentMap<uint64_t, LogEntry*>> recordMap_;

  /** TrackThread traverses the synced log list and record in
   * syncedLogEntryByReqKey_ and syncedLogEntryByLogId_ */
  std::vector<LogEntry*> trackedEntry_;

  /** earlyBuffer_ uses the pair <deadline, reqKey> as key. std::map will sort
   * them in ascending order by default */
  std::map<std::pair<uint64_t, uint64_t>, LogEntry*> earlyBuffer_;

  /** lastReleasedEntryByKeys_ is used to support communativity, we record the
   * last relased entry for each key. When new requests come, it compares with
   * the last released entry in the same key */
  std::vector<std::pair<uint64_t, uint64_t>> lastReleasedEntryByKeys_;

  /**  keyNum_ indicates the number of keys that requests will work on (to
   * support commutativity optimization). We assume one request will only work
   * on one key */
  uint32_t keyNum_;

  /**
   * Log entries are organized as a list.
   * On the leader, it only needs to maintain one list, i.e., synced log list;
   * But on the follower, it maintains two lists, i.e., unsynced log list and
   * synced log list.
   *
   * syncedLogEntryHead_/unSyncedLogEntryHead_ are the starting point of the two
   * lists, which we crearte a dummy node for each list to serve as the head
   *
   * maxSyncedLogEntry_ and maxUnSyncedLogEntry_ are the tails of the two lists
   * respectively
   */
  LogEntry* syncedLogEntryHead_;
  LogEntry* unSyncedLogEntryHead_;
  std::atomic<LogEntry*> maxSyncedLogEntry_;
  std::atomic<LogEntry*> maxUnSyncedLogEntry_;
  /**
   * minUnSyncedLogEntry_ is initialized as unSyncedLogEntryHead_, but our
   * garbage-collection thread can advance it (TODO). In this way, it can
   * accelerate the generation of filteredUnSyncedEntries_
   */
  LogEntry* minUnSyncedLogEntry_;

  /**
   * These three vecs can be cosnidered as finer-grained version of
   * maxSyncedLogEntry_,maxUnSyncedLogEntry_ and minUnSyncedLogEntry_.
   * They are mainly used to support commutativity optimization.
   *
   * maxSyncedLogEntryByKey_ and minUnSyncedLogEntryByKey_ combine to work as
   * the sync-point, as illustrated in Figure 5 of our paper.
   */
  std::vector<LogEntry*> maxSyncedLogEntryByKey_;
  std::vector<LogEntry*> maxUnSyncedLogEntryByKey_;
  std::vector<LogEntry*> minUnSyncedLogEntryByKey_;

  /** Index Map, facilate for entry look-up */
  ConcurrentMap<uint64_t, LogEntry*> syncedLogEntryByReqKey_;
  ConcurrentMap<uint32_t, LogEntry*> syncedLogEntryByLogId_;

  /** Each thread is given a unique name (key) and stored in the pool */
  std::map<std::string, std::thread*> threadPool_;

  /** committedLogId_ and toCommitLogId_ are used for peridical synchronization
   * (to accelerate failure recovery) */
  std::atomic<uint32_t> committedLogId_;
  std::atomic<uint32_t> toCommitLogId_;

  /** Context (including a message handler and a monitor timer) */
  ReceiverContext* masterContext_;
  std::vector<ReceiverContext*> requestContext_;
  ReceiverContext* indexSyncContext_;
  ReceiverContext* missedIndexAckContext_;
  ReceiverContext* missedReqAckContext_;

  /** Timers
   *
   * Since message can be dropped after it is sent. For those messages which are
   * required to be eventually delivered, we register a timer to the endpoint,
   * which keeps sending the message, until the sender knows it is
   * delivered and unregister the timer
   */
  Timer* heartbeatCheckTimer_;
  Timer* indexAskTimer_;
  Timer* requestAskTimer_;
  Timer* viewChangeTimer_;
  Timer* stateTransferTimer_;
  Timer* periodicSyncTimer_;
  Timer* crashVectorRequestTimer_;
  Timer* recoveryRequestTimer_;

  /** Endpoints
   *
   * These endpoints are only used as senders, so they do not need the complex
   * context struct as receivers
   */
  std::vector<Endpoint*> indexSender_;  // send indices (Sec 5.4)
  std::vector<Endpoint*> fastReplySender_;
  std::vector<Endpoint*> slowReplySender_;
  Endpoint* indexRequester_; /** In the slow path, when indices are missing,
                                Follower uses this endpoint to send requests
                                asking for the missing indices */
  Endpoint* reqRequester_;   /** Follower uses this endpoint to send requests
                                asking for the missed requests */
  Endpoint* indexAcker_; /** Leader uses this endpoint to reply the indices to
                            the requested followers */

  /** Addresses */
  std::vector<Address*>
      indexReceiver_; /** Leader will send indices to these addresses (each
                         follower has such an address to receive index) */
  std::vector<Address*>
      indexAskReceiver_; /** Follower sends ask-requests to these addresses
                            when it is missing some indices */
  std::vector<Address*>
      requestAskReceiver_; /** Followers send ask-requests to these addresses
                              when it is missing some requests */
  std::vector<Address*>
      masterReceiver_; /** Each replica maintains a master thread, which
                          sends/receives/processes different types of control
                          messages, therefore, each replica matains such an
                          address vector (size of replicaNum) to know the
                          address of others' master thread */

  /* Round robin indices are used to achieve load balance among threads of the
   * same functionality (e.g., multiple reply threads) */
  uint32_t roundRobinProcessIdx_;
  uint32_t roundRobinIndexAskIdx_;
  uint32_t roundRobinRequestAskIdx_;

  /** Version-based CrashVector (version number as the key), to facilitate
   * garbage-collection */
  ConcurrentMap<uint32_t, CrashVectorStruct*> crashVector_;
  /** Each related thread (i.e. fast reply threads + index recv thread + index
   * ack thread) will hold an atomic pointer, pointing to the crash vector they
   * are currently using.
   *
   * The garbage collect thread will check crashVectorInUse_ to decide which
   * CrashVectorStruct can be safely reclaimed.
   *  */
  std::atomic<CrashVectorStruct*>* crashVectorInUse_;
  /** The number of threads using crash vectors (i.e., the length of
   * crashVectorInUse_) */
  uint32_t crashVectorVecSize_;

  /** The sync messages (for index sync process) which have not been processed
   */
  std::map<std::pair<uint32_t, uint32_t>, IndexSync> pendingIndexSync_;
  /** Each key in missedReqKeys_ indicating a request is missing on this replica
   */
  std::set<uint64_t> missedReqKeys_;

  /** Each pair indicates a segment of indices is missing during index sync
   * process */
  std::pair<uint32_t, uint32_t> missedIndices_;

  /** The max number of indices/reqKeys/requests that can be carried in one
   * stateTransfer message */
  uint32_t indexTransferBatch_;
  uint32_t requestKeyTransferBatch_;
  uint32_t requestTrasnferBatch_;

  /* State-Transfer related variables **/
  uint64_t stateTransferTimeout_;
  bool transferSyncedEntry_;
  /** key: the target replica to ask for requests; value: the segment <begin,
   * end> of requests that will be transferred */
  std::map<uint32_t, std::pair<uint32_t, uint32_t>> stateTransferIndices_;
  std::map<uint32_t, std::pair<uint32_t, uint32_t>>
      stateTransferIndicesRef_;  // Only serves as the references
  std::function<void(void)> stateTransferCallback_;
  /** The max amount of time that the state transfer can last */
  std::uint64_t stateTransferTerminateTime_;
  /** If the state transfer cannot be completed within
   * stateTransferTerminateTime_, execute the following callback and terminate
   * the state transfer */
  std::function<void(void)> stateTransferTerminateCallback_;

  /** Before transfer unsynced logs, the replica needs to first filter all the
   * unsynced logs, because most of them overlap with synced logs, which has
   * already been transferred, so we only need to transfer a small portion of
   * unsynced logs after filtering out those overlapped ones  */
  std::vector<LogEntry*> filteredUnSyncedEntries_;

  /** During leader election, the new leader use requestsToMerge_ to merge logs
   * collected from the quorum of replicas.
   *
   * Key: <deadline, reqKey>; Value: <request, the number of remaining replicas
   * containing this request>  */
  std::map<std::pair<uint64_t, uint64_t>, std::pair<LogEntry*, uint32_t>>
      requestsToMerge_;

  // Recovery related variables
  std::string nonce_;
  /** Key: replicaId. These structuers are used to check whether a quorum has
   * been formed */
  std::map<uint32_t, CrashVectorReply> crashVectorReplySet_;
  std::map<uint32_t, RecoveryReply> recoveryReplySet_;
  std::map<uint32_t, ViewChange> viewChangeSet_;
  std::map<uint32_t, SyncStatusReport> syncStatusSet_;

  /** Inserted by ReceiveThread, and looked up by
   * FastReplyThread/SlowReplyThread */
  ConcurrentMap<uint64_t, Address*> proxyAddressMap_;

  /** Followers periodically check lastHeartBeatTime_ to decide whether it
   * should issue view change
   *
   * lastHeartBeatTime_ is updated every time the follower receives a heartbeat
   * message (i.e. IndexSync and CommitInstruction)
   *  */
  std::atomic<uint64_t> lastHeartBeatTime_;

  /** Tentative-- TODO: Add more explanation */
  uint64_t lastAskMissedIndexTime_;
  uint64_t lastAskMissedRequestTime_;
  std::unordered_map<uint64_t, uint64_t> askTimebyReqKey_;
  std::vector<uint64_t> fetchTime_;

  /** Replicas use it to check whether every worker thread has stopped */
  std::atomic<uint32_t> activeWorkerNum_;
  /** The total number of worker threads. When terminating, replicas use this
   * variable to detect whether every thread has been terminated and exited */
  uint32_t totalWorkerNum_;
  /** To implement blocking mechanism, see BlockWhenStatusIsNot function */
  std::condition_variable waitVar_;
  std::mutex waitMutext_;

  ConcurrentQueue<uint64_t> tagQu_;  // For Debug, will be deleted
  /** To communicate between ReceiveThread and ProcessThread */
  ConcurrentQueue<LogEntry*> processQu_;
  /** To communicate between ReceiveThread and RecordThread */
  std::vector<ConcurrentQueue<RequestBody*>> recordQu_;
  /** To communicate between IndexRecvThread and IndexProcessThread */
  ConcurrentQueue<std::pair<MessageHeader*, char*>> indexQu_;

  /** To communinicate between ProcessThread and FastReplyThread */
  std::vector<ConcurrentQueue<LogEntry*>> fastReplyQu_;
  /** To communinicate between ProcessThread and SlowReplyThread */
  std::vector<ConcurrentQueue<LogEntry*>> slowReplyQu_;
  /** To communicate between ReceiveThread and OWDCalcThread (Transmit <proxyId,
   * owd>)
   */
  ConcurrentQueue<std::pair<uint64_t, uint32_t>> owdQu_;
  /** Record the one-way delay for each proxy. Updated by OWDCalcThread, read by
   * FastReplyThread/SlowReplyThread */
  ConcurrentMap<uint64_t, uint32_t> owdMap_;
  /** To window size used to estimate one-way delay */
  uint32_t slidingWindowLen_;
  double movingPercentile_;
  std::map<uint64_t, std::vector<uint32_t>> slidingWindow_;  // <proxyid, vec>
  std::map<uint64_t, uint64_t> owdSampleNum_;

  /** Garbage-Collection related variables */
  uint32_t reclaimTimeout_;
  /** The old versions of crash vectors in crashVector_ that can be reclaimed */
  uint32_t cvVersionToClear_;
  /**  GarbageCollectThread use prepareToClearLateBufferLogId_ to tell
   * IndexSyncThread that it intends to clear the requests before this point
   * [included]  */
  std::atomic<uint32_t> prepareToClearLateBufferLogId_;
  /**  GarbageCollectThread use prepareToClearLateBufferLogId_ to tell
   * IndexSyncThread and FastReplyThread that it intends to clear the log
   * entries before this point [included]  */
  std::atomic<uint32_t> prepareToClearUnSyncedLogId_;
  /** IndexSyncThread use safeToClearLateBufferLogId_ to tell
   * GarbageCollectThread that it can safely clear the requests in late buffer
   * up to this point [included]
   */
  std::atomic<uint32_t> safeToClearLateBufferLogId_;
  /** FastReplyThread(s) and IndexSyncThread use these atomic variables to tell
   * GarbageCollectThread, telling that it can safely clear unsynced log entries
   * up to this point [included] */
  std::atomic<uint32_t>* safeToClearUnSyncedLogId_;

  /** Create/Initialize all the necessary variables, it is only called once
   * during the lifetime of the replica */
  void CreateContext();
  /** Launch all the threads, only called once during the lifetime of the
   * replica */
  void LaunchThreads();
  /** After a view change or recovery is completed, the replica enters a new
   * view*/
  void EnterNewView();

  /** View Change (recovery) related */
  /** Reset the necessary variables. It is called every time when we initiate a
   * view change, and this function is  much more lightweight than CreateContext
   */
  void ResetContext();
  void InitiateViewChange(const uint32_t view);
  /** Send ViewChangeRequest to every replica and send ViewChange to the
   * leader. Used to instantiate viewChangeTimer_*/
  void BroadcastViewChange();
  /** Send ViewChangeRequest to a specific replica */
  void SendViewChangeRequest(const int toReplicaId);
  /** Send ViewChange to the leader(i.e., whose replicaId = view % replicaNum)
   */
  void SendViewChange();
  /** A crashed replica needs to first call InitiateRecovery in order to join
   * the system */
  void InitiateRecovery();
  /** The RECOVERING replica asks every healthy replica for crash vector */
  void BroadcastCrashVectorRequest();
  /** The RECOVERING replica asks every healthy replica for necessary recovery
   * information (e.g., the current view, the synced logs on that replica) */
  void BroadcastRecoveryRequest();
  /** The new leader, after fully recovery, send StartView to others */
  void SendStartView(const int toReplicaId);
  /** Replicas use state transfer to retrieve (large number of) log entries from
   * others. Used to instantiate stateTransferTimer_ */
  void SendStateTransferRequest();
  /** If the view change process takes too long and cannot be completed (this
   * can happen when the leader in the new view also fails), the replica will
   * terminate the current view change process and starts a new view change with
   * higher viewId */
  void RollbackToViewChange();
  /** If the recovery process takes too long and cannot be completed (this can
   * happen when the RECOVERING replica happens to be the leader in the new
   * view), this replica will terminate the in-progress recovery and starts a
   * new round of recovery, after the healthy replicas have elected a new leader
   among themseleves */
  void RollbackToRecovery();
  /** During view change, replicas may have some uncommitted requests, which
   * will not show in the new view, so replicas will rewind log list and
   * eliminate those uncommitted onces, and appended with the committed entries
   * from the leader
   */
  void RewindSyncedLogTo(uint32_t rewindPoint);

  /** Periodic Sync related */
  /** Followers periodically report their sync-point to the leader, so the
   * leader can decide the commit-point.
   * Used to instantiate periodicSyncTimer_ */
  void SendSyncStatusReport();
  /** Leader send commit-point to followers, so followers can safely execute the
  log entries up to commit-point. This is very useful to accelerate view change
  after the leader fails (details in  para. ``Acceleration of Recovery'' of Sec
  6 of our paper) */
  void SendCommit();

  /** Garbage-Collect related */
  /** If the logs (on the followers) have not been added into synced log list
   * and has been stayed on the replica for too long, then the garbage-collect
   * (gc) thread will reclaim it and free its memory */
  void ReclaimStaleLogs();
  void PrepareNextReclaim();
  /** If the crashVectorStruct is no longer used by any thread on this replica,
   * the gc-thread collects it */
  void ReclaimStaleCrashVector();

  /** Message handler */
  bool ProcessIndexSync(const IndexSync& idxSyncMsg);
  void ProcessViewChangeReq(const ViewChangeRequest& viewChangeReq);
  void ProcessViewChange(const ViewChange& viewChange);
  void ProcessStateTransferRequest(
      const StateTransferRequest& stateTransferReq);
  void ProcessStateTransferReply(const StateTransferReply& stateTransferRep);
  void ProcessStartView(const StartView& startView);
  void ProcessCrashVectorRequest(const CrashVectorRequest& request);
  void ProcessCrashVectorReply(const CrashVectorReply& reply);
  void ProcessRecoveryRequest(const RecoveryRequest& request);
  void ProcessRecoveryReply(const RecoveryReply& reply);
  void ProcessSyncStatusReport(const SyncStatusReport& report);
  void ProcessCommitInstruction(const CommitInstruction& commit);
  void ProcessRequest(LogEntry* rb, const bool isSyncedReq = true,
                      const bool sendReply = true,
                      const bool canExecute = false);

  /** The interfaces to bridge specific applications with Nezha */
  std::string ApplicationExecute(const RequestBody& request);

  /** Tools */
  /** Check whether this replica is leader, return true if it is */
  bool AmLeader();
  /** During view change, BlockWhenStatusIsNot uses the conditional variable
   * (waitVar_) to block the worker threads. Finally only the master thread is
   * alive, so that it can run the related procedure without risks of data race
   */
  void BlockWhenStatusIsNot(char targetStatus);

  /**
   * CheckView returns true if the message's view (Parameter-1) is consistent
   * with the replica's current view
   *
   * Master thread (isMaster) can initiate view change, non-master threads only
   * switch status to ViewChange  */
  bool CheckView(const uint32_t view, const bool isMaster = true);

  /** CheckCV checks the crashVector to decide whether the incoming message is
   * stray message. It returns true if the cv is valid (i.e., the message is not
   * stray message) */
  bool CheckCV(const uint32_t senderId,
               const google::protobuf::RepeatedField<uint32_t>& cv);

  /** Check whether the incoming message's crash vector (the passed-in cv) will
   * lead to the update of replica's crashVector (i.e., crashVector_[0]).
   * If it needs aggregation, this function will aggreate it and return true */
  bool Aggregated(const google::protobuf::RepeatedField<uint32_t>& cv);

  /**
   * During state transfer, the log transfer are divided into two parts, synced
   * log transfer and unsynced log transfer. Which are undertaken by the
   * following two functions
   */
  void TransferSyncedLog();
  void TransferUnSyncedLog();

  /**
   * After enough unsynced logs have been collected by the leader, the leader
   * merges the unsynced logs to deice which logs can be includec in the
   * newly-built log list (details in Sec 6 and Appendix A.3 of our paper)
   */
  void MergeUnSyncedLog();

  /** Convert our self-defined message to proto message */
  void RequestBodyToMessage(const RequestBody& rb, RequestBodyMsg* rbMsg);

  /** Threads
   *
   * Functions whose names are ended with ``Thread`` will be instianted with a
   * thread. Some functions are heavy and needed to be parallelized, so the
   * parallized threads with the same functionality are distinguished with the
   * first parameter, id.
   *
   * Some functions will also use crash vector, to distinguish the crash vectors
   * used by them, the functions also accept the second parameter, cvId.  */
  void ReceiveThread(int id = -1);
  void ProcessThread(int id = -1);
  void RecordThread(int id = -1);
  void TrackThread(int id = -1);
  void FastReplyThread(int id = -1, int cvId = -1);
  void SlowReplyThread(int id = -1);
  void IndexSendThread(int id = -1, int cvId = -1);
  void IndexRecvThread();
  void IndexProcessThread();
  void MissedIndexAckThread();
  void MissedReqAckThread();
  void OWDCalcThread();
  void GarbageCollectThread();

  /** Message handler functions
   * These message handler functions will be used to instantiate MessageHandlers
   * and attached to their related endpoints.
   */
  void ReceiveClientRequest(MessageHeader* msgHdr, char* msgBuffer,
                            Address* sender);
  void ReceiveIndexSyncMessage(MessageHeader* msgHdr, char* msgBuffer);
  void ReceiveAskMissedReq(MessageHeader* msgHdr, char* msgBuffer);
  void ReceiveAskMissedIdx(MessageHeader* msgHdr, char* msgBuffer);
  void ReceiveMasterMessage(MessageHeader* msgHdr, char* msgBuffer);

  /** Used to instantiate indexAskTimer_ */
  void AskMissedIndex();
  /** Used to instantiate requestAskTimer_*/
  void AskMissedRequest();
  /** Used to instantiate heartbeatCheckTimer_ */
  void CheckHeartBeat();

 public:
  /** Replica accepts a config file, which contains all the necessary
   * information to instantiate the object, then it can call Run method
   *
   * Specifically, if this replica has crashed before, it will recieve
   * isRecovering=true, then it first completes the recovery procedure before it
   * can join the system
   *  */
  Replica(
      const std::string& configFile = "../configs/nezha-replica-config.yaml",
      bool isRecovering = false);
  ~Replica();

  void Run();
  void Terminate();

  /** Tentative */
  std::atomic<uint32_t>* repliedSyncPoint_;
  uint32_t maxProxyNum_ = 16;
};

}  // namespace nezha

#endif