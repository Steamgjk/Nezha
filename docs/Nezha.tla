
`^\textbf{\large N  TLA+ Specification}\\^' 
 
------------------------------ MODULE Nezha ----------------------------------

EXTENDS Naturals, TLC, FiniteSets, Sequences

--------------------------------------------------------------------------------
(* `^\textbf{\large Bounds for Model Check [Configurable]}^' *)

\* Time Range [Configurable]
MaxTime == 3

\* Each client is only allowed to submit MaxReqNum requests [Configurable]
\* In the specification, we will only consider two roles, client and replicas
\* (i.e. it can be considered as co-locating one proxy with one client)
\* For the proxy-based design, we just need to replace client with proxy, 
\* and then the specification describes the interaction between proxy and replicas
MaxReqNum == 1 

\* The leader is only allowed to crash when the view < MaxViews [Configurable]
MaxViews == 3

\* These variables are used to implment at-most-once primitives
\* i.e. The variables record the messages processed by Replicas/Clients, so 
\* that the Replicas/Clients will not process twice
VARIABLE  vReplicaProcessed, \* Messages that have been processed by replicas
          vClientProcessed \* Messages that have been processed by clients

VARIABLE DebugAction

(* `^\textbf{\large Constants}^' *)

\* The set of replicas and an ordering of them
CONSTANTS Replicas, ReplicaOrder, Clients, LatencyBounds
ASSUME IsFiniteSet(Replicas) 
ASSUME ReplicaOrder \in Seq(Replicas)


F == (Cardinality(Replicas) - 1) \div 2
ceilHalfF == IF (F \div 2) * 2 = F THEN F \div 2 ELSE (F+1) \div 2
floorHalfF == F \div 2
QuorumSize == F + 1
FastQuorumSize == F + ceilHalfF + 1
RecoveryQuorumSize == ceilHalfF + 1
FastQuorums == {R \in SUBSET(Replicas) : Cardinality(R) >= FastQuorumSize }
Quorums == {R \in SUBSET(Replicas) : Cardinality(R) * 2 > Cardinality(Replicas)}   

\* Replica Statuses
StNormal == 1
StViewChange == 2
StRecovering == 3

\* Message Types
MClientRequest == 1 \* Sent by client to replicas
MFastReply == 2 \* Fast Reply Message
MSlowReply == 3 \* Slow Reply Message
MLogIndex == 4  \* LogIndex
MLogEntry == 5  \* Log entry, different from index, it includes command field, which can be large in practice
MIndexSync == 6 \* Sync message during the index sync process
MMissEntryRequest == 7 \* Sent by followers once they fail to find the entry on itself
MMissEntryReply == 8  \* Response to MMissEntryRequest, providing the missing entries

MViewChangeReq == 9       \* Sent when leader/sequencer failure detected
MViewChange == 10        \* Sent to ACK view change
MStartView == 11           \* Sent by new leader to start view

\* The following messages are mainly used for periodic sync
\* Just as described in NOPaxos, it is an optional optimization to enable fast recovery after failure
MSyncPrepare == 12         \* Sent by the leader to ensure log durability
MSyncRep == 13             \* Sent by followers as ACK
MSyncCommit == 14           \* Sent by leaders to indicate stable log

\* The following messages are mainly used for replica recovery
MCrashVectorReq == 15
MCrashVectorRep == 16
MRecoveryReq == 17
MRecoveryRep == 18
MStateTransferReq == 19
MStateTransferRep == 20
      

(*
  `^\textbf{Message Schemas}^'

  ViewIDs == [ leaderNum |-> n \in (1..) ]

  \* <clientID, requestID> uniquely identifies one request on one replica
  \* But across replicas, the same <clientID, requestID> may have different deadlines
  \* (the leader may modify the deadline to make the request eligible to enter the early-buffer)
  \* so <deadline, clientID, reqID> uniquely identifes one request across replicas 

  ClientRequest
      [ mtype       |-> MClientRequest,
        sender      |-> c \in Clients,
        dest        |-> r \in Replicas,
        requestID   |-> i \in (1..), 
        command     |-> "", 
        s           |-> t \in (1..MaxTime), 
        l           |-> l \in (1..MaxBound)
      ]
  
  \* logSlotNum is not necessary and it is not described in the paper
  \* Here we include logSlotNum in FastReply and SlowReply messages
  \* to facilitate the check of Linearizability invariant
  FastReply
      [ mtype      |-> MFastReply,
        sender     |-> r \in Replicas,
        dest       |-> c \in Clients,
        viewID     |-> v \in ViewIDs,
        requestID  |-> i \in (1..vClientReqNum)
        hash       |-> [
                        log |-> vLogs[1..n], 
                        cv |-> crashVector
                       ] 
        deadline   |-> i \in (1..MaxTime+MaxBound),
        logSlotNum |-> n \in (1..)
      ]

  SlowReply
      [ mtype      |-> MSlowReply,
        sender     |-> r \in Replicas,
        dest       |-> c \in Clients,
        viewID     |-> v \in ViewIDs,
        requestID  |-> i \in (1..vClientReqNum)
        logSlotNum |-> n \in (1..)
      ]
      
  LogIndex
      [ mtype      |-> MLogIndex,
        clientID   |-> c \in Clients,
        requestID  |-> i \in (1..vClientReqNum),
        deadline   |-> i \in (1..MaxTime+MaxBound),
      ]
      
  LogEntry
      [ mtype      |-> MLogEntry,
        clientID   |-> c \in Clients,
        requestID  |-> i \in (1..vClientReqNum),
        deadline   |-> i \in (1..MaxTime+MaxBound),
        command    |-> ""
      ]
      
  IndexSync
      [ mtype      |-> MIndexSync,
        sender     |-> r \in Replicas,
        dest       |-> c \in Clients,
        viewID     |-> v \in ViewIDs,
        logindcies |-> index \in vLogs[leaderIdx]
      ]

   MMissEntryRequest
      [ mtype      |-> MMissEntryRequest,
        sender     |-> r \in Replicas,
        dest       |-> d \in Replicas,
        viewID     |-> v \in ViewIDs,
        miss       |-> {log indices}
      ]

   MMissEntryRequest
      [ mtype      |-> MMissEntryReply,
        sender     |-> r \in Replicas,
        dest       |-> d \in Replicas,
        viewID     |-> v \in ViewIDs,
        entries    |-> {log entries}
      ]
      
  ViewChangeReq
      [ mtype  |-> MViewChangeReq,
        sender |-> r \in Replicas,
        dest   |-> r \in Replicas,
        viewID |-> v \in ViewIDs,
        cv     |-> crash vector 
      ]

  ViewChange
      [ mtype      |-> MViewChange,
        sender     |-> r \in Replicas,
        dest       |-> r \in Replicas,
        viewID     |-> v \in ViewIDs,
        lastNormal |-> v \in ViewIDs,
        log        |-> l \in vLogs[1..n],
        cv         |-> crash vector  
      ]

  StartView
      [ mtype      |-> MStartView,
        dest       |-> r \in Replicas,
        viewID     |-> v \in ViewIDs,
        log        |-> l \in vLogs[1..n],
        cv         |-> crash vector 
      ]


  SyncPrepare
      [ mtype      |-> MSyncPrepare,
        dest       |-> r \in Replicas,
        sender     |-> r \in Replicas,
        viewID     |-> v \in ViewIDs,
        log        |-> l \in vLogs[1..n] ]

  SyncRep
      [ mtype         |-> MSyncRep,
        dest          |-> r \in Replicas,
        sender        |-> r \in Replicas,
        viewID        |-> v \in ViewIDs,
        logSlotNumber |-> n \in (1..) ]

  SyncCommit
      [ mtype         |-> MSyncCommit,
        dest          |-> r \in Replicas,
        sender        |-> r \in Replicas,
        viewID        |-> v \in ViewIDs,
        log           |-> l \in vLogs[1..n] ]
        
  CrashVectorReq
      [ mtype         |-> MCrashVectorReq,
        sender        |-> r \in Replicas,
        dest          |-> r \in Replicas,
        nonce         |-> nonce
      ] 
  CrashVectorRep
      [ mtype         |-> MCrashVectorRep,
        sender        |-> r \in Replicas,
        dest          |-> r \in Replicas,
        nonce         |-> nonce,
        cv            |-> vector of counters
      ] 
      
  RecoveryReq
      [ mtype         |-> MRecoveryReq,
        sender        |-> r \in Replicas,
        dest          |-> r \in Replicas,
        cv            |-> vector of counters
      ]  
      
  RecoveryRep
      [ mtype         |-> MRecoveryRep,
        sender        |-> r \in Replicas,
        dest          |-> r \in Replicas,
        viewID        |-> v \in ViewIDs,
        cv            |-> vector of counters
      ]           

  StateTransferReq
      [ mtype         |-> MStateTransferReq,
        sender        |-> r \in Replicas,
        dest          |-> r \in Replicas,
        cv            |-> vector of counters
      ]  
  StateTransferRep
      [ mtype         |-> MStateTransferRep,
        sender        |-> r \in Replicas,
        dest          |-> r \in Replicas,
        viewID        |-> v \in ViewIDs,
        log           |-> l \in vLogs[1..n] ],
        cv            |-> vector of counters
      ]  
*)

--------------------------------------------------------------------------------
(* `^\textbf{\large Variables}^' *)

\* `^\textbf{Network State}^'
VARIABLE messages \* Set of all messages sent

networkVars      == << messages >>
InitNetworkState == messages = {}

\* Used as a dummy value
NULLLog == [    deadline        |-> 0, 
                clientID    |-> 0,  
                requestID   |-> 0
           ]


\* `^\textbf{Replica State}^'
VARIABLES vLog,            \* Log of values
          vEarlyBuffer,    \* The early buffer to hold request,
                           \* and release it after clock passes its deadline (s+l)
          vReplicaStatus,  \* One of StNormal, StViewChange, StRecovering
          vViewID,         \* Current viewID replicas recognize
          vReplicaClock,   \* Current Time of the replica
          vLastNormView,   \* Last views in which replicas had status StNormal
          vViewChanges,    \* Used for logging view change votes
          vSyncPoint,      \* Latest synchronization point, 
                           \* to which the replica state (vLog) is consistent with the leader.
          vLateBuffer,     \* The late buffer Used to store the requests 
                           \* which are not eligible to enter vEarlyBuffer 
          
          vTentativeSync,  \* Used by leader to mark current syncPrepare point (during periodic sync process)
                           \* (Actually, vSyncPoint and vTentativeSync can be merged into one Var
                           \* However, we decouple them to make the spec easy to understand)
          vSyncReps,       \* Used for logging sync reps at leader 
          vCommitPoint,    \* Different from vSyncPoint, 
                           \* vCommitPoint indicates that the logs before this point has been replicated to majority
                           \* So followers can safely execute requests (log entries) up to vCommitPoint
                           \* Refer to ``Acceleration of Recovery" para in Sec 6
                           
          vUUIDCounter,    \* Locally unique string (for CrashVectorReq)
          vCrashVector,    \* CrashVector, initialized as all-zero vector
          vCrashVectorReps,\* CrashVectorRep Set
          vRecoveryReps    \* RecoveryRep Set
          
replicaVars      == << vLog, vEarlyBuffer, 
                       vViewID, vReplicaClock,
                       vLastNormView, vViewChanges,vReplicaStatus,
                       vSyncPoint, vLateBuffer,
                       vTentativeSync, vSyncReps, vCommitPoint, 
                       vUUIDCounter, vCrashVector, 
                       vCrashVectorReps, vRecoveryReps>>

InitReplicaState ==
  /\ vLog            = [ r \in Replicas |-> << >> ]
  /\ vEarlyBuffer    = [ r \in Replicas |-> {} ]
  /\ vViewID         = [ r \in Replicas |-> 1 ]  \* 0 should also be okay
  /\ vReplicaClock   = [ r \in Replicas |-> 1 ]
  /\ vLastNormView   = [ r \in Replicas |-> 1 ]
  /\ vViewChanges    = [ r \in Replicas |-> {} ]
  /\ vReplicaStatus  = [ r \in Replicas |-> StNormal ]
  /\ vSyncPoint      = [ r \in Replicas |-> 0 ]
  /\ vLateBuffer     = [ r \in Replicas |-> {} ]
  /\ vTentativeSync  = [ r \in Replicas |-> 0 ]
  /\ vSyncReps       = [ r \in Replicas |-> {} ]
  /\ vCommitPoint    = [ r \in Replicas |-> 0 ]
  /\ vCrashVector    = [ r \in Replicas |-> [ rr \in Replicas |-> 0] ]
  /\ vCrashVectorReps= [ r \in Replicas |-> {} ]
  /\ vRecoveryReps   = [ r \in Replicas |-> {} ]
  /\ vUUIDCounter    = [ c \in Replicas  |-> 0 ]

\* `^\textbf{Client State}^'
VARIABLES   vClientClock,   \* Current Clock Time of the client
            vClientReqNum   \* The number of requests that have been sent by this client

InitClientState  ==
  /\ vClientClock    = [ c \in Clients  |-> 1 ]
  /\ vClientReqNum   = [ c \in Clients  |-> 0 ]

clientVars          == << vClientClock, vClientReqNum >>

\* `^\textbf{Set of all vars}^'
vars == << networkVars, replicaVars, clientVars >>

\*\* `^\textbf{Initial state}^'
Init == /\ InitNetworkState
        /\ InitReplicaState
        /\ InitClientState
        /\ vReplicaProcessed = [ r \in Replicas |-> {} ]
        /\ vClientProcessed = [c \in Clients |-> {}]
        /\ DebugAction = <<"Init", "">>

--------------------------------------------------------------------------------
(* `^\textbf{\large Helpers}^' *)

NumofReplicas(status) == Cardinality({ r \in Replicas: vReplicaStatus[r] = status }) 

DuplicateRep(ReplySet,m) == m.sender \in { mm.sender : mm \in ReplySet } 

Pick(S) == CHOOSE s \in S : TRUE
                              
\* Convert a Set to Sequence
RECURSIVE Set2Seq(_)
Set2Seq(S) == IF Cardinality(S) = 0 THEN <<>>
          ELSE
          LET
            x == CHOOSE x \in S : TRUE
          IN
            <<x>> \o Set2Seq(S \ {x})

\* Convert a Sequence to Set
Seq2Set(seq) ==  { seq[i] : i \in DOMAIN seq }
       
Max(S) == CHOOSE x \in S : \A y \in S : x >= y

Min(S) == CHOOSE x \in S : \A y \in S : x <= y

\* `^\textbf{View ID Helpers}^'
LeaderID(viewID) == (viewID % Len(ReplicaOrder)) + (IF viewID >= Len(ReplicaOrder) THEN 1 ELSE 0)

Leader(viewID) == ReplicaOrder[LeaderID(viewID)]  \* remember <<>> are 1-indexed                             


\* `^\textbf{Log Manipulation Helpers}^'

\* The order of 2 log entries are decided by the tuple <deadline, clientID, requestID>
\* Usually, deadline makes the two entries comparable
\* When 2 different entries have the same deadline, the tie is broken with clientID
\* Further, the tie is broken is requestID 
\* (unnecessary if we only allow client to submit one request at one tick) 
EntryLeq(l1, l2)      == /\ l1.deadline <= l2.deadline
                         /\ l1.clientID <= l2.clientID
                         /\ l1.requestID <= l2.requestID
                         
EntryEq(l1, l2)       == /\ l1.deadline = l2.deadline  
                         /\ l1.clientID = l2.clientID
                         /\ l1.requestID = l2.requestID

EntryLessThan(l1, l2) == /\ EntryLeq(l1, l2)
                         /\ ~(EntryEq(l1, l2))
                            
\* Find entry in one replica's log (<clientID, reqID> can uniquely identify the log entry)
\* We do not check deadline, because the leader may have modified the request's deadline
\* Return 0 when we fail to find it (remember Sequence is 1-indexed in TLA+, so 0 can serve as a dummy value)
FindEntry(clientID, reqID, log) == 
                         LET 
                            entryIndexSet == { i \in 1..Len(log): /\ log[i].clientID = clientID
                                                                  /\ log[i].reqID = reqID }
                          IN
                            IF Cardinality(entryIndexSet) = 0 THEN 
                                0
                            ELSE
                                Pick(entryIndexSet)
                                

SortLogSeq(seq) == SortSeq(seq, LAMBDA x, y: EntryLessThan(x, y) )

\* Given a set of logs, return the sorted log list
GetSortLogSeq(S) == LET
                        seq == Set2Seq(S)
                    IN
                        SortLogSeq(seq)
                        
                            
(* Merge logs, first put all log items together, deduplicated (i.e. UNION them into a set).
   Then, do filtering and only keep those that have appeared in at least
   `^\left \lceil{f/2}\right \rceil +1^' replicas. *)

CountVotes(logll, x) ==  Cardinality({ logSet \in logll : x \in logSet })

MergeUnSyncLogs(unSyncedLogs, lastSyncedLog) == 
        LET 
            unSyncedLogSet == UNION unSyncedLogs
            votedLogSet == {x \in unSyncedLogSet : 
                               /\ EntryLessThan(lastSyncedLog, x)
                               /\ CountVotes(unSyncedLogs, x) >= RecoveryQuorumSize}
        IN
            GetSortLogSeq(votedLogSet)
            
\* `^\textbf{Network Helpers}^'
\* Add a message to the network
Send(ms) == messages' = messages \cup ms

\* Convert the request format to a log format (by summing up s and l to get deadline)
Req2Log(req) == [   mtype       |-> MLogEntry,
                    deadline    |-> req.s + req.l, 
                    clientID    |-> req.sender,
                    requestID   |-> req.requestID,
                    command     |-> req.command
                ]
\* Index does not need to include command field, which is the body of the request/log, and can be very large
GetLogIndex(entry) == [ 
                    mtype       |-> MLogIndex, 
                    deadline    |-> entry.deadline,
                    clientID    |-> entry.clientID,
                    requestID   |-> entry.requestID
                ]

GetLogIndexFromReply(reply) == [
                    mtype       |-> MLogIndex, 
                    deadline    |-> reply.deadline,
                    clientID    |-> reply.dest,
                    requestID   |-> reply.requestID
                ]


IndexEq(index, msg) == /\ index.deadline = msg.deadline
                       /\ index.clientID = msg.clientID
                       /\ index.requestID = msg.requestID

\* Add local time to the message (for easy debug)
Msg2RLog(msg, r) == msg @@ [tl |-> vReplicaClock[r]]


       
LastLog(logList) == IF Len(logList) = 0 THEN NULLLog ELSE  logList[Len(logList)]     

MergeCrashVector(cv1, cv2)== [ r \in Replicas |-> Max({cv1[r], cv2[r]}) ]

CheckCrashVector(m, r) == 
    IF m.cv[m.sender] < vCrashVector[r][m.sender] THEN
        FALSE \* Potential stray message
    ELSE 
        vCrashVector' = [ vCrashVector  EXCEPT ![r] = MergeCrashVector(m.cv, vCrashVector[r])]
                               
FilterStrayMessage(MSet, cv) == {m \in MSet : m.cv[m.sender] >= cv[m.sender] }
    
    
--------------------------------------------------------------------------------
(* `^\textbf{\large Message Handlers and Actions }^' *)

\* `^\textbf{Client action}^'
\* Client c sends a request
\* We assume client can only send one request in one tick of time
\* If time has reached the bound, this client cannot send request any more

ClientSendRequest(c) ==     /\ vClientClock[c] < MaxTime 
                            /\ vClientReqNum[c] < MaxReqNum
                            /\ Send({[ mtype |-> MClientRequest,
                                       sender       |-> c, \* clientID
                                       requestID    |-> vClientReqNum[c] + 1, \* requestID
                                       command      |-> "",
                                       s            |-> vClientClock[c], \* submission time
                                       l            |-> LatencyBounds[c], \* latency bound
                                       dest         |-> r
                                      ]: r \in Replicas })
                            /\ vClientClock' = [ vClientClock EXCEPT ![c] = vClientClock[c] + 1 ]
                            /\ vClientReqNum' = [ vClientReqNum EXCEPT ![c] = vClientReqNum[c] +1 ]
                            /\ UNCHANGED  << replicaVars >>  
                                                    


Duplicate(entry, logSet) == 
  LET
       findSet == {x \in logSet : /\ x.clientID = entry.clientID 
                                  /\ x.requestID = entry.requestID }
  IN
       Cardinality(findSet) > 0
    
\* Replica r receives MClientRequest, m
HandleClientRequest(r, m) ==
  LET
    mlog == Req2Log(m)
  IN
  \* If the request is duplicate, it will no longer be appended to the log
  \* Replicas simply reply the previous execution result of this request 
  \* (we do not model execution in this spec)
  /\ ~Duplicate(mlog, Seq2Set(vLog[r]) \union vEarlyBuffer[r] )
  /\ vReplicaStatus[r] = StNormal
     \* The request can enter the early buffer
  /\ \/ /\ EntryLessThan(LastLog(vLog[r]), mlog)
        /\ vEarlyBuffer' = [ 
                vEarlyBuffer EXCEPT ![r] =vEarlyBuffer[r] \cup { mlog } 
           ]
        /\ UNCHANGED   << networkVars, clientVars, 
                       vLog, vViewID, vReplicaClock,
                       vLastNormView, vViewChanges,vReplicaStatus,
                       vSyncPoint,  vLateBuffer, 
                       vTentativeSync, vSyncReps, vCommitPoint,
                       vUUIDCounter, vCrashVector, 
                       vCrashVectorReps, vRecoveryReps >> 
     \* (1) Followers' early buffers do not accept the request 
     \*     if its deadline is smaller than previously appended (last released) entry,
     \*     so followers directly put the request into the late buffer
     \* (2) Leader modifies its deadline to be larger than the last released entry
     \*     so as to make it eligible for entering the early buffer
     \/ /\ EntryLessThan(mlog, LastLog(vLog[r]))
        /\ IF   r = Leader(vViewID[r])  THEN \* this replica is the leader in the current view
                /\ vEarlyBuffer' = [ 
                        vEarlyBuffer EXCEPT ![r] =vEarlyBuffer[r] \cup {[
                            mtype      |-> MLogEntry,
                            clientID   |-> mlog.clientID,
                            requestID  |-> mlog.requestID,
                            deadline   |-> LastLog(vLog[r]).deadline + 1,
                            command    |-> mlog.command
                        ]} 
                   ]
                /\ UNCHANGED   << networkVars, clientVars, 
                                vLog, vViewID, vReplicaClock,
                                vLastNormView, vViewChanges,vReplicaStatus,
                                vSyncPoint, vLateBuffer, 
                                vTentativeSync, vSyncReps, vCommitPoint, 
                                vUUIDCounter, vCrashVector, 
                                vCrashVectorReps, vRecoveryReps >> 
           ELSE \* this replica is a follower in the current view
                /\ vLateBuffer' = [ 
                        vLateBuffer EXCEPT ![r] =vLateBuffer[r] \cup { mlog } 
                   ]

                /\ UNCHANGED   << networkVars, clientVars, 
                               vLog, vEarlyBuffer, vViewID, vReplicaClock,
                               vLastNormView, vViewChanges,vReplicaStatus,
                               vSyncPoint, vTentativeSync, 
                               vSyncReps, vCommitPoint,  
                               vUUIDCounter, vCrashVector, 
                               vCrashVectorReps, vRecoveryReps >> 

                                              
\* Release relevant requests from vEarlyBuffer and append to vLog, 
\* and then send a fast reply
FlushEarlyBuffer(r) ==
    LET 
       validLogSet == {x \in vEarlyBuffer[r]: 
                         /\ x.deadline < vReplicaClock[r] \* < rather than <= 
                         /\ EntryLessThan(LastLog(vLog[r]), x) }
       validLogs == GetSortLogSeq(validLogSet)
       newLogStart == Len(vLog[r]) + 1
    IN
    /\  vLog' = [vLog EXCEPT ![r] = vLog[r] \o validLogs ]
    /\  vEarlyBuffer' = [ vEarlyBuffer EXCEPT ![r] 
                        = {x \in vEarlyBuffer[r]: x.deadline >= vReplicaClock[r] } ] \* >= rather than >
    /\  Send({[ mtype       |-> MFastReply,
                sender      |-> r,
                dest        |-> vLog'[r][i].clientID,
                viewID      |-> vViewID[r],
                requestID   |-> vLog'[r][i].requestID,
                hash        |-> [
                                 log |-> SubSeq(vLog'[r], 1, i),
                                 cv  |-> vCrashVector
                                 ],
                deadline    |-> vLog'[r][i].deadline,
                logSlotNum  |-> i
               ] : i \in newLogStart..Len(vLog'[r])})
    /\  IF r = Leader(vViewID[r])  THEN 
            /\ vSyncPoint' =  [ vSyncPoint EXCEPT ![r] = Len(vLog'[r]) ]
            /\ UNCHANGED   <<  clientVars, vViewID, vLastNormView, vViewChanges,
                               vReplicaStatus, vReplicaClock, vLateBuffer,
                               vTentativeSync, vSyncReps, vCommitPoint,
                               vUUIDCounter, vCrashVector, 
                               vCrashVectorReps, vRecoveryReps >> 
        ELSE
            UNCHANGED   << clientVars, vViewID, vLastNormView, vViewChanges,
                           vReplicaStatus, vReplicaClock, 
                           vSyncPoint, vLateBuffer, 
                           vTentativeSync, vSyncReps, vCommitPoint,
                           vUUIDCounter, vCrashVector, 
                           vCrashVectorReps, vRecoveryReps  >> 

\* Clock can be random value (RandomElement(1..MaxTime)),
\* because clock sync algorithm can give negative offset, or even fails 
\* But Nezha depend on clock for performance but not for correctness                               
\* If the replica clock goes beyond MaxTime, it will stop processing
\* Since Clock is moved, then replicas can release relevant requests and append to logs                       
ReplicaClockMove(r) ==/\ IF vReplicaClock[r] < MaxTime THEN
                            vReplicaClock' =  [ 
                                vReplicaClock EXCEPT ![r] = RandomElement(1..MaxTime)
                            ]
                         ELSE  
                            UNCHANGED vReplicaClock
                      /\ UNCHANGED << networkVars, clientVars, 
                                      vLog, vEarlyBuffer,vViewID, 
                                      vLastNormView, vViewChanges, vReplicaStatus,
                                      vSyncPoint, vLateBuffer, vTentativeSync,
                                      vSyncReps,vCommitPoint,
                                      vUUIDCounter, vCrashVector, 
                                      vCrashVectorReps, vRecoveryReps >>
\* Client clock move does not change any other things
ClientClockMove(c) == /\  IF vClientClock[c] < MaxTime THEN
                            vClientClock' = [
                                vClientClock EXCEPT ![c] = RandomElement(1..MaxTime)
                            ]
                          ELSE
                            UNCHANGED vClientClock
                      /\  UNCHANGED <<networkVars, replicaVars, vClientReqNum>>

                      
 
--------------------------------------------------------------------------------
\* `^\textbf{\large Index Synchronization to Fix Set Inequality}^'

\* Leader replica r starts index synchronization
StartIndexSync(r) ==
  LET 
    indices == { GetLogIndex(vLog[r][i]) : i \in 1..Len(vLog[r]) }
  IN
  /\ r = Leader(vViewID[r])
  /\ vReplicaStatus[r]  = StNormal
  /\ Cardinality(indices) > 0  \* leader has log entries to sync
  /\ Send({[ mtype      |-> MIndexSync,
             sender     |-> r,
             dest       |-> d,
             viewID     |-> vViewID[r],
             logindcies |-> indices ] : d \in Replicas })
  /\ UNCHANGED << clientVars, replicaVars >>

                       
GetSyncLogs(logSeq, indices) == 
    LET
        logSet == { l \in Seq2Set(logSeq) : \E index \in indices: EntryEq(index, l)}
    IN
        GetSortLogSeq(logSet)

GetUnSyncLogs(logSeq, lastSyncedLog) == 
    LET
        logSet == { l \in Seq2Set(logSeq) : EntryLessThan(lastSyncedLog, l) }
    IN
        GetSortLogSeq(logSet)
        
\* Replica r receives IndexSync message, m
HandleIndexSync(r, m) ==
  /\ r /= Leader(vViewID[r])
  /\ vReplicaStatus[r] = StNormal
  /\ m.viewID = vViewID[r]
  /\ m.sender = Leader(vViewID[r])
  /\ vSyncPoint[r] < Len(m.logindcies)
  /\ LET 
        entries == { vLog[r][i] : i \in 1..Len(vLog[r]) }
        indices == { GetLogIndex(vLog[r][i]) : i \in 1..Len(vLog[r]) }
        missedEntries == m.indices \ indices
     IN
        \* Missing some log entries -> Send MMissEntryRequest
        IF Cardinality(missedEntries) > 0 THEN
            /\ Send({[  mtype      |-> MMissEntryRequest,
                        sender     |-> r,
                        dest       |-> d,
                        viewID     |-> vViewID[r],
                        miss       |-> missedEntries ] : d \in (Replicas \ {r} ) })
            /\ UNCHANGED << vLog, vSyncPoint >>
        \* No missing entries, update vLog and vSyncPoint, and send relevant slow replies
        ELSE
            LET 
                syncLogs ==  GetSyncLogs(vLog[r], indices)
                unsyncLogs ==  GetUnSyncLogs(vLog[r], LastLog(syncLogs))
            IN
            /\ vLog' = [ vLog EXCEPT ![r] = syncLogs \o unsyncLogs ]
            /\ vSyncPoint' = [ vSyncPoint EXCEPT ![r] = Len(syncLogs) ]
            /\ Send({[   mtype      |-> MSlowReply,
                         sender     |-> r,
                         dest       |-> vLog'[r][i].clientID,
                         viewID     |-> vViewID[r],
                         requestID  |-> vLog'[r][i].requestID,
                         logSlotNum |-> i ] : i \in (1..Len(syncLogs))})
            
  /\ UNCHANGED << clientVars, vEarlyBuffer, vViewID, vReplicaClock, 
                 vLastNormView, vViewChanges,  vReplicaStatus, 
                 vLateBuffer, vTentativeSync, vSyncReps, vCommitPoint,
                 vUUIDCounter, vCrashVector, 
                 vCrashVectorReps, vRecoveryReps>>


FindEntries(log, indices)== 
    { l \in Seq2Set(log)  : \E x \in indices: IndexEq(l,x) }

\* Replica r receives a request from other replicas, asking for a missing log entry
HandleMissEntryRequest(r, m) == 
  /\ m.viewID = vViewID[r]
  /\ LET 
        findentries == FindEntries(vLog[r], m.miss)
     IN
     /\ Cardinality(findentries) > 0
     /\ Send({[   mtype      |-> MMissEntryReply,
                  sender     |-> r,
                  dest       |-> m.sender,
                  viewID     |-> vViewID[r],
                  entries    |-> findentries ]})
     /\ UNCHANGED << clientVars, replicaVars >>  
       

   
\* Replica r receives a reply from other replicas, providing the missing entries
HandleMissEntryReply(r, m) == 
    /\ m.viewID = vViewID[r]
    /\ LET
        mergedSet == Seq2Set(vLog[r]) \union m.entries 
       IN
        vLog' = [ vLog EXCEPT ![r] = GetSortLogSeq(mergedSet) ]
    /\ UNCHANGED << networkVars, clientVars, 
                    vEarlyBuffer,vViewID, vReplicaClock, 
                    vLastNormView, vViewChanges, vReplicaStatus,
                    vSyncPoint, vLateBuffer, 
                    vTentativeSync,vSyncReps, vCommitPoint,
                    vUUIDCounter, vCrashVector, 
                    vCrashVectorReps, vRecoveryReps >>

                       
--------------------------------------------------------------------------------
\* `^\textbf{\large Replica Rejoin}^'
\* Failed replica loses all states
StartReplicaFail(r) == 
    /\ NumofReplicas(StRecovering) < F \* We assume at most F replicas can fail at the same time
    /\ vReplicaStatus' = [ vReplicaStatus EXCEPT ![r] = StRecovering ]
    /\ vLog' = [ vLog EXCEPT ![r] = <<>> ]
    /\ vEarlyBuffer' = [ vEarlyBuffer EXCEPT ![r] = {} ]
    /\ vViewID' = [vViewID EXCEPT![r] = 1 ]
    /\ vLastNormView'   = [ vLastNormView EXCEPT ![r] = 1 ]
    /\ vViewChanges' = [ vViewChanges EXCEPT ![r] = {} ]
    /\ vSyncPoint' = [ vSyncPoint EXCEPT ![r] = 0 ]
    /\ vLateBuffer' = [ vLateBuffer EXCEPT ![r] = {} ]
    /\ vTentativeSync' = [ vTentativeSync EXCEPT ![r] = 0 ]
    /\ vSyncReps' = [ vSyncReps EXCEPT ![r] = {} ]
    /\ vCommitPoint' = [ vCommitPoint EXCEPT ![r] = 0 ]
    /\ vCrashVector' = [ vCrashVector EXCEPT ![r] =  [ rr \in Replicas |-> 0] ]
    /\ vCrashVectorReps' = [ vCrashVectorReps EXCEPT ![r] = {} ]
    /\ vRecoveryReps' = [ vRecoveryReps EXCEPT ![r] = {} ]
    /\ UNCHANGED << vReplicaClock, vUUIDCounter, clientVars, networkVars >>




                       
\* Recovering replica starts recovery (by first sending CrashVectorReq)
StartReplicaRecovery(r) ==
    /\ vReplicaStatus[r] = StRecovering
    /\ vUUIDCounter' = [ vUUIDCounter EXCEPT ![r] = vUUIDCounter[r] + 1 ]
    /\ Send({[ mtype  |-> MCrashVectorReq,
               sender |-> r,
               dest   |-> d,
               nonce  |-> vUUIDCounter'[r] ] : d \in Replicas})
    /\ UNCHANGED << vLog, vEarlyBuffer, vViewID, vReplicaClock,
                    vLastNormView, vViewChanges,vReplicaStatus,
                    vSyncPoint, vLateBuffer,
                    vTentativeSync, vSyncReps, vCommitPoint, 
                    vCrashVector, vCrashVectorReps, vRecoveryReps,
                    clientVars  >>
                       
                       
HandleCrashVectorReq(r, m) ==
    /\ vReplicaStatus[r] = StNormal
    /\ Send({[ mtype  |-> MCrashVectorRep,
               sender |-> r,
               dest   |-> m.sender,
               nonce  |-> m.nonce,
               cv     |-> vCrashVector[r] ]})
    /\ UNCHANGED << replicaVars,  clientVars >>
    


HandleCrashVectorRep(r, m) ==
    /\ vReplicaStatus[r] = StRecovering
    /\ vUUIDCounter[r] = m.nonce
    /\ Cardinality(vCrashVectorReps[r]) <= F
    /\ ~DuplicateRep(vCrashVectorReps[r],m)
    /\ vCrashVectorReps' = [ vCrashVectorReps EXCEPT ![r] = vCrashVectorReps[r] \cup {m} ]
    /\ vCrashVector' = [ vCrashVector EXCEPT ![r] = MergeCrashVector(vCrashVector[r], m.cv) ] 
    /\ IF Cardinality(vCrashVectorReps') = F + 1 THEN  \* got enough replies and can settle down cv
        Send({[ mtype  |-> MRecoveryReq,
                sender |-> r,
                dest   |-> d,
                nonce  |-> m.nonce,
                cv     |-> vCrashVector'[r] ]: d \in Replicas })
       ELSE
        UNCHANGED << networkVars >>

    /\ UNCHANGED <<vLog, vEarlyBuffer, vViewID, vReplicaClock,
                    vLastNormView, vViewChanges,vReplicaStatus,
                    vSyncPoint, vLateBuffer,
                    vTentativeSync, vSyncReps, vCommitPoint, 
                    vUUIDCounter, vRecoveryReps,
                    clientVars >>



HandleRecoveryReq(r, m) == 
    /\ vReplicaStatus[r] = StNormal
    /\ vCrashVector' = [ vCrashVector EXCEPT ![r] = MergeCrashVector(vCrashVector[r], m.cv) ] 
    /\ Send({[  mtype  |-> MRecoveryRep,
                sender |-> r,
                dest   |-> m.sender,
                viewID |-> vViewID[r],
                cv     |-> vCrashVector'[r] ]: d \in Replicas })

    /\ UNCHANGED  << vLog, vEarlyBuffer, vViewID, vReplicaClock,
                    vLastNormView, vViewChanges,vReplicaStatus,
                    vSyncPoint, vLateBuffer,
                    vTentativeSync, vSyncReps, vCommitPoint, 
                    vUUIDCounter, vCrashVectorReps, vRecoveryReps,
                    clientVars   >>



HandleRecoveryRep(r, m) ==
    /\ vReplicaStatus[r] = StRecovering
    /\ Cardinality(vRecoveryReps[r]) <= F
    /\ ~DuplicateRep(vRecoveryReps[r], m.sender)
    /\ CheckCrashVector(m, r)
(* `~
    /\ vRecoveryReps' = [ vRecoveryReps EXCEPT 
                          ![r] = vRecoveryReps[r] \cup {m}  ]
~'
*)
\* Note: After crash vector is updated, those previously accepted messages may also become stray message.
\* Those messages should also be filtered out.
    /\ vRecoveryReps' = [ vRecoveryReps EXCEPT 
                          ![r] = FilterStrayMessage(vRecoveryReps[r] \cup {m}, vCrashVector'[r] )  ]
           
    /\ IF Cardinality(vRecoveryReps') = F + 1 THEN  \* got enough replies
        LET 
            newView == Max({ mm.viewID : mm \in vRecoveryReps'[r] })
            leaderId == newView % Cardinality(Replicas)
        IN 
            Send({[ mtype  |-> MStateTransferReq,
                    sender |-> r,
                    dest   |-> leaderId,
                    cv     |-> vCrashVector'[r] ]: d \in Replicas })
       ELSE
        UNCHANGED << networkVars >>

    /\ UNCHANGED <<vLog, vEarlyBuffer, vViewID, vReplicaClock,
                    vLastNormView, vViewChanges,vReplicaStatus,
                    vSyncPoint, vLateBuffer,
                    vTentativeSync, vSyncReps, vCommitPoint, 
                    vUUIDCounter, vCrashVectorReps,
                    clientVars >>



HandleStateTransferReq(r, m) == 
    /\ vReplicaStatus[r] = StNormal
    /\ CheckCrashVector(m, r)
    /\ Send({[  mtype  |-> MStateTransferRep,
                sender |-> r,
                dest   |-> m.sender,
                log    |-> vLog[r],
                sp     |-> vSyncPoint[r],
                cp     |-> vCommitPoint[r],
                cv     |-> vCrashVector'[r] ]})
    /\ UNCHANGED  << vLog, vEarlyBuffer, vViewID, vReplicaClock,
                    vLastNormView, vViewChanges,vReplicaStatus,
                    vSyncPoint, vLateBuffer,
                    vTentativeSync, vSyncReps, vCommitPoint, 
                    vUUIDCounter, vCrashVectorReps, vRecoveryReps,
                    clientVars >>


HandleStateTransferRep(r, m) == 
    /\ vReplicaStatus[r] = StRecovering
    /\ CheckCrashVector(m, r)
    /\ vLog' = [ vLog EXCEPT ![r] = m.log ]
    /\ vSyncPoint' = [ vSyncPoint EXCEPT ![r] = m.sp ]
    /\ vCommitPoint' = [ vCommitPoint EXCEPT ![r] = m.cp ]
    /\ vViewID' = [ vViewID EXCEPT  ![r] = m.viewID ]
    /\ vEarlyBuffer' = [ vEarlyBuffer EXCEPT ![r] = {} ] 
    /\ vLastNormView' = [ vLastNormView EXCEPT ![r] = m.viewID ]
    /\ vViewChanges' = [vViewChanges EXCEPT ![r] = {} ]
    /\ vReplicaStatus' = [ vReplicaStatus EXCEPT ![r] = StNormal ]
    /\ vLateBuffer' = [ vLateBuffer EXCEPT ![r] = {} ]
    /\ vTentativeSync' = [ vTentativeSync EXCEPT ![r] = m.sp ]
    /\ vSyncReps' = [ vSyncReps EXCEPT ![r] = {} ]
    /\ vCrashVectorReps' = [ vCrashVectorReps EXCEPT ![r] = {} ]
    /\ vRecoveryReps' = [ vRecoveryReps EXCEPT ![r]= {} ]
    /\ UNCHANGED  << vReplicaClock, vUUIDCounter, clientVars >>


 
--------------------------------------------------------------------------------
\* `^\textbf{\large Leader Change}^'

\* Replica r starts a Leader change
StartLeaderChange(r) ==
  /\ Send({[ mtype  |-> MViewChangeReq,
             sender |-> r,
             dest   |-> d,
             viewID |-> vViewID[r] + 1,
             cv     |-> vCrashVector[r] ] : d \in Replicas})
  /\ UNCHANGED << replicaVars, clientVars >>
  
  
\* `^\textbf{View Change Handlers}^'
\* Replica r gets MViewChangeReq, m
HandleViewChangeReq(r, m) ==
  LET
    currentViewID == vViewID[r]
    newViewID     == Max({currentViewID, m.viewID})
    newLeaderNum  == LeaderID(newViewID)
  IN
  \* Recovering replica does not participate in view change
  /\ vReplicaStatus[r] /= StRecovering
  /\ currentViewID   /= newViewID
  /\ CheckCrashVector(m, r)
  /\ vReplicaStatus' = [ vReplicaStatus EXCEPT ![r] = StViewChange ]
  /\ vViewID'        = [ vViewID EXCEPT ![r] = newViewID ]
  /\ vViewChanges'   = [ vViewChanges EXCEPT ![r] = {} ]
  /\ Send({[ mtype      |-> MViewChange,
             dest       |-> Leader(newViewID),
             sender     |-> r,
             viewID     |-> newViewID,
             lastNormal |-> vLastNormView[r],
             syncedLog  |-> SubSeq(vLog[r], 1, vSyncPoint[r]),
             unsyncedLog|-> SubSeq(vLog[r], vSyncPoint[r]+1, Len(vLog[r])),
             cv         |-> vCrashVector[r] ]} \cup
           \* Send the MViewChangeReqs in case this is an entirely new view
           {[ mtype  |-> MViewChangeReq,
              sender |-> r,
              dest   |-> d,
              viewID |-> newViewID,
              cv     |-> vCrashVector[r] ] : d \in Replicas})
  /\ UNCHANGED << clientVars, vLog, vEarlyBuffer, vReplicaClock,
                  vLastNormView, vSyncPoint, vLateBuffer, 
                  vTentativeSync, vSyncReps, vCommitPoint,
                  vUUIDCounter, vCrashVectorReps, vRecoveryReps >>

                       
\* Replica r receives MViewChange, m
HandleViewChange(r, m) ==
  \* Recovering replica does not participate in view change
  /\ vReplicaStatus[r] /= StRecovering
  \* Add the message to the log
  /\ vViewID[r]         = m.viewID
  /\ vReplicaStatus[r]  = StViewChange
  \* This replica is the leader
  /\ Leader(vViewID[r]) = r
  /\ CheckCrashVector(m, r)
(* `~
  /\ vViewChanges' = [ vViewChanges EXCEPT ![r] = vViewChanges[r] \cup {m}]
~'
*)
  \* Note: Similar to vRecoveryReps, (potential) stray messages should be filtered out.
  /\ vViewChanges' = [ vViewChanges EXCEPT 
                       ![r] = FilterStrayMessage(vViewChanges[r] \cup {m}, vCrashVector'[r]) ]
  \* If there's enough replies, start the new view
  /\ LET
       isViewPromise(M) == /\ { n.sender : n \in M } \in Quorums
                           /\ \E n \in M : n.sender = r
       vCMs             == { n \in vViewChanges'[r] :
                               /\ n.mtype  = MViewChange
                               /\ n.viewID = vViewID[r] }
       \* Create the state for the new view
       normalViews  == { n.lastNormal : n \in vCMs }
       \* Choose the largest normal view (i.e. the newest)
       lastNormal     == (CHOOSE v \in normalViews : \A v2 \in normalViews : v2 <= v)
       \* For logs before vSyncPoint (i.e. syncedLog), we directly copy from the bestCandiates
       \* For unsyncedLog, we do quorum check to decide which ones should be added to recovery Log
       goodCandidates ==  { o \in vCMs : o.lastNormal = lastNormal }
       \* bestCandidate can only be picked from goodCandidates, 
       \* because previous views may include invalid logs
       bestCandidate  == CHOOSE n \in goodCandidates: 
                            \A y \in goodCandidates: Len(n.syncedLog) >= Len(y.syncedLog)
       unSyncedLogs   == { Seq2Set(n.unsyncedLog) : n \in goodCandidates }

     IN
       IF isViewPromise(vCMs) THEN
         Send({[ mtype      |-> MStartView,
                 dest       |-> d,
                 viewID     |-> vViewID[r],
                 log        |-> bestCandidate.syncedLog 
                                \o MergeUnSyncLogs(unSyncedLogs, LastLog(bestCandidate.syncedLog))
               ] : d \in Replicas })
       ELSE
         UNCHANGED networkVars
  /\ UNCHANGED << clientVars,  vLog,  vEarlyBuffer, vViewID, vReplicaClock, 
                  vLastNormView, vReplicaStatus, vSyncPoint, vLateBuffer,
                  vTentativeSync, vSyncReps,vCommitPoint, 
                  vUUIDCounter, vCrashVectorReps, vRecoveryReps >>


                       
\* Replica r receives a MStartView, m
HandleStartView(r, m) ==
  /\ vReplicaStatus[r] /= StRecovering
  /\ \/ vViewID[r]   < m.viewID
     \/ vViewID[r]   = m.viewID /\ vReplicaStatus[r] = StViewChange
  /\ CheckCrashVector(m, r)
  /\ vLog'           = [ vLog EXCEPT ![r] = m.log ]
  /\ vReplicaStatus' = [ vReplicaStatus EXCEPT ![r] = StNormal ]
  /\ vViewID'        = [ vViewID EXCEPT ![r] = m.viewID ]
  /\ vLastNormView'  = [ vLastNormView EXCEPT ![r] = m.viewID ]
  /\ vEarlyBuffer' = [ vEarlyBuffer EXCEPT ![r] = {} ] \* clear Early Buffer for the new view
  /\ vLateBuffer' = [ vLateBuffer EXCEPT ![r] = {}] \* clear Late Buffer for the new view
  /\ vSyncPoint' = [ vSyncPoint EXCEPT![r] = Len(m.log) ]  
  /\ vTentativeSync' = [ vTentativeSync EXCEPT![r] = Len(m.log) ]
  \* Send replies (in the new view) for all log items
  /\ IF r = Leader(m.viewID) THEN   \* Leader only sends fast reply
        Send({[  mtype      |-> MFastReply,
                 sender     |-> r,
                 dest       |-> m.log[i].clientID,
                 viewID     |-> m.viewID,
                 requestID  |-> m.log[i].requestID,
                 hash       |-> [
                                    log |-> SubSeq(m.log, 1, i),
                                    cv  |-> vCrashVector
                                ],
                 deadline   |-> m.log[i].deadline,
                 logSlotNum |-> i ] : i \in (1..Len(m.log))})
     ELSE \* While staring view, followers knows the log is synced with the leader, so send slow-reply
        Send({[  mtype      |-> MSlowReply,
                 sender     |-> r,
                 dest       |-> m.log[i].clientID,
                 viewID     |-> m.viewID,
                 requestID  |-> m.log[i].requestID,
                 logSlotNum |-> i ] : i \in (1..Len(m.log))})
  /\ UNCHANGED << clientVars, vReplicaClock, vViewChanges, 
                  vSyncReps, vCommitPoint, vCrashVector,
                  vUUIDCounter, vCrashVectorReps, vRecoveryReps >> 
                       
--------------------------------------------------------------------------------
\* `^\textbf{\large Periodic Synchronization}^'
\* Leader replica r conduct synchronization periodically
\* This periodic sync process is different from index sync process
\* It ensures that all replicas’ logs are stable up to their CommitPoint (for fast recovery)
\* Our CommitPoint is essentially the `^\emph{sync-point}^' defined in NOPaxos paper 
\* Just as mentioned in NOPaxos paper, it is an optional optimization for fast recovery
\* Nezha still works even without this part
StartSync(r) ==
  /\ Leader(vViewID[r]) = r
  /\ vReplicaStatus[r]  = StNormal
  /\ vTentativeSync[r] < Len(vLog[r])  \* If >= then no need to sync
  /\ vSyncReps'         = [ vSyncReps EXCEPT ![r] = {} ]
  /\ vTentativeSync'    = [ vTentativeSync EXCEPT ![r] = Len(vLog[r]) ]
  /\ Send({[ mtype      |-> MSyncPrepare,
             sender     |-> r,
             dest       |-> d,
             viewID     |-> vViewID[r],
             log        |-> vLog[r] ] : d \in Replicas })
  /\ UNCHANGED << clientVars, vLog, vEarlyBuffer, vViewID, vReplicaClock, 
                  vLastNormView, vViewChanges, vReplicaStatus,
                  vSyncPoint, vLateBuffer, vCommitPoint,
                  vUUIDCounter, vCrashVector, 
                  vCrashVectorReps, vRecoveryReps >>

                       
            
\* Replica r receives MSyncPrepare, m
HandleSyncPrepare(r, m) ==
  LET
    newLog == m.log \o GetUnSyncLogs(vLog[r], LastLog(m.log) )
  IN
  /\ vReplicaStatus[r] = StNormal
  /\ m.viewID          = vViewID[r]
  /\ m.sender          = Leader(vViewID[r])
  /\ IF     vSyncPoint[r]  < Len(m.log) THEN
            /\ vSyncPoint' = [vSyncPoint EXCEPT ![r] = Len(m.log)]
            /\ vLog'       = [ vLog EXCEPT ![r] = newLog ]
            /\ Send({[   mtype      |-> MSlowReply,
                         sender     |-> r,
                         dest       |-> m.log[i].clientID,
                         viewID     |-> m.viewID,
                         requestID  |-> m.log[i].requestID,
                         logSlotNum |-> i ] : i \in (1..Len(m.log))})
     ELSE
            UNCHANGED <<vLog, vSyncPoint >>
  /\ Send({[ mtype         |-> MSyncRep,
             sender        |-> r,
             dest          |-> m.sender,
             viewID        |-> vViewID[r],
             logSlotNumber |-> Len(m.log) ]}
          )
  /\ UNCHANGED <<clientVars, vEarlyBuffer, vViewID,  vReplicaClock,
                 vLastNormView, vViewChanges, vReplicaStatus, 
                 vLateBuffer, vTentativeSync, vSyncReps, vCommitPoint,
                 vUUIDCounter, vCrashVector, 
                 vCrashVectorReps, vRecoveryReps>>

                       
\* Replica r receives MSyncRep, m
HandleSyncRep(r, m) ==
  /\ m.viewID          = vViewID[r]
  /\ vReplicaStatus[r] = StNormal
  /\ vSyncReps'        = [ vSyncReps EXCEPT ![r] = vSyncReps[r] \cup { m } ]
  /\ LET isViewPromise(M) == /\ { n.sender : n \in M } \in Quorums
                             /\ \E n \in M : n.sender = r
         sRMs             == { n \in vSyncReps'[r] :
                                 /\ n.mtype         = MSyncRep
                                 /\ n.viewID        = vViewID[r]
                                 /\ n.logSlotNumber = vTentativeSync[r] }
         committedLog     == IF vTentativeSync[r] >= 1 THEN
                               SubSeq(vLog[r], 1, vTentativeSync[r])
                             ELSE
                               << >>
     IN
       IF isViewPromise(sRMs) THEN
         /\ Send({[ mtype         |-> MSyncCommit,
                    sender        |-> r,
                    dest          |-> d,
                    viewID        |-> vViewID[r],
                    log           |-> committedLog] :
                    d \in Replicas })
         /\ vCommitPoint' =  [ vCommitPoint EXCEPT ![r] = vTentativeSync[r] ]
       ELSE
         UNCHANGED << networkVars, vCommitPoint >>
  /\ UNCHANGED  << clientVars, vLog, vEarlyBuffer, vViewID,
                   vReplicaClock, vLastNormView, vViewChanges,  
                   vReplicaStatus, vSyncPoint, vLateBuffer, 
                   vTentativeSync, vUUIDCounter, vCrashVector, 
                   vCrashVectorReps, vRecoveryReps >>


\* Replica r receives MSyncCommit, m
HandleSyncCommit(r, m) ==
  LET
    newLog == m.log \o GetUnSyncLogs(vLog[r], LastLog(m.log) )
  IN
  /\ vReplicaStatus[r] = StNormal
  /\ m.viewID          = vViewID[r]
  /\ m.sender          = Leader(vViewID[r])
  /\ IF  Len(m.log) <=  vCommitPoint[r] THEN
        UNCHANGED <<vCommitPoint, vLog>>
     ELSE
        /\ vLog'        = [ vLog EXCEPT ![r] = newLog ]
        /\ vCommitPoint'  = [ vCommitPoint  EXCEPT ![r] = Len(m.log) ]
        /\ Send({[ mtype      |-> MSlowReply,
                   sender     |-> r,
                   dest       |-> m.log[i].clientID,
                   viewID     |-> m.viewID,
                   requestID  |-> m.log[i].requestID,
                   logSlotNum |-> i ] : i \in (1..Len(m.log))})
  /\ UNCHANGED << networkVars, clientVars,  vEarlyBuffer, 
                  vViewID,  vReplicaClock,  vLastNormView, vViewChanges, 
                  vReplicaStatus, vSyncPoint, vLateBuffer, 
                  vTentativeSync, vSyncReps, 
                  vUUIDCounter, vCrashVector, 
                  vCrashVectorReps, vRecoveryReps >>

--------------------------------------------------------------------------------
(* `^\textbf{\large Invariants and Helper Functions}^' *)
    
(*
  A request/log is committed in two possible cases:
  (1) A fast quorum has sent either slow-reply messages, or fast-reply messages with consistent hashes [Fast Path]
  (2) A simple quorum has sent slow-reply messages [Slow Path]
  Both quorums should include the leader
*)

\* Check whether log <clientID, requestID> is committed at position logSlotNum
Committed(clientID, requestID, logSlotNum) ==
    \* Fast path
    \/ \E M \in SUBSET ({m \in messages : /\ \/ m.mtype = MFastReply
                                             \/ m.mtype = MSlowReply 
                                          /\ m.logSlotNum = logSlotNum
                                          /\ m.dest = clientID 
                                          /\ m.requestID = requestID }) :
        \* Sent from a fast quorum
        /\ { m.sender : m \in M } \in FastQuorums
        \* Matching view-id
        /\ \E m1 \in M : \A m2 \in M : m1.viewID = m2.viewID
        \* One from the leader
        /\ \E m \in M : m.sender = Leader(m.viewID)
        \* Hash values are consistent
        /\  LET 
                leaderReply == CHOOSE m \in M : m.sender = Leader(m.viewID)
            IN
            \A m1 \in M : IF m1.mtype = MFastReply THEN 
                             m1.hash = leaderReply.hash 
                          ELSE 
                             TRUE  \* SlowReply has consistent hash for sure
    \* Slow path
    \/ \E M \in SUBSET ({m \in messages : /\ \/ m.mtype = MSlowReply
                                             \/ /\ m.mtype = MFastReply  \* Leader only sends fast-reply
                                                /\ m.sender =Leader(m.viewID)
                                          /\ m.logSlotNum = logSlotNum
                                          /\ m.dest = clientID
                                          /\ m.requestID = requestID }) : 
        /\ { m.sender : m \in M } \in Quorums
        \* Matching view-id
        /\ \E m1 \in M : \A m2 \in M : m1.viewID = m2.viewID
        \* One from the leader
        /\ \E m \in M : m.sender = Leader(m.viewID)
 
 
 \* Check whether log <clientID, requestID> is committed in view viewID
 CommittedInView(clientID, requestID, viewID) ==
    \* Fast path
    \/ \E M \in SUBSET ({m \in messages : /\ \/ m.mtype = MFastReply
                                             \/ m.mtype = MSlowReply 
                                          /\ m.dest = clientID
                                          /\ m.requestID = requestID
                                          /\ m.viewID = viewID}) :
        \* Sent from a fast quorum
        /\ { m.sender : m \in M } \in FastQuorums
        \* One from the leader
        /\ \E m \in M : m.sender = Leader(m.viewID)
        \* Hash values are the same
        /\  LET 
                leaderReply == CHOOSE m \in M : m.sender = Leader(m.viewID)
            IN
            \A m1 \in M : IF m1.mtype = MFastReply THEN 
                             m1.hash = leaderReply.hash 
                          ELSE 
                             TRUE  \* SlowReply has consistent hash for sure
    \* Slow path
    \/ \E M \in SUBSET ({m \in messages : /\ \/ m.mtype = MSlowReply
                                             \/ /\ m.mtype = MFastReply  \* Leader only sends fast-reply
                                                /\ m.sender = Leader(m.viewID)
                                          /\ m.dest = clientID
                                          /\ m.requestID = requestID
                                          /\ m.viewID = viewID}) : 
        /\ { m.sender : m \in M } \in Quorums
        \* Hash values are the same
        /\ \E m1 \in M : \A m2 \in M : m1.hash = m2.hash
        \* One from the leader
        /\ \E m \in M : m.sender = Leader(m.viewID)
              
                
SystemRecovered(viewID) ==  /\ \E RM \in SUBSET(Replicas): 
                               /\ Cardinality(RM) >= QuorumSize
                               /\ \A r \in RM: vLastNormView[r] >= viewID
                               /\ \A r \in RM: vReplicaStatus[r] = StNormal \* These replicas must be normal
                            \* The leader of this view has also recovered or even goes beyond this view 
                            /\  vLastNormView[Leader(viewID)] >= viewID

(* `^\textbf{Invariants}^' *)
\* Durability: Committed Requests always survive failure
\* i.e. If a request is committed in one view, then it will remain committed in the higher views
\* One thing to note, the check of "committed" only happens when the system is still "normal"
\* While the system is under recovery (i.e. less than f+1 replicas are normal), 
\* the check of committed does not make sense
Durability == \A v1, v2 \in 1..MaxViews:
                \* If a request is committed in lower view (v1,), 
                \* it is impossible to make this request uncommited in higher view (v2)
                   ~(/\ v1 < v2 
                     \* To check Durability of request in higher views, 
                     \* the system should have entered the higher views
                     /\ SystemRecovered(v2)
                     /\ \E c \in Clients :
                        \E r \in 1..MaxReqNum:
                            /\ CommittedInView(c,r, v1)
                            /\ ~CommittedInView(c,r, v2))

\* Consistency: Committed requests have the same history even after view changes
\* i.e. If a request is committed in a lower view (v1), then (based on Durability Property)
\* it remains committed in higher view (v2)
\* Consistency requires the history of the request (i.e. all the request before this request) remain the same                         
Consistency == 
     \A v1, v2 \in 1..MaxViews:   
              ~(/\ v1 < v2
                \* To check Consistency of request in higher views, 
                \* the system should have entered the higher views
                /\ SystemRecovered(v2) 
                /\ \E c \in Clients :
                   \E r \in 1..MaxReqNum:
                   \E t \in 1..MaxTime:
                     \* Durability has been checked in another invariant
                     /\ CommittedInView(c,r, v1)
                     /\ CommittedInView(c,r, v2)
                     /\ LET 
                            v1LeaderReply == CHOOSE m \in messages: 
                                                /\ m.mtype = MFastReply
                                                /\ m.deadline = t
                                                /\ m.dest = c 
                                                /\ m.requestID = r
                                                /\ m.viewID = v1
                                                /\ m.sender = Leader(v1) 
                            v2LeaderReply == CHOOSE m \in messages: 
                                                /\ m.mtype = MFastReply
                                                /\ m.deadline = t
                                                /\ m.dest = c 
                                                /\ m.requestID = r
                                                /\ m.viewID = v2
                                                /\ m.sender = Leader(v2)                                                                    
                        IN
                           v1LeaderReply.hash /= v2LeaderReply.hash)  
                            
\* Linearizability: Only one request can be committed for a given position
\* i.e. If one request has committed at position i, then no contrary observation can be made
\* i.e. there cannot be a second request committed at the same position
Linearizability ==
  LET
    maxLogPosition == Max({1} \cup
      { m.logSlotNum : m \in {m \in messages : 
                          \/ m.mtype = MFastReply
                          \/ m.mtype = MSlowReply } })
  IN ~(\E c1, c2 \in Clients :
       \E r1, r2 \in 1..MaxReqNum:
         /\ << c1, r1 >> /= << c2, r2 >>
         /\ \E i \in (1 .. maxLogPosition) :
            /\ Committed(c1, r1, i)
            /\ Committed(c2, r2, i)
      )

(* `~
SyncSafety == \A r \in Replicas :
              \A i \in 1..vSyncPoint[r] :
              IF SystemRecovered(vViewID[r]) THEN
                \* Committed can only be checked when the system is recovered 
                \* (i.e. when there are f+1 replicas alive)
                Committed(vLog[r][i].ta,vLog[r][i].clientID, vLog[r][i].reqID, i)
              ELSE
                TRUE
 ~'
 *)               
--------------------------------------------------------------------------------
(* `^\textbf{\large Main Transition Function}^' *)

Next == \* Handle Messages
    \/ \E m \in messages : 
                        /\ m.mtype = MClientRequest
                        /\ m \notin vReplicaProcessed[m.dest]
                        /\ HandleClientRequest(m.dest, m)
                        /\ vReplicaProcessed' = 
                            [vReplicaProcessed EXCEPT ![m.dest] = 
                            vReplicaProcessed[m.dest] \cup { Msg2RLog(m, m.dest) } ]
                        /\ UNCHANGED vClientProcessed
                        /\ DebugAction' = << "HandleClientRequest", m >>
                            
    \/ \E m \in messages : 
                        /\ m.mtype = MViewChangeReq
                        /\ m \notin vReplicaProcessed[m.dest]
                        /\ HandleViewChangeReq(m.dest, m)  
                        /\ vReplicaProcessed' = 
                            [vReplicaProcessed EXCEPT ![m.dest] = 
                            vReplicaProcessed[m.dest] \cup { Msg2RLog(m, m.dest) } ]
                        /\ UNCHANGED vClientProcessed
                        /\ DebugAction' = << "HandleViewChangeReq", m >>
                                                                
    \/ \E m \in messages : 
                        /\ m.mtype = MViewChange
                        /\ m \notin vReplicaProcessed[m.dest]
                        /\ HandleViewChange(m.dest, m)
                        /\ vReplicaProcessed' = 
                            [vReplicaProcessed EXCEPT ![m.dest] = 
                            vReplicaProcessed[m.dest] \cup { Msg2RLog(m, m.dest) } ]
                        /\ UNCHANGED vClientProcessed
                        /\ DebugAction' = << "HandleViewChange", m >>
                            
    \/ \E m \in messages : 
                        /\ m.mtype = MStartView
                        /\ m \notin vReplicaProcessed[m.dest]
                        /\ HandleStartView(m.dest, m)
                        /\ vReplicaProcessed' = 
                            [vReplicaProcessed EXCEPT ![m.dest] = 
                            vReplicaProcessed[m.dest] \cup { Msg2RLog(m, m.dest) } ]
                        /\ UNCHANGED vClientProcessed
                        /\ DebugAction' = << "HandleStartView", m >>
    
    \/ \E m \in messages : 
                        /\ m.mtype = MSyncPrepare
                        /\ m \notin vReplicaProcessed[m.dest]
                        /\ HandleSyncPrepare(m.dest, m)
                        /\ vReplicaProcessed' = 
                            [vReplicaProcessed EXCEPT ![m.dest] =
                                vReplicaProcessed[m.dest] \cup { Msg2RLog(m, m.dest) } ]
                        /\ UNCHANGED vClientProcessed
                        /\ DebugAction' = << "HandleSyncPrepare", m >>
                            
    \/ \E m \in messages :
                        /\ m.mtype = MSyncRep
                        /\ m \notin vReplicaProcessed[m.dest]
                        /\ HandleSyncRep(m.dest, m)
                        /\ vReplicaProcessed' = 
                            [vReplicaProcessed EXCEPT ![m.dest] = 
                            vReplicaProcessed[m.dest] \cup { Msg2RLog(m, m.dest) } ]
                        /\ UNCHANGED vClientProcessed
                        /\ DebugAction' = << "HandleSyncRep", m >>
    \/ \E m \in messages :
                        /\ m.mtype = MSyncCommit
                        /\ m \notin vReplicaProcessed[m.dest]
                        /\ HandleSyncCommit(m.dest, m)
                        /\ vReplicaProcessed' = 
                            [vReplicaProcessed EXCEPT ![m.dest] = 
                            vReplicaProcessed[m.dest] \cup { Msg2RLog(m, m.dest) } ]
                        /\ UNCHANGED vClientProcessed
                        /\ DebugAction' = << "HandleSyncCommit", m >>
                            
    \/ \E m \in messages:
                        /\ m.mtype = MMissEntryRequest
                        /\ m \notin vReplicaProcessed[m.dest]
                        /\ HandleMissEntryRequest(m.dest, m)
                        /\ vReplicaProcessed' = 
                            [vReplicaProcessed EXCEPT ![m.dest] = 
                             vReplicaProcessed[m.dest] \cup { Msg2RLog(m, m.dest) } ]
                        /\ UNCHANGED vClientProcessed
                        /\ DebugAction' = << "HandleMissEntryRequest", m >>
                
    \/ \E m \in messages:
                        /\ m.mtype = MMissEntryReply
                        /\ m \notin vReplicaProcessed[m.dest]
                        /\ HandleMissEntryReply(m.dest, m)
                        /\ vReplicaProcessed' = 
                            [vReplicaProcessed EXCEPT ![m.dest] = 
                             vReplicaProcessed[m.dest] \cup { Msg2RLog(m, m.dest) } ]
                        /\ UNCHANGED vClientProcessed
                        /\ DebugAction' = << "HandleMissEntryReply", m >>
                          
    \* Client Actions
    \/ \E c \in Clients :  
                        /\ vClientReqNum[c] < MaxReqNum
                        /\ ClientSendRequest(c)
                        /\ UNCHANGED << vReplicaProcessed, vClientProcessed >>
                        /\ DebugAction' = << "ClientSendRequest", "" >>                              
                                                                
    \* Start Synchronization
    \/ \E r \in Replicas :  
                        /\ StartSync(r)
                        /\ UNCHANGED << vReplicaProcessed, vClientProcessed >>
                        /\ DebugAction' = << "StartSync", "" >>                
    
    \* Replica Fail
    \/ \E r \in Replicas :
                        /\ vReplicaStatus[r] = StNormal
                        /\ StartReplicaFail(r)
                        /\ UNCHANGED << vReplicaProcessed, vClientProcessed >>
                        /\ DebugAction' = << "StartReplicaFail", "" >>
    
    
    \* Leader Change
    \/ \E r \in Replicas : 
                        /\ vViewID[r] < MaxViews
                        /\ StartLeaderChange(r)
                        /\ UNCHANGED << vReplicaProcessed, vClientProcessed >>
                        /\ DebugAction' = << "StartLeaderChange", "" >>
                        
    \* Replica Rejoin                    
    \/ \E r \in Replicas :
                        /\ vReplicaStatus[r] = StRecovering
                        /\ StartReplicaRecovery(r)
                        /\ UNCHANGED << vReplicaProcessed, vClientProcessed >>
                        /\ DebugAction' = << "StartReplicaRecovery", "" >>
    
    \* Replica Actions:
    \/ \E r \in Replicas:
                        /\ StartIndexSync(r)
                        /\ UNCHANGED << vReplicaProcessed, vClientProcessed >>
                        /\ DebugAction' = << "StartIndexSync", "" >>
                        
    \/ \E r \in Replicas:
                    /\ FlushEarlyBuffer(r)
                    /\ UNCHANGED << vReplicaProcessed, vClientProcessed >>
                    /\ DebugAction' = << "FlushReplicaBuffer", "" >>
    \* Clock Move
    \/ \E r \in Replicas : 
                        /\ ReplicaClockMove(r)                             
                        /\ UNCHANGED << vReplicaProcessed, vClientProcessed >>
                        /\ DebugAction' = << "ReplicaClockMove", "" >>
    
    \/ \E c \in Clients  : 
                        /\ ClientClockMove(c)
                        /\ UNCHANGED << vReplicaProcessed, vClientProcessed >>                          
                        /\ DebugAction' = << "ClientClockMove", "" >>

                    
================================================================================
