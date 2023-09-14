#ifndef NEZHA_COMMON_TYPE_H
#define NEZHA_COMMON_TYPE_H

/** We currently only support UDP endpoint, and GRPC endpoint will be supported
 * in the near future*/
enum EndpointType {
  UDP_ENDPOINT = 1,
  GRPC_ENDPOINT  // To be supported
};

/** Refer to Sec 5 of our paper for detailed explanation of different replica
 * statuses */
enum ReplicaStatus { NORMAL = 1, VIEWCHANGE, RECOVERING, TERMINATED };

/** A LogEntry is INITIAL at the beginning, then it may switch to either
 * IN_PROCESS->PROCESSED->REPLIED  or directly IN_LATEBUFFER */
enum EntryStatus {
  INITIAL = 1,
  IN_PROCESS,
  IN_LATEBUFFER,
  PROCESSED,
  TO_SLOW_REPLY,
  REPLIED
};

/**
 * The message types are defined according to the proto files and the
 * information will be included in each message to facilitate
 * serialize/deserialize proto messages
 */
enum MessageType {
  CLIENT_REQUEST = 1,
  LEADER_REQUEST,
  SYNC_INDEX,
  MISSED_INDEX_ASK,
  MISSED_REQ_ASK,
  FAST_REPLY,
  SLOW_REPLY,
  COMMIT_REPLY,
  MISSED_REQ,
  VIEWCHANGE_REQ,
  VIEWCHANGE_MSG,
  START_VIEW,
  STATE_TRANSFER_REQUEST,
  STATE_TRANSFER_REPLY,
  CRASH_VECTOR_REQUEST,
  CRASH_VECTOR_REPLY,
  RECOVERY_REQUEST,
  RECOVERY_REPLY,
  SYNC_STATUS_REPORT,
  COMMIT_INSTRUCTION,
  SUSPEND_REPLY,
  ERROR_MSG
};

#endif