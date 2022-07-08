#include <stdint.h>
#ifndef NEZHA_MESSAGE_TYPE_H
#define NEZHA_MESSAGE_TYPE_H

#define CONCURRENT_MAP_START_INDEX (2u)
#define CONCAT_UINT32(a, b) ((((uint64_t)a)<<32u)|(uint32_t)b)
#define HIGH_32BIT(a) ((uint32_t)(a>>32))
#define LOW_32BIT(a) ((uint32_t)a)

struct MessageHeader {
    char msgType;
    uint32_t msgLen;
    MessageHeader(const char t, const uint32_t l) :msgType(t), msgLen(l) {}
};


namespace MessageType {
    extern char  CLIENT_REQUEST;
    extern char  LEADER_REQUEST;
    extern char  SYNC_INDEX;
    extern char  MISSED_INDEX_ASK;
    extern char  MISSED_REQ_ASK;
    extern char  FAST_REPLY;
    extern char  SLOW_REPLY;
    extern char  COMMIT_REPLY;
    extern char  MISSED_REQ;
    extern char  VIEWCHANGE_REQ;
    extern char  VIEWCHANGE;
    extern char  START_VIEW;
    extern char  STATE_TRANSFER_REQUEST;
    extern char  STATE_TRANSFER_REPLY;
    extern char  CRASH_VECTOR_REQUEST;
    extern char  CRASH_VECTOR_REPLY;
    extern char  RECOVERY_REQUEST;
    extern char  RECOVERY_REPLY;
    extern char  SYNC_STATUS_REPORT;
    extern char  COMMIT_INSTRUCTION;
    extern char  SUSPEND_REPLY;
    extern char  ERROR_MSG;
};

#endif