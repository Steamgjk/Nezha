#ifndef NEZHA_UTILS_H
#define NEZHA_UTILS_H

#include <stdio.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <ev.h>
#include <strings.h>
#include <cstring>
#include <chrono>
#include <unistd.h>
#include <arpa/inet.h>
#include <openssl/sha.h>
#include <glog/logging.h>
#include <gflags/gflags.h>


#define CONCURRENT_MAP_START_INDEX (2u)
#define CONCAT_UINT32(a, b) ((((uint64_t)a)<<32u)|(uint32_t)b)
#define HIGH_32BIT(a) ((uint32_t)(a>>32))
#define LOW_32BIT(a) ((uint32_t)a)

namespace  ReplicaStatus {
    extern char NORMAL;
    extern char VIEWCHANGE;
    extern char RECOVERING;
    extern char TERMINATED;
};


namespace MessageType {
    extern char CLIENT_REQUEST;
    extern char LEADER_REQUEST;
    extern char SYNC_INDEX;
    extern char MISSED_INDEX_ASK;
    extern char MISSED_REQ_ASK;
    extern char FAST_REPLY;
    extern char SLOW_REPLY;
    extern char COMMIT_REPLY;
    extern char MISSED_REQ;
    extern char VIEWCHANGE_REQ;
    extern char VIEWCHANGE;
    extern char START_VIEW;
    extern char STATE_TRANSFER_REQUEST;
    extern char STATE_TRANSFER_REPLY;
    extern char CRASH_VECTOR_REQUEST;
    extern char CRASH_VECTOR_REPLY;
    extern char RECOVERY_REQUEST;
    extern char RECOVERY_REPLY;
    extern char SYNC_STATUS_REPORT;
    extern char COMMIT_INSTRUCTION;
    extern char SUSPEND_REPLY;
    extern char ERROR_MSG;
};

namespace StateTransferType {
    extern char SYNCED_ENTRY;
    extern char UNSYNCED_ENTRY;
}


union SHA_HASH {
    uint32_t item[5];
    unsigned char hash[SHA_DIGEST_LENGTH];
    SHA_HASH() {
        memset(item, 0, sizeof(uint32_t) * 5);
    }
    SHA_HASH(const char* str, const uint32_t len) {
        if (len >= SHA_DIGEST_LENGTH) {
            memcpy(hash, str, SHA_DIGEST_LENGTH);
        }
        else {
            memcpy(hash, str, len);
        }
    }
    SHA_HASH(const SHA_HASH& h) {
        memcpy(item, h.item, sizeof(uint32_t) * 5);
    }
    SHA_HASH& operator=(const SHA_HASH& sh) {
        memcpy(item, sh.item, sizeof(uint32_t) * 5);
        return *this;
    }
    void XOR(const SHA_HASH& h) {
        item[0] ^= h.item[0];
        item[1] ^= h.item[1];
        item[2] ^= h.item[2];
        item[3] ^= h.item[3];
        item[4] ^= h.item[4];
    }
    std::string toString() {
        return (std::to_string(item[0]) + "-"
            + std::to_string(item[1]) + "-"
            + std::to_string(item[2]) + "-"
            + std::to_string(item[3]) + "-"
            + std::to_string(item[4])
            );
    }
};

SHA_HASH CalculateHash(uint64_t deadline, uint64_t reqKey);

SHA_HASH CalculateHash(uint64_t deadline, uint32_t clientId, uint64_t reqId);

SHA_HASH CalculateHash(const unsigned char* content, const uint32_t contentLen);

// Get Current Microsecond Timestamp
uint64_t GetMicrosecondTimestamp();

struct MessageHeader {
    char msgType;
    uint32_t msgLen;
    MessageHeader(const char t, const uint32_t l) :msgType(t), msgLen(l) {}
};


struct RequestBody {
    uint64_t deadline;
    uint64_t reqKey;
    uint32_t opKey; // for commutativity optimization
    uint64_t proxyId;
    std::string command;
    RequestBody() {}
    RequestBody(const uint64_t d, const uint64_t r, const uint32_t ok, const uint64_t p, const std::string& cmd) :deadline(d), reqKey(r), opKey(ok), proxyId(p), command(cmd) {}
    bool LessThan(const RequestBody& bigger) {
        return (deadline < bigger.deadline || (deadline == bigger.deadline && reqKey < bigger.reqKey));
    }
    bool LessThan(const std::pair<uint64_t, uint64_t>& bigger) {
        return (deadline < bigger.first || (deadline == bigger.first && reqKey < bigger.second));
    }
    bool LessOrEqual(const RequestBody& bigger) {
        return (deadline < bigger.deadline || (deadline == bigger.deadline && reqKey <= bigger.reqKey));
    }
    bool LessOrEqual(const std::pair<uint64_t, uint64_t>& bigger) {
        return (deadline < bigger.first || (deadline == bigger.first && reqKey <= bigger.second));
    }
};

struct LogEntry {
    // Request Body
    RequestBody body;
    /** Other atttributes */
    SHA_HASH myhash; // the hash value of this entry
    SHA_HASH hash; // the accumulative hash
    uint32_t prevLogId; // The log id of the next non-commutative request, initialized as 0
    uint32_t nextLogId; // The log id of the next non-commutative request, initialized as UINT_MAX
    std::string result;

    LogEntry() {}
    LogEntry(const RequestBody& rb, const SHA_HASH& mh, const SHA_HASH& h, const uint32_t prev, const uint32_t next, const std::string& re) :body(rb), myhash(mh), hash(h), prevLogId(prev), nextLogId(next), result(re) {}
    LogEntry(const uint64_t d, const uint64_t r, const uint32_t ok, const uint64_t p, const std::string& cmd, const SHA_HASH& mh, const SHA_HASH& h, const uint32_t prev, const uint32_t next, const std::string& re) :body(d, r, ok, p, cmd), myhash(mh), hash(h), prevLogId(prev), nextLogId(next), result(re) {}

    bool LessThan(const LogEntry& bigger) {
        return body.LessThan(bigger.body);
    }
    bool LessThan(const std::pair<uint64_t, uint64_t>& bigger) {
        return  body.LessThan(bigger);
    }
    bool LessOrEqual(const LogEntry& bigger) {
        return body.LessOrEqual(bigger.body);
    }
    bool LessOrEqual(const std::pair<uint64_t, uint64_t>& bigger) {
        return body.LessOrEqual(bigger);
    }
};


struct CrashVectorStruct {
    std::vector<uint32_t> cv_;
    uint32_t version_;
    SHA_HASH cvHash_;
    CrashVectorStruct(const std::vector<uint32_t>& c, const uint32_t v) :cv_(c), version_(v) {
        const uint32_t contentLen = c.size() * sizeof(uint32_t);
        const unsigned char* content = (const unsigned char*)(void*)(c.data());
        cvHash_ = CalculateHash(content, contentLen);
    }
    CrashVectorStruct(const CrashVectorStruct& c) :cv_(c.cv_), version_(c.version_), cvHash_(c.cvHash_) {}
};



#endif 