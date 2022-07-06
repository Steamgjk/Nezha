#include "lib/utils.h"


namespace  ReplicaStatus {
    char NORMAL = 1;
    char VIEWCHANGE = 2;
    char RECOVERING = 3;
    char TERMINATED = 4;
};


namespace MessageType {
    char CLIENT_REQUEST = 1;
    char LEADER_REQUEST = 2;
    char SYNC_INDEX = 3;
    char MISSED_INDEX_ASK = 4;
    char MISSED_REQ_ASK = 5;
    char FAST_REPLY = 6;
    char SLOW_REPLY = 7;
    char COMMIT_REPLY = 8;
    char MISSED_REQ = 9;
    char VIEWCHANGE_REQ = 10;
    char VIEWCHANGE = 11;
    char START_VIEW = 12;
    char STATE_TRANSFER_REQUEST = 13;
    char STATE_TRANSFER_REPLY = 14;
    char CRASH_VECTOR_REQUEST = 15;
    char CRASH_VECTOR_REPLY = 16;
    char RECOVERY_REQUEST = 17;
    char RECOVERY_REPLY = 18;
    char SYNC_STATUS_REPORT = 19;
    char COMMIT_INSTRUCTION = 20;
    char SUSPEND_REPLY = 21;
    char ERROR_MSG = 22;
};


namespace StateTransferType {
    char SYNCED_ENTRY = 1;
    char UNSYNCED_ENTRY = 2;
}


SHA_HASH CalculateHash(uint64_t deadline, uint64_t reqKey) {
    SHA_HASH hash;
    const uint32_t contentLen = sizeof(uint64_t) + sizeof(uint32_t) + sizeof(uint32_t);
    unsigned char content[contentLen];
    memcpy(content, &deadline, sizeof(uint64_t));
    memcpy(content + sizeof(uint64_t), &reqKey, sizeof(uint64_t));
    SHA1(content, contentLen, hash.hash);
    return hash;
}

SHA_HASH CalculateHash(uint64_t deadline, uint32_t clientId, uint64_t reqId) {
    SHA_HASH hash;
    const uint32_t contentLen = sizeof(uint64_t) + sizeof(uint32_t) + sizeof(uint32_t);
    unsigned char content[contentLen];
    memcpy(content, &deadline, sizeof(uint64_t));
    memcpy(content + sizeof(uint64_t), &clientId, sizeof(uint32_t));
    memcpy(content + sizeof(uint64_t) + sizeof(uint32_t), &reqId, sizeof(uint32_t));
    SHA1(content, contentLen, hash.hash);
    return hash;
}


SHA_HASH CalculateHash(const unsigned char* content, const uint32_t contentLen) {
    SHA_HASH hash;
    SHA1(content, contentLen, hash.hash);
    return hash;
}

// Get Current Microsecond Timestamp
uint64_t GetMicrosecondTimestamp() {
    auto tse = std::chrono::system_clock::now().time_since_epoch();
    return std::chrono::duration_cast<std::chrono::microseconds>(tse).count();
}

