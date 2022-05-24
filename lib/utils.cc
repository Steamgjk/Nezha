#include "lib/utils.h"



namespace  ReplicaStatus {
    char NORMAL = 1;
    char VIEWCHANGE = 2;
    char RECOVERING = 3;
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
    char STARTVIEW = 12;
};


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

