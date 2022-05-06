#ifndef NEZHA_UTILS_H
#define NEZHA_UTILS_H

#include <stdio.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <ev.h>
#include <strings.h>
#include <chrono>
#include <unistd.h>
#include <arpa/inet.h>
#include <openssl/sha.h>
#include "nezha/nezha-proto.pb.h"

union SHA_HASH {
    uint32_t item[5];
    unsigned char hash[SHA_DIGEST_LENGTH];
    SHA_HASH() {
        memset(item, 0, sizeof(uint32_t) * 5);
    }
    SHA_HASH(const SHA_HASH& h) {
        memcpy(item, h.item, sizeof(uint32_t) * 5);
    }
    void XOR(const SHA_HASH& h) {
        item[0] ^= h.item[0];
        item[1] ^= h.item[1];
        item[2] ^= h.item[2];
        item[3] ^= h.item[3];
        item[4] ^= h.item[4];
    }
};

struct LogEntry {
    uint64_t deadline;
    uint64_t reqKey;
    SHA_HASH hash;
    std::string result;
    LogEntry() {}
    LogEntry(const uint64_t d, const uint64_t r, const SHA_HASH& h, const std::string& re) :deadline(d), reqKey(r), hash(h), result(re) {}

};

inline SHA_HASH CalculateHash(uint64_t deadline, uint64_t reqKey) {
    SHA_HASH hash;
    const uint32_t contentLen = sizeof(uint64_t) + sizeof(uint32_t) + sizeof(uint32_t);
    unsigned char content[contentLen];
    memcpy(content, &deadline, sizeof(uint64_t));
    memcpy(content + sizeof(uint64_t), &reqKey, sizeof(uint64_t));
    SHA1(content, contentLen, hash.hash);
    return hash;
}

inline SHA_HASH CalculateHash(uint64_t deadline, uint32_t clientId, uint64_t reqId) {
    SHA_HASH hash;
    const uint32_t contentLen = sizeof(uint64_t) + sizeof(uint32_t) + sizeof(uint32_t);
    unsigned char content[contentLen];
    memcpy(content, &deadline, sizeof(uint64_t));
    memcpy(content + sizeof(uint64_t), &clientId, sizeof(uint32_t));
    memcpy(content + sizeof(uint64_t) + sizeof(uint32_t), &reqId, sizeof(uint32_t));
    SHA1(content, contentLen, hash.hash);
    return hash;
}

// Get Current Microsecond Timestamp
inline uint64_t GetMicrosecondTimestamp() {
    auto tse = std::chrono::system_clock::now().time_since_epoch();
    return std::chrono::duration_cast<std::chrono::microseconds>(tse).count();
}


#endif 