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

enum REPLICA_STATUS {
    NORMAL = 1,
    VIEWCHANGE = 2,
    RECOVERING = 3
};
enum REPLY_TYPE {
    FAST_REPLY = 1,
    SLOW_REPLY = 2,
    COMMIT_REPLY = 3
};

enum REQUEST_TYPE {
    CLIENT_REQUEST = 1,
    SYNC_INDEX,
    MISSED_INDEX_ASK,
    MISSED_REQ_ASK
};

enum SENDER_TYPE {
    INDEX_SYNC = 0,
    MISSED_REQ = 1,
    ASK_INDEX = 2,
    ASK_REQ = 3
};
const int MAX_SENDER_TYPE_NUM = 4;

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

struct MessageHeader {
    char msgType;
    uint32_t msgLen;
    MessageHeader(const char t, const uint32_t l) :msgType(t), msgLen(l) {}
};

struct LogEntry {
    uint64_t deadline;
    uint64_t reqKey;
    SHA_HASH myhash; // the hash value of this entry
    SHA_HASH hash; // the accumulative hash
    uint32_t opKey; // for commutativity optimization
    std::string result;
    uint64_t proxyId;
    LogEntry() {}
    LogEntry(const uint64_t d, const uint64_t r, const SHA_HASH& mh, const SHA_HASH& h, const uint32_t ok, const std::string& re, const uint64_t p) :deadline(d), reqKey(r), myhash(mh), hash(h), opKey(ok), result(re), proxyId(p) {}

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


inline SHA_HASH CalculateHash(const unsigned char* content, const uint32_t contentLen) {
    SHA_HASH hash;
    SHA1(content, contentLen, hash.hash);
    return hash;
}

// Get Current Microsecond Timestamp
inline uint64_t GetMicrosecondTimestamp() {
    auto tse = std::chrono::system_clock::now().time_since_epoch();
    return std::chrono::duration_cast<std::chrono::microseconds>(tse).count();
}


// Create socket fd and bind ip:port
inline int CreateSocketFd(const std::string& ip, const int port) {
    int fd = socket(PF_INET, SOCK_DGRAM, 0);
    if (fd < 0) {
        LOG(ERROR) << "Receiver Fd fail " << ip << ":" << port;
        return fd;
    }
    struct sockaddr_in addr;
    bzero(&addr, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = inet_addr(ip.c_str());
    // Bind socket to address
    int bindRet = bind(fd, (struct sockaddr*)&addr, sizeof(addr));
    if (bindRet != 0) {
        LOG(ERROR) << "bind error\t" << bindRet;
        return bindRet;
    }
    return fd;
}


#endif 