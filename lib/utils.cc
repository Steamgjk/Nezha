#include "lib/utils.h"


namespace EndpointType {
    char UDP_ENDPOINT = 1;
    char TCP_ENDPOINT = 2;
}


namespace  ReplicaStatus {
    char NORMAL = 1;
    char VIEWCHANGE = 2;
    char RECOVERING = 3;
    char TERMINATED = 4;
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


Endpoint* CreateEndpoint(const uint32_t endpointType, const std::string& sip,
    const int sport, const bool isMasterReceiver) {
    if (endpointType == EndpointType::UDP_ENDPOINT) {
        return new UDPSocketEndpoint(sip, sport, isMasterReceiver);
    }
    else if (endpointType == EndpointType::TCP_ENDPOINT) {
        return new TCPSocketEndpoint(sip, sport, isMasterReceiver);
    }
    else {
        LOG(ERROR) << "Unknown endpoint type: " << endpointType;
        return NULL;
    }
}

EndpointMsgHandler* CreateMsgHandler(const uint32_t endpointType,
    std::function<void(MessageHeader*, char*, Address*, void*, Endpoint*)> msghdl,
    void* ctx) {
    if (endpointType == EndpointType::UDP_ENDPOINT) {
        return new UDPMsgHandler(msghdl, ctx);
    }
    else if (endpointType == EndpointType::TCP_ENDPOINT) {
        return new TCPMsgHandler(msghdl, ctx);
    }
    else {
        LOG(ERROR) << "Unknown endpoint type: " << endpointType;
        return NULL;
    }
}
