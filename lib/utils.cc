#include "lib/utils.h"

SHA_HASH CalculateHash(uint64_t deadline, uint64_t reqKey) {
  SHA_HASH hash;
  const uint32_t contentLen =
      sizeof(uint64_t) + sizeof(uint32_t) + sizeof(uint32_t);
  unsigned char content[contentLen];
  memcpy(content, &deadline, sizeof(uint64_t));
  memcpy(content + sizeof(uint64_t), &reqKey, sizeof(uint64_t));
  SHA1(content, contentLen, hash.hash);
  return hash;
}

// Get Current Microsecond Timestamp
uint64_t GetMicrosecondTimestamp() {
  auto tse = std::chrono::system_clock::now().time_since_epoch();
  return std::chrono::duration_cast<std::chrono::microseconds>(tse).count();
}

Endpoint* CreateEndpoint(const char endpointType, const std::string& sip,
                         const int sport, const bool isMasterReceiver) {
  if (endpointType == EndpointType::UDP_ENDPOINT) {
    return new UDPSocketEndpoint(sip, sport, isMasterReceiver);
  } else if (endpointType == EndpointType::GRPC_ENDPOINT) {
    // To support GRPC later
    return NULL;
  } else {
    LOG(ERROR) << "Unknown endpoint type: " << endpointType;
    return NULL;
  }
}

MessageHandler* CreateMsgHandler(const char endpointType,
                                 MessageHandlerFunc msghdl, void* ctx) {
  if (endpointType == EndpointType::UDP_ENDPOINT) {
    return new UDPMsgHandler(msghdl, ctx);
  } else if (endpointType == EndpointType::GRPC_ENDPOINT) {
    // To support GRPC later
    return NULL;
  } else {
    LOG(ERROR) << "Unknown endpoint type: " << endpointType;
    return NULL;
  }
}
