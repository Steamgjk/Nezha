#ifndef NEZHA_UTILS_H
#define NEZHA_UTILS_H

#include <arpa/inet.h>
#include <ev.h>
#include <glog/logging.h>
#include <junction/ConcurrentMap_Leapfrog.h>
#include <netinet/in.h>
#include <openssl/sha.h>
#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <unistd.h>
#include <chrono>
#include <cstring>
#include "concurrentqueue.h"
#include "gflags/gflags.h"
#include "lib/udp_socket_endpoint.h"

template <typename T1>
using ConcurrentQueue = moodycamel::ConcurrentQueue<T1>;
template <typename T1, typename T2>
using ConcurrentMap = junction::ConcurrentMap_Leapfrog<T1, T2>;

/** The concurrent map we used (i.e.junction::ConcurrentMap) reserves 0 and 1 ,
 * so the start value should be 2 */
#define CONCURRENT_MAP_START_INDEX (2u)
#define CONCAT_UINT32(a, b) ((((uint64_t)a) << 32u) | (uint32_t)b)
/** Get the high/low 32bits of a uint64 */
#define HIGH_32BIT(a) ((uint32_t)(a >> 32))
#define LOW_32BIT(a) ((uint32_t)a)

// Since <deadline, reqKey> is sufficient to uniquely identify one request, we
// calculate hash based on them to represent the corresponding request/log
SHA_HASH CalculateHash(uint64_t deadline, uint64_t reqKey);

// Get Current Microsecond Timestamp
uint64_t GetMicrosecondTimestamp();

// Factory function, to create different types of endpoints and msghandlers
Endpoint* CreateEndpoint(const char endpointType, const std::string& sip = "",
                         const int sport = -1,
                         const bool isMasterReceiver = false);

MessageHandler* CreateMsgHandler(
    const char endpointType,
    std::function<void(MessageHeader*, char*, Address*, void*)> msghdl,
    void* ctx = NULL);

#endif