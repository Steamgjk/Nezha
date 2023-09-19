

#ifndef NEZHA_COMMON_STRUCT_H
#define NEZHA_COMMON_STRUCT_H
#include <openssl/sha.h>
#include <stdio.h>
#include <stdlib.h>
#include <cstring>
#include <string>
#include <vector>
#include "lib/common_type.h"

/**
 * Nezha relies on proto messages to communicate.
 * When the proto message has been serialized and is about to be sent by the
 * endpoint, MessageHeader is prepended to the head of the proto message (refer
 * to SendMsgTo in udp_socket_endpoint.h), which describes the type of proto
 * message and its length. In this way, when the receiver endpoint receives the
 * message, it can know the type and length of the proto message, then it can
 * choose the proper way to deserialize it.
 */
struct MessageHeader {
  char msgType;
  uint32_t msgLen;
  MessageHeader(const char t, const uint32_t l) : msgType(t), msgLen(l) {}
};

/**
 * SHA_HASH is included in the FastReply message to represent the replica state
 * of replica. More details at Sec 5.2 of our paper
 * https://arxiv.org/pdf/2206.03285.pdf
 */
union SHA_HASH {
  uint32_t item[5];
  unsigned char hash[SHA_DIGEST_LENGTH];
  SHA_HASH() { memset(item, 0, sizeof(uint32_t) * 5); }
  SHA_HASH(const char* str, const uint32_t len) {
    if (len >= SHA_DIGEST_LENGTH) {
      memcpy(hash, str, SHA_DIGEST_LENGTH);
    } else {
      memcpy(hash, str, len);
    }
  }
  SHA_HASH(const SHA_HASH& h) { memcpy(item, h.item, sizeof(uint32_t) * 5); }
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
    return (std::to_string(item[0]) + "-" + std::to_string(item[1]) + "-" +
            std::to_string(item[2]) + "-" + std::to_string(item[3]) + "-" +
            std::to_string(item[4]));
  }
};

/** When request is received by the replica, it will be first converted to
 * RequestBody, which includes all the useful information of the request */
struct RequestBody {
  uint64_t deadline;
  uint64_t reqKey;  // reqKey uniquely identifies the request on this replica,
                    // it is concated by the clientId and reqId. With reqKey,
                    // the replica can easily check whether this request has
                    // been previously received or not.
  uint32_t opKey;   // opKey indicates which key the request is operating on (
                    // imagine we are working on a database system and different
                    // requests wil read/write different keys). opKey is
                    // important for commutativity optimization. dd
  uint64_t proxyId;     // proxyId indicates which proxy delivers the request to
                        // the replica, and later replicas will send the
                        // corresponding reply to the proxy.
  std::string command;  // command is the content to execute
  bool isWrite;
  RequestBody() {}
  RequestBody(const uint64_t d, const uint64_t r, const uint32_t ok,
              const uint64_t p, const std::string& cmd, const bool isw)
      : deadline(d),
        reqKey(r),
        opKey(ok),
        proxyId(p),
        command(cmd),
        isWrite(isw) {}

  /** The following methods are used to compare different requests so as to
   * decide their order*/
  bool LessThan(const RequestBody& bigger) {
    return (deadline < bigger.deadline ||
            (deadline == bigger.deadline && reqKey < bigger.reqKey));
  }
  bool LessThan(const std::pair<uint64_t, uint64_t>& bigger) {
    return (deadline < bigger.first ||
            (deadline == bigger.first && reqKey < bigger.second));
  }
  bool LessOrEqual(const RequestBody& bigger) {
    return (deadline < bigger.deadline ||
            (deadline == bigger.deadline && reqKey <= bigger.reqKey));
  }
  bool LessOrEqual(const std::pair<uint64_t, uint64_t>& bigger) {
    return (deadline < bigger.first ||
            (deadline == bigger.first && reqKey <= bigger.second));
  }
};

/**
 * After RequestBody is processed and eventually replied, it will be converted
 * into a LogEntry, and stored in the replica.
 * LogEntry, compares with RequestBody, includes more information
 */
struct LogEntry {
  // Request Body
  RequestBody body;
  SHA_HASH entryHash;  // The hash value of this **single** entry
  SHA_HASH logHash;  // The accumulative hash, which is calculated based on all
                     // the log entries from the beginning to this entry

  /** prevNonCommutative and nextNonCommutative organize the LogEntries as a
   * skiplist, and easier and more efficient to traverse/modify/delete */
  LogEntry* prevNonCommutative;  // The previous non-commutative entry
  LogEntry* nextNonCommutative;  // The next non-commutative entry

  LogEntry* prevNonCommutativeWrite;  // The entry's prevNonCommutative may be a
                                      // write, or may be a read
  // But only the prevNonCommutativeWrite is used to calculate the incremental
  // hash, see Sec 8.2 of Nezha's Technical Report
  LogEntry* nextNonCommutativeWrite;

  /** prev and next organizes the LogEntries as a link list, and easier to
   * traverse/modify/delete */

  LogEntry* prev;  // The previous LogEntry pointer
  LogEntry* next;  // The next LogEntry pointer

  std::string result;  // The execution result of the LogEntry
  char status;         //
  uint32_t logId;  // The logId (the position of the LogEntry in the list) of
                   // the entry

  LogEntry()
      : prevNonCommutative(NULL),
        nextNonCommutative(NULL),
        prevNonCommutativeWrite(NULL),
        nextNonCommutativeWrite(NULL),
        prev(NULL),
        next(NULL),
        result(""),
        status(EntryStatus::INITIAL),
        logId(0) {}
  LogEntry(const RequestBody& rb, const SHA_HASH& eh, const SHA_HASH& h,
           LogEntry* prevNonComm = NULL, LogEntry* nextNonComm = NULL,
           LogEntry* preNonCOmmW = NULL, LogEntry* nextNonCommW = NULL,
           LogEntry* pre = NULL, LogEntry* nxt = NULL,
           const std::string& re = "", const char sts = EntryStatus::INITIAL,
           const uint32_t lid = 0)
      : body(rb),
        entryHash(eh),
        logHash(h),
        prevNonCommutative(prevNonComm),
        nextNonCommutative(nextNonComm),
        prevNonCommutativeWrite(preNonCOmmW),
        nextNonCommutativeWrite(nextNonCommW),
        prev(pre),
        next(nxt),
        result(re),
        status(sts),
        logId(lid) {}
  LogEntry(const uint64_t d, const uint64_t r, const uint32_t ok,
           const uint64_t p, const std::string& cmd, const bool& isw,
           const SHA_HASH& eh, const SHA_HASH& h, LogEntry* prevNonComm = NULL,
           LogEntry* nextNonComm = NULL, LogEntry* preNonCOmmW = NULL,
           LogEntry* nextNonCommW = NULL, LogEntry* pre = NULL,
           LogEntry* nxt = NULL, const std::string& re = "",
           const char sts = EntryStatus::INITIAL, const uint32_t lid = 0)
      : body(d, r, ok, p, cmd, isw),
        entryHash(eh),
        logHash(h),
        prevNonCommutative(prevNonComm),
        nextNonCommutative(nextNonComm),
        prevNonCommutativeWrite(preNonCOmmW),
        nextNonCommutativeWrite(nextNonCommW),
        prev(pre),
        next(nxt),
        result(re),
        status(sts),
        logId(lid) {}

  bool LessThan(const LogEntry& bigger) { return body.LessThan(bigger.body); }
  bool LessThan(const std::pair<uint64_t, uint64_t>& bigger) {
    return body.LessThan(bigger);
  }
  bool LessOrEqual(const LogEntry& bigger) {
    return body.LessOrEqual(bigger.body);
  }
  bool LessOrEqual(const std::pair<uint64_t, uint64_t>& bigger) {
    return body.LessOrEqual(bigger);
  }
};

/**
 * CrashVectorStruct is necessary for Nezha to avoid stray messages, details in
 * Appendix A.1 and Appendix J of our paper
 */
struct CrashVectorStruct {
  std::vector<uint32_t> cv_;
  uint32_t version_;  // Newer crash vector will have a larger version_
  SHA_HASH cvHash_;
  CrashVectorStruct(const std::vector<uint32_t>& c, const uint32_t v)
      : cv_(c), version_(v) {
    const uint32_t contentLen = c.size() * sizeof(uint32_t);
    const unsigned char* content = (const unsigned char*)(void*)(c.data());
    SHA1(content, contentLen, cvHash_.hash);
  }
  CrashVectorStruct(const CrashVectorStruct& c)
      : cv_(c.cv_), version_(c.version_), cvHash_(c.cvHash_) {}
};

#endif