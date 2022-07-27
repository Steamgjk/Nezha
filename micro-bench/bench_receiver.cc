#include <fstream>
#include <iostream>
#include "lib/utils.h"
#include "lib/zipfian.h"
#include "proto/nezha_proto.pb.h"
DEFINE_string(receiver_ip, "127.0.0.1", "The ip address of the receiver");

DEFINE_int32(receiver_port, 33333, "The port of the receiver");

DEFINE_int32(replica_id, 1, "The id of the replica");
DEFINE_int32(enable_dom, 0, "Whether enable DOM");
DEFINE_int32(percentile, 50, "The percentile of the owd estimation");

DEFINE_int32(client_port, 33336,
             "The port of the client listens for OWD reply");

ConcurrentMap<uint32_t, Address*> clientAddrs;
ConcurrentQueue<std::pair<uint32_t, uint32_t>> owdQu;
ConcurrentQueue<nezha::proto::Request> processQu;
std::vector<std::pair<uint32_t, uint32_t>> traceVec;
void MsgHandlerFunc(MessageHeader* msgHeader, char* msgBuffer, Address* sender,
                    void* context) {
  if (msgHeader->msgType == MessageType::CLIENT_REQUEST &&
      msgHeader->msgLen > 0) {
    nezha::proto::Request request;
    if (request.ParseFromArray(msgBuffer, msgHeader->msgLen)) {
      if (clientAddrs.get(request.clientid()) == NULL) {
        Address* senderAddr =
            new Address(sender->GetIPAsString(), FLAGS_client_port);
        clientAddrs.assign(request.clientid(), senderAddr);
      }
      processQu.enqueue(request);
      uint64_t nowTime = GetMicrosecondTimestamp();
      if (nowTime > request.sendtime()) {
        uint32_t owd = nowTime - request.sendtime();
        owdQu.enqueue({request.clientid(), owd});
      }
    }
  }
}
void ProcessTd() {
  traceVec.reserve(10000000ul);
  nezha::proto::Request request;
  std::map<std::pair<uint64_t, uint64_t>, nezha::proto::Request> earlyBuffer;
  uint64_t startTime = GetMicrosecondTimestamp();
  LOG(INFO) << "FLAGS_enable_dom=" << FLAGS_enable_dom;
  while (true) {
    if (FLAGS_enable_dom == 1) {
      if (processQu.try_dequeue(request)) {
        uint64_t deadline = request.sendtime() + request.bound();
        uint64_t reqKey = CONCAT_UINT32(request.clientid(), request.reqid());
        earlyBuffer.insert({{deadline, reqKey}, request});
      }
      uint64_t nowTime = GetMicrosecondTimestamp();
      while (earlyBuffer.empty() == false &&
             earlyBuffer.begin()->first.first <= nowTime) {
        traceVec.push_back({earlyBuffer.begin()->second.clientid(),
                            earlyBuffer.begin()->second.reqid()});
        earlyBuffer.erase(earlyBuffer.begin());
        if (traceVec.size() >= 10000000ul) {
          break;
        }
      }
    } else {
      while (processQu.try_dequeue(request)) {
        traceVec.push_back({request.clientid(), request.reqid()});
        if (traceVec.size() >= 10000000ul) {
          break;
        }
      }
    }
    uint64_t nowTime = GetMicrosecondTimestamp();

    if (nowTime - startTime >= 60 * 1000ul * 1000ul ||
        traceVec.size() >= 10000000ul) {
      LOG(INFO) << "To terminated ..." << traceVec.size();
      std::ofstream ofs("Replica-Stats-" + std::to_string(FLAGS_replica_id) +
                        ".csv");
      // ofs << "ClientID,ReqID" << std::endl;
      for (auto& p : traceVec) {
        ofs << p.first << "\t" << p.second << std::endl;
      }
      ofs.close();
      exit(0);
    }
  }
}

void OWDTd() {
  std::pair<uint32_t, uint32_t> owdSample;
  std::map<uint32_t, std::vector<uint32_t>> owdMap;
  std::map<uint32_t, uint32_t> owdCnt;
  UDPSocketEndpoint* replyEP = dynamic_cast<UDPSocketEndpoint*>(
      CreateEndpoint(EndpointType::UDP_ENDPOINT));
  nezha::proto::Reply reply;
  reply.set_replicaid(FLAGS_replica_id);
  while (true) {
    if (owdQu.try_dequeue(owdSample)) {
      uint32_t senderId = owdSample.first;
      uint32_t owd = owdSample.second;
      if (owdMap.find(senderId) == owdMap.end()) {
        owdMap[senderId].resize(1000);
        owdCnt[senderId] = 0;
      }
      owdMap[senderId][owdCnt[senderId] % 1000] = owd;
      owdCnt[senderId]++;
      if (owdCnt[senderId] % 1000 == 0) {
        std::vector<uint32_t> temp = owdMap[senderId];
        sort(temp.begin(), temp.end());
        uint32_t estimate = temp[1000 * FLAGS_percentile / 100];
        reply.set_clientid(senderId);
        reply.set_owd(estimate +
                      10);  // plus the 3 * error bound (sigma1+sigma2), the
                            // sigma ranges 1-3, here we plus 10 to simulate it
        Address* clientAddr = clientAddrs.get(senderId);
        if (clientAddr) {
          // LOG(INFO) << "Send to " << senderId << "\t" << estimate;
          replyEP->SendMsgTo(*clientAddr, reply, MessageType::FAST_REPLY);
        }
      }
    }
  }
}

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  FLAGS_logtostderr = 1;
  std::thread* processTd = new std::thread(ProcessTd);
  std::thread* owdTd = new std::thread(OWDTd);
  Endpoint* requestEP = CreateEndpoint(
      EndpointType::UDP_ENDPOINT, FLAGS_receiver_ip, FLAGS_receiver_port, true);
  UDPMsgHandler* msgHandler = new UDPMsgHandler(MsgHandlerFunc);
  requestEP->RegisterMsgHandler(msgHandler);
  requestEP->LoopRun();
  processTd->join();
  owdTd->join();

  delete requestEP;
  delete processTd;
  delete owdTd;
}