#include <fstream>
#include <iostream>
#include "lib/utils.h"
#include "lib/zipfian.h"
#include "proto/nezha_proto.pb.h"
DEFINE_string(receiver_1_ip, "127.0.0.1", "The ip address of the 1st receiver");
DEFINE_string(receiver_2_ip, "127.0.0.1", "The ip address of the 2nd receiver");
DEFINE_string(receiver_3_ip, "127.0.0.1", "The ip address of the 3rd receiver");
DEFINE_string(receiver_4_ip, "127.0.0.1", "The ip address of the 4th receiver");
DEFINE_string(receiver_5_ip, "127.0.0.1", "The ip address of the 5th receiver");

DEFINE_int32(receiver_1_port, 33333, "The port of the 1st receiver");
DEFINE_int32(receiver_2_port, 33333, "The port of the 2nd receiver");
DEFINE_int32(receiver_3_port, 33333, "The port of the 3rd receiver");
DEFINE_int32(receiver_4_port, 33333, "The port of the 4th receiver");
DEFINE_int32(receiver_5_port, 33333, "The port of the 5th receiver");

DEFINE_int32(receiver_num, 2, "The number of receivers to test");

DEFINE_string(client_ip, "127.0.0.1", "The ip address of the client");

DEFINE_int32(client_port, 33336,
             "The port of the client listens for OWD reply");

DEFINE_uint64(poisson_rate, 10000, "Request Per Second");

DEFINE_uint64(duration, 60, "Duration of the experiment");

DEFINE_uint64(client_id, 1, "Client ID");

std::vector<uint32_t> latencyBounds;
std::atomic<uint32_t> bound;

void ReplyHandlerFunc(MessageHeader* msgHeader, char* msgBuffer,
                      Address* sender, void* context) {
  if (msgHeader->msgType == MessageType::FAST_REPLY && msgHeader->msgLen > 0) {
    nezha::proto::Reply reply;
    if (reply.ParseFromArray(msgBuffer, msgHeader->msgLen)) {
      // LOG(INFO) << "replyOWD " << reply.owd() << "\t" << reply.replicaid();
      if (reply.owd() > 0 && reply.owd() < 200) {
        latencyBounds[reply.replicaid()] = reply.owd();
        auto it =
            max_element(std::begin(latencyBounds), std::end(latencyBounds));

        if (*it != bound) {
          bound.store(*it);
        }
      }
    }
  }
}

void OWDUpdate() {
  latencyBounds.resize(FLAGS_receiver_num, 80);
  bound = 80;
  UDPSocketEndpoint* replyEP = dynamic_cast<UDPSocketEndpoint*>(CreateEndpoint(
      EndpointType::UDP_ENDPOINT, FLAGS_client_ip, FLAGS_client_port));
  UDPMsgHandler* msgHandler = new UDPMsgHandler(ReplyHandlerFunc);
  replyEP->RegisterMsgHandler(msgHandler);
  replyEP->LoopRun();
}

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  FLAGS_logtostderr = 1;
  Endpoint* requestEP =
      CreateEndpoint(EndpointType::UDP_ENDPOINT, "", -1, true);
  LOG(INFO) << "ClientId = " << FLAGS_client_id << "\t"
            << " rate=" << FLAGS_poisson_rate;
  std::vector<uint32_t> reqPer10msVec;
  reqPer10msVec.reserve(FLAGS_duration * 100);
  std::default_random_engine generator(
      FLAGS_client_id);  // clientId as the seed
  std::poisson_distribution<int> distribution(FLAGS_poisson_rate / 100);
  for (uint32_t i = 0; i < FLAGS_duration * 100; i++) {
    reqPer10msVec.push_back(distribution(generator));
  }
  uint32_t maxReqId = FLAGS_poisson_rate * (FLAGS_duration - 10);
  std::thread* replyTd = new std::thread(OWDUpdate);
  uint32_t reqCnt = 0;
  std::vector<Address*> receiverAddrs;
  receiverAddrs.resize(5, NULL);
  receiverAddrs[0] = new Address(FLAGS_receiver_1_ip, FLAGS_receiver_1_port);
  receiverAddrs[1] = new Address(FLAGS_receiver_2_ip, FLAGS_receiver_2_port);
  receiverAddrs[2] = new Address(FLAGS_receiver_3_ip, FLAGS_receiver_3_port);
  receiverAddrs[3] = new Address(FLAGS_receiver_4_ip, FLAGS_receiver_4_port);
  receiverAddrs[4] = new Address(FLAGS_receiver_5_ip, FLAGS_receiver_5_port);
  nezha::proto::Request request;
  request.set_clientid(FLAGS_client_id);
  srand(FLAGS_client_id);
  for (uint32_t i = 0; i < reqPer10msVec.size(); i++) {
    uint32_t reqNum = reqPer10msVec[i];
    if (reqNum <= 0) {
      usleep(10000);
    } else {
      uint32_t intval = 10000 / reqNum;
      uint64_t nowTime = GetMicrosecondTimestamp();
      for (uint32_t j = 1; j <= reqNum; j++) {
        while (GetMicrosecondTimestamp() < nowTime + intval * j) {
        }
        uint64_t sendTime = GetMicrosecondTimestamp();
        request.set_sendtime(sendTime);
        request.set_bound(bound);
        request.set_reqid(reqCnt + 1);
        for (int k = 0; k < FLAGS_receiver_num; k++) {
          requestEP->SendMsgTo(*(receiverAddrs[k]), request,
                               MessageType::CLIENT_REQUEST);
        }

        reqCnt++;
        if (reqCnt >= maxReqId) {
          LOG(INFO) << "reqCnt=" << reqCnt << "\tTerminate Here";
          exit(0);
        }
      }
    }
  }
  delete requestEP;
  replyTd->join();
  delete replyTd;
}