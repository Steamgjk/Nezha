#ifndef NEZHA_UDP_SOCKET_SENDER_H
#define NEZHA_UDP_SOCKET_SENDER_H

#include "lib/endpoint.h"

class UDPSocketEndpoint : public Endpoint {
 private:
  /* data */
  struct UDPMsgHandler* msgHandler_;

 public:
  UDPSocketEndpoint(const std::string& ip = "", const int port = -1,
                    const bool isMasterReceiver = false);
  ~UDPSocketEndpoint();

  int SendMsgTo(const Address& dstAddr, const google::protobuf::Message& msg,
                const char msgType) override;
  bool RegisterMsgHandler(MessageHandler* msgHdl) override;
  bool UnRegisterMsgHandler(MessageHandler* msgHdl) override;
  bool isMsgHandlerRegistered(MessageHandler* msgHdl) override;
  void UnRegisterAllMsgHandlers() override;
};

#endif