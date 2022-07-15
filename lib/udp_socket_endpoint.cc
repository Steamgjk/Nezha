#include "lib/udp_socket_endpoint.h"

UDPSocketEndpoint::UDPSocketEndpoint(const std::string& ip, const int port,
                                     const bool isMasterReceiver)
    : Endpoint(ip, port, isMasterReceiver), msgHandler_(NULL) {
  fd_ = socket(PF_INET, SOCK_DGRAM, 0);
  if (fd_ < 0) {
    LOG(ERROR) << "Receiver Fd fail ";
    return;
  }
  // Set Non-Blocking
  int status = fcntl(fd_, F_SETFL, fcntl(fd_, F_GETFL, 0) | O_NONBLOCK);
  if (status < 0) {
    LOG(ERROR) << " Set NonBlocking Fail";
  }
  if (ip == "" || port < 0) {
    return;
  }
  struct sockaddr_in addr;
  bzero(&addr, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  addr.sin_addr.s_addr = inet_addr(ip.c_str());
  // Bind socket to Address
  int bindRet = bind(fd_, (struct sockaddr*)&addr, sizeof(addr));
  if (bindRet != 0) {
    LOG(ERROR) << "bind error\t" << bindRet << "\t port=" << port;
    return;
  }
}

UDPSocketEndpoint::~UDPSocketEndpoint() {}

int UDPSocketEndpoint::SendMsgTo(const Address& dstAddr,
                                 const google::protobuf::Message& msg,
                                 char msgType) {
  char buffer[UDP_BUFFER_SIZE];
  MessageHeader* msgHdr = (MessageHeader*)(void*)buffer;
  msgHdr->msgType = msgType;
  std::string serializedString = msg.SerializeAsString();
  msgHdr->msgLen = serializedString.length();
  if (serializedString.length() + sizeof(MessageHeader) > UDP_BUFFER_SIZE) {
    LOG(ERROR) << "Msg too large " << (uint32_t)msgType
               << "\t length=" << serializedString.length();
    return -1;
  }
  if (msgHdr->msgLen > 0) {
    // Serialization succeed
    // Prepend MesageHeader to the serialized string
    memcpy(buffer + sizeof(MessageHeader), serializedString.c_str(),
           msgHdr->msgLen);
    int ret = sendto(fd_, buffer, msgHdr->msgLen + sizeof(MessageHeader), 0,
                     (struct sockaddr*)(&(dstAddr.addr_)), sizeof(sockaddr_in));
    if (ret < 0) {
      VLOG(1) << pthread_self() << "\tSend Fail ret =" << ret;
    }
    return ret;
  }
  return -1;
}

bool UDPSocketEndpoint::RegisterMsgHandler(MessageHandler* msgHdl) {
  UDPMsgHandler* udpMsgHdl = (UDPMsgHandler*)msgHdl;
  if (evLoop_ == NULL) {
    LOG(ERROR) << "No evLoop!";
    return false;
  }
  if (isMsgHandlerRegistered(msgHdl)) {
    LOG(ERROR) << "This msgHdl has already been registered";
    return false;
  }

  msgHandler_ = udpMsgHdl;
  ev_io_set(udpMsgHdl->evWatcher_, fd_, EV_READ);
  ev_io_start(evLoop_, udpMsgHdl->evWatcher_);

  return true;
}

bool UDPSocketEndpoint::UnRegisterMsgHandler(MessageHandler* msgHdl) {
  UDPMsgHandler* udpMsgHdl = (UDPMsgHandler*)msgHdl;
  if (evLoop_ == NULL) {
    LOG(ERROR) << "No evLoop!";
    return false;
  }
  if (!isMsgHandlerRegistered(udpMsgHdl)) {
    LOG(ERROR) << "The handler has not been registered ";
    return false;
  }
  ev_io_stop(evLoop_, udpMsgHdl->evWatcher_);
  msgHandler_ = NULL;
  return true;
}

bool UDPSocketEndpoint::isMsgHandlerRegistered(MessageHandler* msgHdl) {
  return (UDPMsgHandler*)msgHdl == msgHandler_;
}

void UDPSocketEndpoint::UnRegisterAllMsgHandlers() {
  ev_io_stop(evLoop_, msgHandler_->evWatcher_);
  msgHandler_ = NULL;
}
