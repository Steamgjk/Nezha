#include "lib/udp_socket_endpoint.h"

UDPSocketEndpoint::UDPSocketEndpoint() :Endpoint() {
    fd_ = socket(PF_INET, SOCK_DGRAM, 0);
    if (fd_ < 0) {
        LOG(ERROR) << "Receiver Fd fail ";
    }
    // Set Non-Blocking
    int status = fcntl(fd_, F_SETFL, fcntl(fd_, F_GETFL, 0) | O_NONBLOCK);
    if (status < 0) {
        LOG(ERROR) << " Set NonBlocking Fail";
    }
}

UDPSocketEndpoint::UDPSocketEndpoint(const std::string& sip, const int sport, const bool isMasterReceiver) :
    Endpoint(sip, sport, isMasterReceiver) {
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
    struct sockaddr_in addr;
    bzero(&addr, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(sport);
    addr.sin_addr.s_addr = inet_addr(sip.c_str());
    // Bind socket to Address
    int bindRet = bind(fd_, (struct sockaddr*)&addr, sizeof(addr));
    if (bindRet != 0) {
        LOG(ERROR) << "bind error\t" << bindRet;
        return;
    }
}

UDPSocketEndpoint::UDPSocketEndpoint(const Address& addr, const bool isMasterReceiver)
    :UDPSocketEndpoint(addr.ip_, addr.port_, isMasterReceiver) {}


UDPSocketEndpoint::~UDPSocketEndpoint() {
    LoopBreak();
    ev_loop_destroy(evLoop_);
}


int UDPSocketEndpoint::SendMsgTo(const Address& dstAddr, const google::protobuf::Message& msg, char msgType) {
    char buffer[UDP_BUFFER_SIZE];
    MessageHeader* msgHdr = (MessageHeader*)(void*)buffer;
    msgHdr->msgType = msgType;
    std::string serializedString = msg.SerializeAsString();
    msgHdr->msgLen = serializedString.length();
    if (serializedString.length() + sizeof(MessageHeader) >= UDP_BUFFER_SIZE) {
        LOG(ERROR) << "Msg too large " << (uint32_t)msgType << "\t length=" << serializedString.length();
        return -1;
    }
    if (msgHdr->msgLen > 0) {
        // serialization succeed
        memcpy(buffer + sizeof(MessageHeader), serializedString.c_str(), msgHdr->msgLen);
        int ret = sendto(fd_, buffer, msgHdr->msgLen + sizeof(MessageHeader), 0,
            (struct sockaddr*)(&(dstAddr.addr_)), sizeof(sockaddr_in));
        if (ret < 0) {
            LOG(ERROR) << pthread_self() << "\tSend Fail ret =" << ret;
        }
        return ret;
    }
    return -1;

}

bool UDPSocketEndpoint::RegisterMsgHandler(UDPMsgHandler* msgHdl) {
    if (evLoop_ == NULL) {
        LOG(ERROR) << "No evLoop!";
        return false;
    }
    if (isRegistered(msgHdl)) {
        LOG(ERROR) << "This msgHdl has already been registered";
        return false;
    }

    msgHdl->attachedEP_ = this;
    msgHandlers_.insert(msgHdl);
    ev_io_set(msgHdl->evWatcher_, this->fd_, EV_READ);
    ev_io_start(evLoop_, msgHdl->evWatcher_);

    return true;
}


bool UDPSocketEndpoint::UnregisterMsgHandler(UDPMsgHandler* msgHdl) {
    if (evLoop_ == NULL) {
        LOG(ERROR) << "No evLoop!";
        return false;
    }
    if (!isRegistered(msgHdl)) {
        LOG(ERROR) << "The handler has not been registered ";
        return false;
    }
    ev_io_stop(evLoop_, msgHdl->evWatcher_);
    msgHandlers_.erase(msgHdl);
    return true;
}

bool UDPSocketEndpoint::isRegistered(UDPMsgHandler* msgHdl) {
    return (msgHandlers_.find(msgHdl) != msgHandlers_.end());
}

void UDPSocketEndpoint::LoopRun() {
    ev_run(evLoop_, 0);
}

void UDPSocketEndpoint::LoopBreak() {
    for (UDPMsgHandler* msgHdl : msgHandlers_) {
        ev_io_stop(evLoop_, msgHdl->evWatcher_);
    }
    ev_break(evLoop_, EVBREAK_ALL);
    msgHandlers_.clear();
    eventTimers_.clear();
}

