#include "lib/tcp_socket_endpoint.h"


TCPSocketEndpoint::TCPSocketEndpoint(const std::string& sip, const int sport, const bool isMasterReceiver)
    :Endpoint(sip, sport, isMasterReceiver) {
    fd_ = socket(PF_INET, SOCK_STREAM, 0);
    if (fd_ < 0) {
        LOG(ERROR) << "Accept Fd fail ";
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
        LOG(ERROR) << "Bind error\t" << bindRet;
        return;
    }

    // Register Connection Handler
    int listenRet = listen(fd_, 32);
    if (listenRet != 0) {
        LOG(ERROR) << "Listen Error\t" << listenRet;
    }
    connectionWatcher_ = new ev_io();
    connectionWatcher_->data = this;
    ev_io_init(connectionWatcher_, [](struct ev_loop* loop, struct ev_io* w, int revents) {
        struct sockaddr_in peerAddr;
        struct sockaddr_in myAddr;
        socklen_t len;
        int newFd = accept(w->fd, (struct sockaddr*)&peerAddr, &len);
        if (newFd > 0) {
            // Get 5-tuples of the connection
            uint32_t peerIP = peerAddr.sin_addr.s_addr;
            uint32_t peerPort = htons(peerAddr.sin_port);
            uint64_t otherPoint = CONCAT_UINT32(peerIP, peerPort);
            getsockname(newFd, (struct sockaddr*)&myAddr, &len);
            uint32_t myIP = myAddr.sin_addr.s_addr;
            uint32_t myPort = htons(myAddr.sin_port);
            uint64_t myPoint = CONCAT_UINT32(myIP, myPort);
            // New Connection should be given a msgHandler
            TCPSocketEndpoint* ep = (TCPSocketEndpoint*)(w->data);
            TCPChannel* channel = ep->GetChannel(otherPoint);
            if (channel) {
                channel->myFd_ = newFd;
                channel->myPoint_ = myPoint;
                TCPMsgHandler* m = channel->msgHandler_;
                m->attachedEP_ = ep;
                ev_io_set(m->evWatcher_, channel->myFd_, EV_READ);
                ev_io_start(ep->evLoop_, m->evWatcher_);
            }
        }
        }, fd_, EV_READ);
    ev_io_start(evLoop_, connectionWatcher_);

}

TCPSocketEndpoint::TCPSocketEndpoint(const Address& addr, const bool isMasterReceiver)
    :TCPSocketEndpoint(addr.ip_, addr.port_, isMasterReceiver) {}

TCPSocketEndpoint::~TCPSocketEndpoint() {
    ev_io_stop(evLoop_, connectionWatcher_);
    delete connectionWatcher_;
}

int TCPSocketEndpoint::SendMsgTo(const Address& dstAddr,
    const google::protobuf::Message& msg, const char msgType) {
    uint32_t dstIP = dstAddr.addr_.sin_addr.s_addr;
    uint32_t dstPort = htons(dstAddr.addr_.sin_port);
    uint64_t channelId = CONCAT_UINT32(dstIP, dstPort);
    TCPChannel* channel = GetChannel(channelId);
    if (channel->myPoint_ > 0) {
        // Send 
        std::string serializedStr = msg.SerializeAsString();
        uint32_t len = serializedStr.length() + sizeof(MessageHeader);
        char* buffer = new char[len];
        MessageHeader* msgHeader = (MessageHeader*)(void*)buffer;
        msgHeader->msgLen = serializedStr.length();
        msgHeader->msgType = msgType;
        memcpy(buffer + sizeof(MessageHeader), serializedStr.c_str(), serializedStr.length());
        uint32_t sentLen = 0;
        int ans = sentLen;
        while (sentLen < len) {
            int ret = send(channel->myFd_, buffer + sentLen, len - sentLen, 0);
            if (ret <= 0) {
                LOG(ERROR) << "send fail " << ret;
                if (errno == ECONNRESET) {
                    // The other side has been closed
                    ans = -1;
                    break;
                }
            }
            else {
                sentLen += ret;
            }
        }
        delete[] buffer;
        return ans;
    }
    else {
        LOG(ERROR) << " The Connection has not been established ";
        return -1;
    }
}

int TCPSocketEndpoint::ConnectTo(const Address& myAddr, const Address& dstAddr) {
    uint32_t dstIP = dstAddr.addr_.sin_addr.s_addr;
    uint32_t dstPort = htons(dstAddr.addr_.sin_port);
    uint64_t channelId = CONCAT_UINT32(dstIP, dstPort);
    TCPChannel* channel = GetChannel(channelId);
    if (channel) {
        LOG(ERROR) << "Already Connected-Channel Established ";
        return 0;
    }

    int newFd = socket(PF_INET, SOCK_STREAM, 0);

    // Bind socket to Address
    int bindRet = bind(fd_, (struct sockaddr*)&(myAddr.addr_), sizeof(struct sockaddr));
    if (bindRet != 0) {
        LOG(ERROR) << "Bind error\t" << bindRet;
        return bindRet;
    }

    int ret = connect(newFd, (struct sockaddr*)&(dstAddr.addr_), sizeof(sockaddr));
    if (ret < 0) {
        close(newFd);
        LOG(ERROR) << "Connect Fail " << ret;
        return ret;
    }

    // connect does not fail, since it is blocking fd, it means connection successfully
    // Set Non-Blocking
    int status = fcntl(newFd, F_SETFL, fcntl(newFd, F_GETFL, 0) | O_NONBLOCK);
    if (status < 0) {
        LOG(ERROR) << " Set NonBlocking Fail";
        close(newFd);
        return status;
    }
    channel = new TCPChannel();
    channel->myFd_ = newFd;
    uint32_t myIP = myAddr.addr_.sin_addr.s_addr;
    uint32_t myPort = htons(myAddr.addr_.sin_port);
    channel->myPoint_ = CONCAT_UINT32(myIP, myPort);
    return 0;
}

bool TCPSocketEndpoint::RegisterMsgHandler(uint64_t channelId, TCPMsgHandler* tcpMsgHdl) {
    if (isRegistered(channelId)) {
        LOG(ERROR) << "channel: " << channelId << " has been registered";
        return false;
    }
    TCPChannel* channel = new TCPChannel(tcpMsgHdl);
    channels_[channelId] = channel;
    return true;
}

bool TCPSocketEndpoint::UnRegisterMsgHandler(uint64_t channelId) {
    if (!isRegistered(channelId)) {
        LOG(ERROR) << "channel: " << channelId << " has not been registered";
        return false;
    }
    channels_.erase(channelId);
    return true;
}

bool TCPSocketEndpoint::isRegistered(uint64_t channelId) {
    return channels_.find(channelId) != channels_.end();
}

TCPChannel* TCPSocketEndpoint::GetChannel(uint64_t channelId) {
    if (!isRegistered(channelId)) {
        return NULL;
    }
    else {
        return channels_[channelId];
    }
}