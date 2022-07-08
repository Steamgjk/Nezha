#include "lib/tcp_socket_endpoint.h"


TCPSocketEndpoint::TCPSocketEndpoint(const std::string& sip, const int sport, const bool isMasterReceiver)
    :Endpoint(sip, sport, isMasterReceiver), serverCallBack_(nullptr) {
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
        socklen_t len;
        int newFd = accept(w->fd, (struct sockaddr*)&peerAddr, &len);
        if (newFd > 0) {
            // Get 5-tuples of the connection
            uint32_t peerIP = peerAddr.sin_addr.s_addr;
            uint32_t peerPort = htons(peerAddr.sin_port);
            uint64_t channelId = CONCAT_UINT32(peerIP, peerPort);
            TCPSocketEndpoint* ep = (TCPSocketEndpoint*)(w->data);
            // New Connection should be given a msgHandler
            TCPMsgHandler* tcpHdl = new TCPMsgHandler(ep->serverCallBack_, ep->context_, ep);
            ep->channelFds_[channelId] = newFd;
            ep->channelMsgHandlers_[channelId] = tcpHdl;
            ep->msgHandlers_.insert(tcpHdl);
            ev_io_set(tcpHdl->evWatcher_, newFd, EV_READ);
            ev_io_start(ep->evLoop_, tcpHdl->evWatcher_);
        }
        }, fd_, EV_READ);
    ev_io_start(evLoop_, connectionWatcher_);

}


TCPSocketEndpoint::~TCPSocketEndpoint() {
    delete connectionWatcher_;
}

int TCPSocketEndpoint::SendMsgTo(const Address& dstAddr,
    const google::protobuf::Message& msg, const char msgType) {
    uint32_t dstIP = dstAddr.addr_.sin_addr.s_addr;
    uint32_t dstPort = htons(dstAddr.addr_.sin_port);
    uint64_t channelId = CONCAT_UINT32(dstIP, dstPort);
    auto kv = channelFds_.find(channelId);
    if (kv != channelFds_.end()) {
        // Send 
        int fd = kv->second;
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
            int ret = send(fd, buffer + sentLen, len - sentLen, 0);
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

// Return channelId if the connection succeeds
uint64_t TCPSocketEndpoint::ConnectTo(const Address& dstAddr) {
    uint32_t dstIP = dstAddr.addr_.sin_addr.s_addr;
    uint32_t dstPort = htons(dstAddr.addr_.sin_port);
    uint64_t channelId = CONCAT_UINT32(dstIP, dstPort);

    if (channelFds_.find(channelId) != channelFds_.end()) {
        LOG(ERROR) << "Already Connected-Channel Established ";
        return channelId;
    }

    int newFd = socket(PF_INET, SOCK_STREAM, 0);

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
        LOG(ERROR) << " Set NonBlocking Fail status=" << status;
        close(newFd);
        return 0;
    }

    channelFds_[channelId] = newFd;
    if (channelMsgHandlers_.find(channelId) != channelMsgHandlers_.end()) {
        // We have some pre-registed msg handler
        TCPMsgHandler* tcpHdl = channelMsgHandlers_[channelId];
        ev_io_set(tcpHdl->evWatcher_, newFd, EV_READ);
        ev_io_start(evLoop_, tcpHdl->evWatcher_);
    }
    return channelId;
}



void TCPSocketEndpoint::RegisterServerCallBack(
    std::function<void(MessageHeader*, char*, Address*, void*, Endpoint*)>& func, void* ctx) {
    serverCallBack_ = func;
    context_ = ctx;
}

void TCPSocketEndpoint::UnRegisterServerCallBack() {
    serverCallBack_ = nullptr;
    context_ = NULL;
}

bool TCPSocketEndpoint::RegisterMsgHandler(const Address& dstAddr, TCPMsgHandler* tcpMsgHdl) {
    uint32_t dstIP = dstAddr.addr_.sin_addr.s_addr;
    uint32_t dstPort = htons(dstAddr.addr_.sin_port);
    uint64_t channelId = CONCAT_UINT32(dstIP, dstPort);
    if (channelMsgHandlers_.find(channelId) != channelMsgHandlers_.end()) {
        LOG(ERROR) << "channel: " << channelId << " has some registered handlers";
        return false;
    }
    if (msgHandlers_.find(tcpMsgHdl) != msgHandlers_.end()) {
        LOG(ERROR) << " This msgHandler has already been registered to other channels";
        return false;
    }
    msgHandlers_.insert(tcpMsgHdl);
    channelMsgHandlers_[channelId] = tcpMsgHdl;
    return true;
}

bool TCPSocketEndpoint::UnRegisterMsgHandler(const Address& dstAddr) {
    uint32_t dstIP = dstAddr.addr_.sin_addr.s_addr;
    uint32_t dstPort = htons(dstAddr.addr_.sin_port);
    uint64_t channelId = CONCAT_UINT32(dstIP, dstPort);
    if (channelMsgHandlers_.find(channelId) != channelMsgHandlers_.end()) {
        msgHandlers_.erase(channelMsgHandlers_[channelId]);
        channelMsgHandlers_.erase(channelId);
    }

    return true;
}
