#include "lib/udp_socket_endpoint.h"


namespace nezha_network
{
    UDPSocketEndpoint::UDPSocketEndpoint() :addr_("", -1) {
        fd_ = socket(PF_INET, SOCK_DGRAM, 0);
        if (fd_ < 0) {
            LOG(ERROR) << "Receiver Fd fail ";
        }
        evLoop_ = NULL;
    }
    UDPSocketEndpoint::UDPSocketEndpoint(const std::string& sip, const int sport, const bool isMasterReceiver) :addr_(sip, sport) {
        fd_ = socket(PF_INET, SOCK_DGRAM, 0);
        if (fd_ < 0) {
            LOG(ERROR) << "Receiver Fd fail ";
            return;
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
        evLoop_ = isMasterReceiver ? ev_default_loop() : ev_loop_new();
        if (!evLoop_) {
            LOG(ERROR) << "Event Loop error";
            return;
        }
    }

    UDPSocketEndpoint::UDPSocketEndpoint(const Address& addr, const bool isMasterReceiver) :UDPSocketEndpoint(addr.ip_, addr.port_, isMasterReceiver) {}


    UDPSocketEndpoint::~UDPSocketEndpoint() {}

    int UDPSocketEndpoint::SendMsgTo(const Address& dstAddr, const char* buffer, const uint32_t bufferLen) {

        int ret = sendto(fd_, buffer, bufferLen, 0, &(dstAddr.addr_), sizeof(sockaddr_in));
        if (ret < 0) {
            LOG(ERROR) << "Send Fail ret =" << ret;
        }
        return ret;
    }

    int UDPSocketEndpoint::SendMsgTo(const Address& dstAddr, const std::string& msg) {
        return this->SendMsgTo(dstAddr, msg.c_str(), msg.length());
    }


    uint64_t UDPSocketEndpoint::RegisterReceiveHandler(std::function<void(char*, int, void*)> msgHandler, void* context) {
        if (evLoop_ == NULL) {
            LOG(ERROR) << "No evLoop: This is not a receiver endpoint";
            return 0;
        }
        struct ev_io* evIO = new ev_io();
        evIO->data = new ParaStruct(msgHandler, this, context, buffer, evIO);
        char* buffer = new char[BUFFER_SIZE];
        ev_io_init(evIO, [](struct ev_loop* loop, struct ev_io* w, int revents) {
            ParaStruct* para = (ParaStruct*)(w->data);
            struct sockaddr sender;
            socklen_t sockLen;
            int msgLen = recvfrom(w->fd, para->buffer_, BUFFER_SIZE, 0, &sender, &sockLen);
            para->msgHandler_(para->buffer_, msgLen, para->context_);
            }, fd_, EV_READ);
        ev_io_start(evLoop_, evIO);
        uint64_t key = reinterpret_cast<uint64_t>(ev_io);
        if (evWatchers_.get(key) != 0) {
            LOG(ERROR) << "Watcher Conflict " << key;
            return 0;
        }
        evWatchers_.assign(key, para);
        return key;
    }
    void UDPSocketEndpoint::ReceiverRun() {
        ev_run(evLoop_, 0);
    }

    bool UDPSocketEndpoint::UnregisterReceiveHandler(uint64_t key) {
        if (evWatchers_.get(key) == 0) {
            LOG(ERROR) << "The handler does not exist " << key;
            return false;
        }
        struct ParaStruct* para = evWatchers_.get(key);
        evWatchers_.erase(key);
        ev_stop(evLoop_, para->evWatcher_);
        delete[]buffer_;
        delete para;
        return true;
    }

} // namespace nezha_network
