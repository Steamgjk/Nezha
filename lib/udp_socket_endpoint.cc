#include "lib/udp_socket_endpoint.h"



UDPSocketEndpoint::UDPSocketEndpoint() :addr_("", -1) {
    fd_ = socket(PF_INET, SOCK_DGRAM, 0);
    if (fd_ < 0) {
        LOG(ERROR) << "Receiver Fd fail ";
    }
    evLoop_ = ev_loop_new();
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


UDPSocketEndpoint::~UDPSocketEndpoint() {
    LoopBreak();
    ev_loop_destroy(evLoop_);
}

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


bool UDPSocketEndpoint::RegisterMsgHandler(MsgHandlerStruct* msgHdl) {
    if (evLoop_ == NULL) {
        LOG(ERROR) << "No evLoop!";
        return false;
    }
    if (msgHandlers_.find(msgHdl) != msgHandlers_.end()) {
        LOG(ERROR) << "This msgHdl has already been registered";
        return false;
    }

    msgHdl->attachedEP_ = this;
    msgHandlers_.insert(msgHdl);
    ev_io_start(evLoop_, msgHdl->evWatcher_);

    return true;
}


bool UDPSocketEndpoint::UnregisterMsgHandler(MsgHandlerStruct* msgHdl) {
    if (msgHandlers_.find(msgHdl) == msgHandlers_.end()) {
        LOG(ERROR) << "The handler does not exist " << key;
        return false;
    }
    ev_io_stop(evLoop_, msgHdl->evWatcher_);
    msgHandlers_.erase(msgHdl);
    return true;
}


bool UDPSocketEndpoint::RegisterTimer(TimerStruct* timer) {
    if (evLoop_ == NULL) {
        LOG(ERROR) << "No evLoop!";
        return false;
    }

    if (eventTimers_.find(timer) != eventTimers_.end()) {
        LOG(ERROR) << "This timer has already been registered";
        return false;
    }

    timer->attachedEP_ = this;
    eventTimers_.insert(timer);
    ev_timer_again(evLoop_, timer->evTimer_);
    return true;
}

bool UDPSocketEndpoint::UnregisterTimer(TimerStruct* timer) {
    if (eventTimers_.find(timer) == eventTimers_.end()) {
        LOG(ERROR) << "The timer does not exist ";
        return false;
    }
    ev_timer_stop(evLoop_, timer->evTimer_);
    eventTimers_.erase(timer);
    return true;
}


void UDPSocketEndpoint::LoopRun() {
    ev_run(evLoop_, 0);
}

void UDPSocketEndpoint::LoopBreak() {
    for (MsgHandlerStruct* msgHdl : msgHandlers_) {
        ev_io_stop(evLoop_, msgHdl->evWatcher_)
    }

    for (TimerStruct* timer : eventTimers_) {
        ev_timer_stop(evLoop_, timer->evTimer_);
    }

    ev_break(evLoop_, EVBREAK_ALL);
    msgHandlers_.clear();
    eventTimers_.clear();
}

