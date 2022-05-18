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
        LOG(ERROR) << "No evLoop!";
        return 0;
    }
    struct ev_io* evIO = new ev_io();
    uint64_t key = reinterpret_cast<uint64_t>(evIO);
    if (evWatchers_.find(key) != evWatchers_.end()) {
        LOG(ERROR) << "Watcher Conflict Key" << key;
        delete evIO;
        return 0;
    }
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

    evWatchers_[key] = (ParaStruct*)(evIO->data);
    return key;
}


bool UDPSocketEndpoint::UnregisterReceiveHandler(uint64_t key) {
    if (evWatchers_.find(key) == evWatchers_.end()) {
        LOG(ERROR) << "The handler does not exist " << key;
        return false;
    }
    struct ParaStruct* para = evWatchers_[key];
    evWatchers_.erase(key);
    ev_stop(evLoop_, para->evWatcher_);
    delete[]buffer_;
    delete para;
    return true;
}


uint64_t UDPSocketEndpoint::RegisterTimer(std::function<void(void*)> timerFunc, void* context, uint32_t periodMs) {
    if (evLoop_ == NULL) {
        LOG(ERROR) << "No evLoop!";
        return 0;
    }
    struct ev_timer* evTimer = new ev_timer();
    uint64_t key = reinterpret_cast<uint64_t>(evTimer);
    if (evTimers_.find(key) != evTimers_.end()) {
        LOG(ERROR) << "Timer Conflict Key" << key;
        delete evTimer;
        return 0;
    }

    evTimer->data = new TimerStruct(timerFunc, context, evTimer);
    evTimer->repeat = periodMs * 1e-3;
    ev_init(evTimer, [](struct ev_loop* loop, struct ev_timer* w, int revents) {
        TimerStruct* timerInfo = (TimerStruct*)(w->data);
        timerInfo->timerFunc_(timerInfo->context_);
        });
    ev_timer_again(evLoop_, evTimer);

    evTimers_[key] = (TimerStruct*)(evTimer->data);
    return key;
}

bool UDPSocketEndpoint::CancelTimer(uint64_t key) {
    if (evTimers_.find(key) == evTimers_.end()) {
        LOG(ERROR) << "The timer does not exist " << key;
        return false;
    }
    struct TimerStruct* timerInfo = evTimers_[key];
    evTimers_.erase(key);
    ev_timer_stop(evLoop_, timerInfo->evTimer_);
    delete timerInfo;
    return true;
}


void UDPSocketEndpoint::LoopRun() {
    ev_run(evLoop_, 0);
}