#ifndef NEZHA_UDP_SOCKET_SENDER_H
#define NEZHA_UDP_SOCKET_SENDER_H 

#include <string>
#include <netinet/in.h>
#include <glog/logging.h>
#include <ev.h>
#include <arpa/inet.h>
#include <functional>
#include <set>
#include "lib/address.h"


#define BUFFER_SIZE (65535)

// TODO: In the future, we will extract a base class, and from that class, we derive two sub-classes: one for UDP and the other for TCP

class UDPSocketEndpoint
{
private:
    /* data */
    Address addr_;
    int fd_;
    struct ev_loop* evLoop_;
    std::set<struct MsgHandlerStruct*> msgHandlers_;
    std::set<struct TimerStruct*> eventTimers_;
public:
    UDPSocketEndpoint();
    UDPSocketEndpoint(const std::string& sip, const int sport, const bool isMasterReceiver = false);
    UDPSocketEndpoint(const Address& addr, const bool isMasterReceiver = false);
    ~UDPSocketEndpoint();

    int SendMsgTo(const Address& dstAddr, const std::string& msg);
    int SendMsgTo(const Address& dstAddr, const char* buffer, const uint32_t bufferLen);


    bool RegisterMsgHandler(MsgHandlerStruct* msgHdl);
    bool UnregisterMsgHandler(MsgHandlerStruct* msgHdl);

    bool RegisterTimer(TimerStruct* timer);
    bool UnregisterTimer(TimerStruct* timer);


    void LoopRun();

    void LoopBreak();


};

struct MsgHandlerStruct {
    std::function<void(char*, int, Address, void*)> msgHandler_;
    UDPSocketEndpoint* thisPtr_;
    void* context_;
    char* buffer_;
    Address sender_;
    struct ev_io* evWatcher_;

    MsgHandlerStruct(std::function<void(char*, int, Address*, void*)> msghdl, UDPSocketEndpoint* t = NULL, void* ctx = NULL) :msgHandler_(msghdl), thisPtr_(t), context_(ctx) {
        buffer = new char[BUFFER_SIZE];
        evWatcher_ = new ev_io();
        evWatcher_->data = (void*)this;
        ev_io_init(evWatcher_, [](struct ev_loop* loop, struct ev_io* w, int revents) {
            MsgHandlerStruct* m = (MsgHandlerStruct*)(w->data);
            socklen_t sockLen;
            int msgLen = recvfrom(w->fd, m->buffer_, BUFFER_SIZE, 0, &(m->sender_.addr_), &sockLen);
            m->msgHandler_(m->buffer_, msgLen, &(m->sender_), m->context_);
            }, fd_, EV_READ);
    }
};

struct TimerStruct {
    std::function<void(void*)> timerFunc_;
    void* context_;
    struct ev_timer* evTimer_;
    TimerStruct(std::function<void(void*)> timerf, void* ctx = NULL, uint32_t periodMs = 1) :timerFunc_(timerf), context_(ctx) {
        evTimer_ = new ev_timer();
        evTimer_->data = (void*)this;
        evTimer->repeat = periodMs * 1e-3;
        ev_init(evTimer_, [](struct ev_loop* loop, struct ev_timer* w, int revents) {
            TimerStruct* t = (TimerStruct*)(w->data);
            t->timerFunc_(t->context_);
            });
    }
};



#endif 