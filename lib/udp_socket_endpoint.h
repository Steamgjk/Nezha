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
#include "lib/utils.h"
#include "nezha/nezha-proto.pb.h"

using namespace nezha::proto;

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
    int epId_; // for debug
    UDPSocketEndpoint();
    UDPSocketEndpoint(const std::string& sip, const int sport, const bool isMasterReceiver = false);
    UDPSocketEndpoint(const Address& addr, const bool isMasterReceiver = false);
    ~UDPSocketEndpoint();

    int SendMsgTo(const Address& dstAddr, const std::string& msg);
    int SendMsgTo(const Address& dstAddr, const char* buffer, const uint32_t bufferLen);
    int SendMsgTo(const Address& dstAddr, const google::protobuf::Message& msg, const char msgType);

    bool RegisterMsgHandler(MsgHandlerStruct* msgHdl);
    bool UnregisterMsgHandler(MsgHandlerStruct* msgHdl);
    bool isRegistered(MsgHandlerStruct* msgHdl);

    bool RegisterTimer(TimerStruct* timer);
    bool UnregisterTimer(TimerStruct* timer);
    bool isRegistered(TimerStruct* timer);


    void LoopRun();

    void LoopBreak();


};

struct MsgHandlerStruct {
    std::function<void(char*, int, Address*, void*, UDPSocketEndpoint*)> msgHandler_;
    void* context_;
    UDPSocketEndpoint* attachedEP_;
    char* buffer_;
    Address sender_;
    struct ev_io* evWatcher_;

    MsgHandlerStruct(std::function<void(char*, int, Address*, void*, UDPSocketEndpoint*)> msghdl, void* ctx = NULL, UDPSocketEndpoint* aep = NULL) :msgHandler_(msghdl), context_(ctx), attachedEP_(aep) {
        buffer_ = new char[UDP_BUFFER_SIZE];
        evWatcher_ = new ev_io();
        evWatcher_->data = (void*)this;

        ev_init(evWatcher_, [](struct ev_loop* loop, struct ev_io* w, int revents) {
            MsgHandlerStruct* m = (MsgHandlerStruct*)(w->data);
            if (m->attachedEP_ == NULL) {
                LOG(ERROR) << "This message handler is not attached to any endpoints";
                return;
            }
            socklen_t sockLen;
            int msgLen = recvfrom(w->fd, m->buffer_, UDP_BUFFER_SIZE, 0, (struct sockaddr*)(&(m->sender_.addr_)), &sockLen);
            m->msgHandler_(m->buffer_, msgLen, &(m->sender_), m->context_, m->attachedEP_);
            });
    }
    ~MsgHandlerStruct() {
        delete evWatcher_;
        delete[]buffer_;
    }
};

struct TimerStruct {
    std::function<void(void*, UDPSocketEndpoint*)> timerFunc_;
    void* context_;
    UDPSocketEndpoint* attachedEP_;
    struct ev_timer* evTimer_;

    TimerStruct(std::function<void(void*, UDPSocketEndpoint*)> timerf, void* ctx = NULL, uint32_t periodMs = 1, UDPSocketEndpoint* aep = NULL) :timerFunc_(timerf), context_(ctx), attachedEP_(aep) {
        evTimer_ = new ev_timer();
        evTimer_->data = (void*)this;
        evTimer_->repeat = periodMs * 1e-3;
        ev_init(evTimer_, [](struct ev_loop* loop, struct ev_timer* w, int revents) {
            TimerStruct* t = (TimerStruct*)(w->data);
            t->timerFunc_(t->context_, t->attachedEP_);
            });
    }
    ~TimerStruct() {
        delete evTimer_;
    }
};



#endif 