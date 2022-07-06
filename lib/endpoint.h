#ifndef NEZHA_ENDPOINT_H
#define NEZHA_ENDPOINT_H 

#include <string>
#include <netinet/in.h>
#include <glog/logging.h>
#include <ev.h>
#include <arpa/inet.h>
#include <functional>
#include <set>
#include <fcntl.h>
#include <google/protobuf/message.h>
#include "lib/address.h"
#include "lib/utils.h"



class Endpoint
{
protected:
    /* data */
    Address addr_;
    int fd_;
    struct ev_loop* evLoop_;
    std::set<struct EndpointTimer*> eventTimers_;
public:
    int epId_; // for debug
    Endpoint();
    Endpoint(const std::string& sip, const int sport, const bool isMasterReceiver = false);
    Endpoint(const Address& addr, const bool isMasterReceiver = false);
    ~Endpoint();

    virtual int SendMsgTo(const Address& dstAddr,
        const google::protobuf::Message& msg, const char msgType) = 0;

    bool RegisterTimer(EndpointTimer* timer);
    bool UnRegisterTimer(EndpointTimer* timer);
    bool isRegistered(EndpointTimer* timer);
    void UnRegisterAllTimers();

    void LoopRun();
    void LoopBreak();


};



struct EndpointTimer {
    std::function<void(void*, Endpoint*)> timerFunc_;
    void* context_;
    Endpoint* attachedEP_;
    struct ev_timer* evTimer_;
    Address sender_;

    EndpointTimer(std::function<void(void*, Endpoint*)> timerf, void* ctx = NULL,
        uint32_t periodMs = 1, Endpoint* aep = NULL) :timerFunc_(timerf), context_(ctx), attachedEP_(aep) {
        evTimer_ = new ev_timer();
        evTimer_->data = (void*)this;
        evTimer_->repeat = periodMs * 1e-3;
        ev_init(evTimer_, [](struct ev_loop* loop, struct ev_timer* w, int revents) {
            EndpointTimer* t = (EndpointTimer*)(w->data);
            t->timerFunc_(t->context_, t->attachedEP_);
            });
    }
    ~EndpointTimer() {
        delete evTimer_;
    }
};



#endif 