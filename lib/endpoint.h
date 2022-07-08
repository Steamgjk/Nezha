#ifndef NEZHA_ENDPOINT_H
#define NEZHA_ENDPOINT_H

#include <arpa/inet.h>
#include <ev.h>
#include <fcntl.h>
#include <glog/logging.h>
#include <google/protobuf/message.h>
#include <netinet/in.h>
#include <functional>
#include <set>
#include <string>
#include "lib/address.h"
#include "lib/message_type.h"

struct EndpointTimer;
struct EndpointMsgHandler;

class Endpoint {
 protected:
  /* data */
  Address addr_;
  int fd_;
  struct ev_loop* evLoop_;
  std::set<struct EndpointTimer*>
      eventTimers_;  // One endpoint can have multiple timers
                     //   std::set<struct EndpointMsgHandler*>

 public:
  int epId_;  // for debug
  Endpoint();
  Endpoint(const std::string& sip = "", const int sport = -1,
           const bool isMasterReceiver = false);
  ~Endpoint();

  virtual int SendMsgTo(const Address& dstAddr,
                        const google::protobuf::Message& msg,
                        const char msgType) = 0;
  virtual bool RegisterMsgHandler(EndpointMsgHandler* msgHdl) = 0;
  virtual bool UnregisterMsgHandler(EndpointMsgHandler* msgHdl) = 0;

  bool RegisterTimer(EndpointTimer* timer);
  bool UnRegisterTimer(EndpointTimer* timer);
  bool isRegistered(EndpointTimer* timer);
  void UnRegisterAllTimers();

  void LoopRun();
  void LoopBreak();
};

typedef std::function<void(MessageHeader*, char*, Address*, void*, Endpoint*)>
    MsgHandlerFunc;

struct EndpointTimer {
  std::function<void(void*, Endpoint*)> timerFunc_;
  void* context_;
  Endpoint* attachedEP_;
  struct ev_timer* evTimer_;
  Address sender_;

  EndpointTimer(std::function<void(void*, Endpoint*)> timerf, void* ctx = NULL,
                uint32_t periodMs = 1, Endpoint* aep = NULL)
      : timerFunc_(timerf), context_(ctx), attachedEP_(aep) {
    evTimer_ = new ev_timer();
    evTimer_->data = (void*)this;
    evTimer_->repeat = periodMs * 1e-3;
    ev_init(evTimer_,
            [](struct ev_loop* loop, struct ev_timer* w, int revents) {
              EndpointTimer* t = (EndpointTimer*)(w->data);
              t->timerFunc_(t->context_, t->attachedEP_);
            });
  }
  ~EndpointTimer() { delete evTimer_; }
};

struct EndpointMsgHandler {
  std::function<void(MessageHeader*, char*, Address*, void*, Endpoint*)>
      msgHandler_;
  void* context_;
  Endpoint* attachedEP_;
  Address sender_;
  struct ev_io* evWatcher_;
  EndpointMsgHandler(
      std::function<void(MessageHeader*, char*, Address*, void*, Endpoint*)>
          msghdl,
      void* ctx = NULL, Endpoint* aep = NULL)
      : msgHandler_(msghdl), context_(ctx), attachedEP_(aep) {
    evWatcher_ = new ev_io();
    evWatcher_->data = (void*)this;
  }
  ~EndpointMsgHandler() { delete evWatcher_; }
};

#endif