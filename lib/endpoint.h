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
#include "lib/common_struct.h"
#include "lib/message_handler.h"
#include "lib/timer.h"

/**
 * Endpoint is the basic abstraction, and it can be derived to more specific
 * endpoints, based on the communication primtive (e.g., UDPSocketEndpoint)
 *
 * An Endpoint supports three major functionalities:
 * (1) Send/Receive messages;
 * (2) Process the received messages according to (pre-registered) customized
 * message handlers;
 * (3) Conduct periodical actions according to (pre-registered)
 * customized timer functions.
 */
class Endpoint {
 protected:
  /* The address of this endpoint */
  Address addr_;
  /** The socket fd it uses to send/recv messages */
  int fd_;
  /** The ev_loop struct from libev, which uses to handle io/timer events */
  struct ev_loop* evLoop_;
  /** One endpoint can have multiple timers registered. We maintain a set to
   * avoid duplicate registration and check whether a specific timer has been
   * registered or not.*/
  std::set<struct Timer*> eventTimers_;

 public:
  int epId_;  // The id of the endpoint, mainly for debug
  /** The endpoint accepts an ip and port. If both are valid, it binds the
   * socket fd to the ip:port. If isMasterReceiver is true, it creates the
   * default loop with libev, otherwise, it creates new loop (refer to libev
   * documentation for detailed explanation at
   * https://metacpan.org/dist/EV/view/libev/ev.pod) */
  Endpoint(const std::string& ip = "", const int port = -1,
           const bool isMasterReceiver = false);
  virtual ~Endpoint();

  /** Send the message to the specific destination. The method needs to know the
   * message type (3rd parameter) and include such information in the buffer */
  virtual int SendMsgTo(const Address& dstAddr,
                        const google::protobuf::Message& msg,
                        const char msgType) = 0;

  /** An endpoint potentially can have multiple message handlers registered, but
   * our UDPSocketEndpoint implementation only supports at most one
   * message handler for one endpoint. So we make them as virtual functions and
   * different derived classes have their own implementation of the methods */
  virtual bool RegisterMsgHandler(MessageHandler* msgHdl) = 0;
  virtual bool UnRegisterMsgHandler(MessageHandler* msgHdl) = 0;
  virtual bool isMsgHandlerRegistered(MessageHandler* msgHdl) = 0;
  virtual void UnRegisterAllMsgHandlers() = 0;

  /** Return true if the timer is successfully registered, otherwise (e.g. it
   * has been registered before and has not been unreigstered), return false */
  bool RegisterTimer(Timer* timer);
  /** Return true if the timer is successfully registered, otherwise (e.g. the
   * timer has not been registered before), return false */
  bool UnRegisterTimer(Timer* timer);
  /** Check whether the timer has been registered */
  bool isTimerRegistered(Timer* timer);
  void UnRegisterAllTimers();

  void LoopRun();
  void LoopBreak();
};

#endif