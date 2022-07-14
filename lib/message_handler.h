
#ifndef NEZHA_MESSAGE_HANDLER_H
#define NEZHA_MESSAGE_HANDLER_H

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
#include "lib/common_type.h"

/**
 * MessageHandler is an encapsulation of libev-based message handler (i.e.
 * ev_io).
 *
 * After the message handler is created, it will be registered to a
 * specific endpoint. Then, the callback func (i.e., MessageHandlerFunc) will be
 * called every time this endpoint receives some messages.
 *
 * Currently, we only support UDP communication. Therefore, we only have one
 * derived struct (UDPMsgHandler) from MessageHandler
 *
 * We will continue to support other types of endpoints. Correspondingly, there
 * will be more derived struct added later
 * **/

/**
 * Para-1: MessageHeader* describes the type and length of the received message
 * Para-2: char* is the payload of the message
 * Para-3: Address* is the address of the sender
 * Para-4: void* points to the (optional) context that is needed by the callback
 * function(i.e., MessageHandlerFunc)
 */
typedef std::function<void(MessageHeader*, char*, Address*, void*)>
    MessageHandlerFunc;

struct MessageHandler {
  MessageHandlerFunc msgHandler_;
  void* context_;
  Address sender_;
  struct ev_io* evWatcher_;
  MessageHandler(MessageHandlerFunc msghdl, void* ctx = NULL)
      : msgHandler_(msghdl), context_(ctx) {
    evWatcher_ = new ev_io();
    evWatcher_->data = (void*)this;
  }
  ~MessageHandler() { delete evWatcher_; }
};

struct UDPMsgHandler : MessageHandler {
  char buffer_[UDP_BUFFER_SIZE];
  UDPMsgHandler(MessageHandlerFunc msghdl, void* ctx = NULL)
      : MessageHandler(msghdl, ctx) {
    ev_init(evWatcher_, [](struct ev_loop* loop, struct ev_io* w, int revents) {
      UDPMsgHandler* m = (UDPMsgHandler*)(w->data);
      socklen_t sockLen = sizeof(struct sockaddr_in);
      int msgLen = recvfrom(w->fd, m->buffer_, UDP_BUFFER_SIZE, 0,
                            (struct sockaddr*)(&(m->sender_.addr_)), &sockLen);
      if (msgLen > 0 && (uint32_t)msgLen > sizeof(MessageHeader)) {
        MessageHeader* msgHeader = (MessageHeader*)(void*)(m->buffer_);
        if (msgHeader->msgLen + sizeof(MessageHeader) >= (uint32_t)msgLen) {
          m->msgHandler_(msgHeader, m->buffer_ + sizeof(MessageHeader),
                         &(m->sender_), m->context_);
        }
      }
    });
  }
  ~UDPMsgHandler() {}
};

#endif