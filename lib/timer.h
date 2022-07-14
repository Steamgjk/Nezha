#ifndef NEZHA_TIMER_
#define NEZHA_TIMER_

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
 * Timer is an encapsulation of libev-based message handler (i.e.
 * ev_timer).
 *
 * After the timer is created, it will be registered to a
 * specific endpoint, together with a period (measures in milliseconds). Then,
 * the callback func (i.e., TimerFunc) will be called periodically until the
 * timer is unregistered
 * **/

/**
 * Para-1: The first void* points to the context, that may be needed by the
 * callback function(i.e., TimerFunc)
 * Para-2: The first void* points to the endpoint that this timer is attached
 * to. It can be passed into the function as NULL if the TimerFunc does not need
 * it. But some TimerFunc (e.g., monitorTimer in replica) callback needs to know
 * the endpoint it has attached to.
 */

typedef std::function<void(void*, void*)> TimerFunc;

struct Timer {
  std::function<void(void*, void*)> timerFunc_;
  void* context_;
  void* attachedEndpoint_;
  struct ev_timer* evTimer_;

  Timer(TimerFunc timerf, uint32_t periodMs = 1, void* ctx = NULL,
        void* aep = NULL)
      : timerFunc_(timerf), context_(ctx), attachedEndpoint_(aep) {
    evTimer_ = new ev_timer();
    evTimer_->data = (void*)this;
    evTimer_->repeat = periodMs * 1e-3;
    ev_init(evTimer_,
            [](struct ev_loop* loop, struct ev_timer* w, int revents) {
              Timer* t = (Timer*)(w->data);
              t->timerFunc_(t->context_, t->attachedEndpoint_);
            });
  }
  ~Timer() { delete evTimer_; }
};

#endif