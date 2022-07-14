#include "lib/endpoint.h"

Endpoint::Endpoint(const std::string& sip, const int sport,
                   const bool isMasterReceiver)
    : addr_(sip, sport) {
  evLoop_ = isMasterReceiver ? ev_default_loop() : ev_loop_new();
  if (!evLoop_) {
    LOG(ERROR) << "Event Loop error";
    return;
  }
}

Endpoint::~Endpoint() {
  LoopBreak();
  ev_loop_destroy(evLoop_);
}

bool Endpoint::RegisterTimer(Timer* timer) {
  if (evLoop_ == NULL) {
    LOG(ERROR) << "No evLoop!";
    return false;
  }

  if (isTimerRegistered(timer)) {
    LOG(ERROR) << "This timer has already been registered";
    return false;
  }

  timer->attachedEndpoint_ = this;
  eventTimers_.insert(timer);
  ev_timer_again(evLoop_, timer->evTimer_);
  return true;
}

bool Endpoint::UnRegisterTimer(Timer* timer) {
  if (evLoop_ == NULL) {
    LOG(ERROR) << "No evLoop!";
    return false;
  }
  if (!isTimerRegistered(timer)) {
    LOG(ERROR) << "The timer has not been registered ";
    return false;
  }
  ev_timer_stop(evLoop_, timer->evTimer_);
  eventTimers_.erase(timer);
  return true;
}

void Endpoint::UnRegisterAllTimers() {
  for (auto& t : eventTimers_) {
    ev_timer_stop(evLoop_, t->evTimer_);
  }
  eventTimers_.clear();
}

bool Endpoint::isTimerRegistered(Timer* timer) {
  return (eventTimers_.find(timer) != eventTimers_.end());
}

void Endpoint::LoopRun() { ev_run(evLoop_, 0); }

void Endpoint::LoopBreak() {
  UnRegisterAllTimers();
  ev_break(evLoop_, EVBREAK_ALL);
}
