#include <stdio.h>
#include  <ev.h>
#include "concurrentqueue.h"
#include "yaml-cpp/yaml.h"

template<typename T1> using ConcurrentQueue = moodycamel::ConcurrentQueue<T1>;
int main(int argc, char* argv[]) {
    ConcurrentQueue<int> queue;
    int a = 1;
    queue.enqueue(a);

    struct ev_loop* evLoop_ = ev_loop_new();;
    struct ev_timer* evTimer_ = new ev_timer();
    evTimer_->data = NULL;
    evTimer_->repeat = 10 * 1e-3;
    ev_init(evTimer_, [](struct ev_loop* loop, struct ev_timer* w, int revents) {
        printf("OK");
        });

    ev_timer_again(evLoop_, evTimer_);
    ev_run(evLoop_);

    std::string configFile = "";
    YAML::Node replicaConfig_ = YAML::LoadFile(configFile);

}