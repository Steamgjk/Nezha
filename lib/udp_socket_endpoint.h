#ifndef NEZHA_UDP_SOCKET_SENDER_H
#define NEZHA_UDP_SOCKET_SENDER_H 

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
#include "lib/endpoint.h"


struct UDPMsgHandler {
    std::function<void(char*, int, Address*, void*, Endpoint*)> msgHandler_;
    void* context_;
    Endpoint* attachedEP_;
    char* buffer_;
    Address sender_;
    struct ev_io* evWatcher_;

    UDPMsgHandler(std::function<void(char*, int, Address*, void*, Endpoint*)> msghdl,
        void* ctx = NULL, Endpoint* aep = NULL)
        :msgHandler_(msghdl), context_(ctx), attachedEP_(aep) {

        buffer_ = new char[UDP_BUFFER_SIZE];
        evWatcher_ = new ev_io();
        evWatcher_->data = (void*)this;

        ev_init(evWatcher_, [](struct ev_loop* loop, struct ev_io* w, int revents) {
            UDPMsgHandler* m = (UDPMsgHandler*)(w->data);
            if (m->attachedEP_ == NULL) {
                LOG(ERROR) << "This message handler is not attached to any endpoints";
                return;
            }
            socklen_t sockLen = sizeof(struct sockaddr_in);
            int msgLen = recvfrom(w->fd, m->buffer_, UDP_BUFFER_SIZE, 0,
                (struct sockaddr*)(&(m->sender_.addr_)), &sockLen);
            m->msgHandler_(m->buffer_, msgLen, &(m->sender_), m->context_, m->attachedEP_);
            });
    }
    ~UDPMsgHandler() {
        delete evWatcher_;
        delete[]buffer_;
    }
};


// TODO: In the future, we will extract a base class, and from that class, we derive two sub-classes: one for UDP and the other for TCP

class UDPSocketEndpoint :public Endpoint
{
private:
    /* data */
    std::set<struct UDPMsgHandler*> msgHandlers_;
public:

    UDPSocketEndpoint();
    UDPSocketEndpoint(const std::string& sip, const int sport, const bool isMasterReceiver = false);
    UDPSocketEndpoint(const Address& addr, const bool isMasterReceiver = false);
    ~UDPSocketEndpoint();

    int SendMsgTo(const Address& dstAddr, const google::protobuf::Message& msg, const char msgType);

    bool RegisterMsgHandler(UDPMsgHandler* msgHdl);
    bool UnregisterMsgHandler(UDPMsgHandler* msgHdl);
    bool isRegistered(UDPMsgHandler* msgHdl);

    void LoopRun();
    void LoopBreak();
};




#endif 