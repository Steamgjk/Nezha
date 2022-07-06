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



struct UDPMsgHandler : EndpointMsgHandler {
    char buffer_[UDP_BUFFER_SIZE];
    UDPMsgHandler(std::function<void(MessageHeader*, char*, Address*, void*, Endpoint*)> msghdl,
        void* ctx = NULL, Endpoint* aep = NULL) :EndpointMsgHandler(msghdl, ctx, aep) {
        ev_init(evWatcher_, [](struct ev_loop* loop, struct ev_io* w, int revents) {
            UDPMsgHandler* m = (UDPMsgHandler*)(w->data);
            if (m->attachedEP_ == NULL) {
                LOG(ERROR) << "This message handler is not attached to any endpoints";
                return;
            }
            socklen_t sockLen = sizeof(struct sockaddr_in);
            int msgLen = recvfrom(w->fd, m->buffer_, UDP_BUFFER_SIZE, 0,
                (struct sockaddr*)(&(m->sender_.addr_)), &sockLen);
            MessageHeader* msgHeader = (MessageHeader*)(void*)(m->buffer_);

            if (msgLen < 0 || (uint32_t)msgLen < sizeof(MessageHeader)) {
                msgHeader->msgType = MessageType::ERROR_MSG;
                msgHeader->msgLen = 0;
            }
            m->msgHandler_(msgHeader, m->buffer_ + sizeof(MessageHeader),
                &(m->sender_), m->context_, m->attachedEP_);
            });
    }
    ~UDPMsgHandler() {}
};


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