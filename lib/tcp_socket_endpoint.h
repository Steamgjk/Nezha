#ifndef NEZHA_TCP_SOCKET_ENDPOINT_H
#define NEZHA_TCP_SOCKET_ENDPOINT_H 

#include <string>
#include <netinet/in.h>
#include <glog/logging.h>
#include <ev.h>
#include <arpa/inet.h>
#include <functional>
#include <set>
#include <fcntl.h>
#include <google/protobuf/message.h>
#include "lib/endpoint.h"



struct TCPMsgHandler {
    std::function<void(MessageHeader*, char*, Address*, void*, Endpoint*)> handleFunc_;
    void* context_;
    Endpoint* attachedEP_;
    struct ev_io* evWatcher_;
    char header_[sizeof(MessageHeader)];
    char* payload_;
    uint32_t currentHeaderLen_;
    uint32_t currentPayloadLen_;
    Address sender_;

    TCPMsgHandler(std::function<void(MessageHeader*, char*, Address*, void*, Endpoint*)> func,
        void* ctx = NULL, Endpoint* aep = NULL)
        :handleFunc_(func), context_(ctx), attachedEP_(aep),
        payload_(NULL), currentHeaderLen_(0), currentPayloadLen_(0) {
        evWatcher_ = new ev_io();
        evWatcher_->data = (void*)this;

        ev_init(evWatcher_, [](struct ev_loop* loop, struct ev_io* w, int revents) {
            TCPMsgHandler* m = (TCPMsgHandler*)(w->data);
            if (m->attachedEP_ == NULL) {
                LOG(ERROR) << "This message handler is not attached to any endpoints";
                return;
            }

            socklen_t sockLen = sizeof(struct sockaddr_in);

            while (m->currentHeaderLen_ < sizeof(MessageHeader)) {
                int msgLen = recvfrom(w->fd, (m->header_) + m->currentHeaderLen_,
                    sizeof(MessageHeader) - m->currentHeaderLen_, 0,
                    (struct sockaddr*)(&(m->sender_.addr_)), &sockLen);
                if (msgLen > 0) {
                    m->currentHeaderLen_ += msgLen;
                }
                else {
                    break;
                }
            }

            if (m->currentHeaderLen_ == sizeof(MessageHeader)) {
                MessageHeader* msgHeader = (MessageHeader*)(void*)(m->header_);
                while (m->currentPayloadLen_ < msgHeader->msgLen) {
                    int msgLen = recvfrom(w->fd, (m->payload_) + m->currentPayloadLen_,
                        msgHeader->msgLen - m->currentPayloadLen_, 0,
                        (struct sockaddr*)(&(m->sender_.addr_)), &sockLen);
                    if (msgLen > 0) {
                        m->currentPayloadLen_ += msgLen;
                    }
                    else {
                        break;
                    }
                }
                if (m->currentPayloadLen_ == msgHeader->msgLen) {
                    m->handleFunc_(msgHeader, m->payload_, &(m->sender_),
                        m->context_, m->attachedEP_);
                    m->currentPayloadLen_ = 0;
                    m->currentHeaderLen_ = 0;
                    delete[](m->payload_);
                    m->payload_ = NULL;
                }
            }

            }
        );
    }
    ~TCPMsgHandler() {
        if (payload_) delete[] payload_;
    }

};

struct TCPChannel {
    TCPMsgHandler* msgHandler_;
    uint64_t myPoint_;
    int myFd_;
    TCPChannel(TCPMsgHandler* hdl = NULL) : msgHandler_(hdl), myPoint_(0), myFd_(-1) {}
};

class TCPSocketEndpoint :public Endpoint
{
private:
    std::unordered_map<uint64_t, TCPChannel*> channels_; // Each channel has a unique channelId, i.e., <dstIP,dstPort>
    struct ev_io* connectionWatcher_;

public:
    TCPSocketEndpoint();
    TCPSocketEndpoint(const std::string& sip, const int sport, const bool isMasterReceiver = false);
    TCPSocketEndpoint(const Address& addr, const bool isMasterReceiver = false);
    ~TCPSocketEndpoint();
    int SendMsgTo(const Address& dstAddr,
        const google::protobuf::Message& msg, const char msgType);
    int ConnectTo(const Address& myAddr, const Address& dstAddr);
    bool RegisterMsgHandler(uint64_t channelId, TCPMsgHandler* tcpHdl);
    bool UnRegisterMsgHandler(uint64_t channelId);
    bool isRegistered(uint64_t channelId);
    TCPChannel* GetChannel(uint64_t channelId);

};




#endif