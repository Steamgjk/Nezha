#ifndef NEZHA_TCP_SOCKET_ENDPOINT_H
#define NEZHA_TCP_SOCKET_ENDPOINT_H 

#include "lib/endpoint.h"



struct TCPMsgHandler : EndpointMsgHandler {
    char header_[sizeof(MessageHeader)];
    char* payload_;
    uint32_t currentHeaderLen_;
    uint32_t currentPayloadLen_;
    TCPMsgHandler(std::function<void(MessageHeader*, char*, Address*, void*, Endpoint*)> func,
        void* ctx = NULL, Endpoint* aep = NULL)
        :EndpointMsgHandler(func, ctx, aep),
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
                    if (m->msgHandler_) {
                        m->msgHandler_(msgHeader, m->payload_, &(m->sender_),
                            m->context_, m->attachedEP_);
                    }
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

class TCPSocketEndpoint :public Endpoint
{
private:
    std::unordered_set<struct TCPMsgHandler*>msgHandlers_;
    std::unordered_map<uint64_t, struct TCPMsgHandler*> channelMsgHandlers_;
    std::unordered_map<uint64_t, int> channelFds_;

    struct ev_io* connectionWatcher_;
    std::function<void(MessageHeader*, char*, Address*, void*, Endpoint*)> serverCallBack_;
    // This func is used to handle incoming messages to this endpoint
    void* context_;
public:
    TCPSocketEndpoint(const std::string& sip = "", const int sport = -1, const bool isMasterReceiver = false);
    ~TCPSocketEndpoint();
    int SendMsgTo(const Address& dstAddr,
        const google::protobuf::Message& msg, const char msgType);
    uint64_t ConnectTo(const Address& dstAddr);

    void RegisterServerCallBack(std::function<void(MessageHeader*, char*, Address*, void*, Endpoint*)>& func, void* ctx);
    void UnRegisterServerCallBack();

    bool RegisterMsgHandler(const Address& dstAddr, TCPMsgHandler* tcpMsgHdl);
    bool UnRegisterMsgHandler(const Address& dstAddr);

};




#endif