#ifndef NEZHA_UDP_SOCKET_SENDER_H
#define NEZHA_UDP_SOCKET_SENDER_H 

#include <string>
#include <netinet/in.h>
#include <glog/logging.h>
#include <ev.h>
#include <arpa/inet.h>
#include <functional>
#include <junction/ConcurrentMap_Leapfrog.h>
#include "lib/address.h"

namespace nezha_network {
#define BUFFER_SIZE (65535)

    class UDPSocketEndpoint
    {
    private:
        /* data */
        Address addr_;
        int fd_;
        struct ev_loop* evLoop_;
        junction::ConcurrentMap_Leapfrog<uint64_t, struct ParaStruct*> evWatchers_;

    public:
        UDPSocketEndpoint();
        UDPSocketEndpoint(const std::string& sip, const int sport, const bool isMasterReceiver = false);
        UDPSocketEndpoint(const Address& addr, const bool isMasterReceiver = false);
        ~UDPSocketEndpoint();

        int SendMsgTo(const Address& dstAddr, const std::string& msg);
        int SendMsgTo(const Address& dstAddr, const char* buffer, const uint32_t bufferLen);

        uint64_t RegisterReceiveHandler(std::function<void(char*, int, void*)> msgHandler, void* context);
        bool UnregisterReceiveHandler(uint64_t key);
        void ReceiverRun();

    };
    struct ParaStruct {
        std::function<void(char*, int, void*)> msgHandler_;
        UDPSocketEndpoint* thisPtr_;
        void* context_;
        char* buffer_;
        struct ev_io* evWatcher_;

        ParaStruct(std::function<void(char*, int, void*)> msghdl, UDPSocketEndpoint* t = NULL, void* ctx = NULL, char* buf = NULL, struct ev_io* w) :msgHandler_(msghdl), thisPtr_(t), context_(ctx), buffer_(buf), evWatcher_(w) {}
    };
}




#endif 