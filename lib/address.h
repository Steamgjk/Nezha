#ifndef NEZHA_ADDRESS 
#define NEZHA_ADDRESS
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <cstring>
#include <string>

#define UDP_BUFFER_SIZE (512)

class Address
{
public:
    std::string ip_;
    int port_;
    std::string mac_; // For future extension (DPDK)
    struct sockaddr_in addr_;

    Address();
    Address(const Address& addr) :ip_(addr.ip_), port_(addr.port_), mac_(addr.mac_) {
        memcpy(&addr_, &(addr.addr_), sizeof(struct sockaddr_in));
    }
    Address(const std::string& ip, const int port, const std::string& mac = std::string(""));
    ~Address();

};



#endif 