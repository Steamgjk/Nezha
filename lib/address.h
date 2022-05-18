#ifndef NEZHA_ADDRESS 
#define NEZHA_ADDRESS

#include <string>

namespace nezha_network
{
    class Address
    {
    public:
        std::string ip_;
        int port_;
        std::string mac_; // For future extension (DPDK)
        struct sockaddr_in addr_;

        Address();
        Address(const std::string& ip, const int port, const std::string& mac = std::string(""));
        ~Address();

    };
    Address::Address() :ip_(""), port_(-1), mac_("") { bzero(&addr_, sizeof(addr_)); }
    Address::Address(const std::string& ip, const int port, const std::string& mac) : ip_(ip), port_(port), mac_(mac) {
        bzero(&addr_, sizeof(addr_));
        addr_.sin_family = AF_INET;
        addr_.sin_port = htons(port);
        addr.sin_addr.s_addr = inet_addr(ip.c_str());
    }
    Address::~Address() {}


} // namespace nezha_network


#endif 