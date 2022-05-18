#ifndef NEZHA_ADDRESS 
#define NEZHA_ADDRESS

#include <string>


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



#endif 