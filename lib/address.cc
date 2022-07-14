#include "lib/address.h"

Address::Address() : ip_(""), port_(-1), mac_("") {
  bzero(&addr_, sizeof(addr_));
}
Address::Address(const std::string& ip, const int port, const std::string& mac)
    : ip_(ip), port_(port), mac_(mac) {
  bzero(&addr_, sizeof(addr_));
  addr_.sin_family = AF_INET;
  addr_.sin_port = htons(port);
  addr_.sin_addr.s_addr = inet_addr(ip.c_str());
}
Address::~Address() {}

std::string Address::GetIPAsString() {
  ip_ = inet_ntoa(addr_.sin_addr);
  return ip_;
}

int Address::GetPortAsInt() {
  port_ = htons(addr_.sin_port);
  return port_;
}