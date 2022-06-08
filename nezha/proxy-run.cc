#include "nezha/proxy.h"
nezha::Proxy* proxy = NULL;
void Terminate(int para) {
    proxy->Terminate();
}
int main(int argc, char* argv[]) {
    signal(SIGINT, Terminate);
    std::string config = "";
    if (argc >= 2) {
        config = argv[1];
    }
    proxy = new nezha::Proxy(config);
    proxy->Run();
    delete proxy;
}
