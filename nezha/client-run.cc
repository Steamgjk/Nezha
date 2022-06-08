#include "nezha/client.h"

nezha::Client* client = NULL;
void Terminate(int para) {
    client->Terminate();
}
int main(int argc, char* argv[]) {
    signal(SIGINT, Terminate);
    std::string config = "";
    if (argc >= 2) {
        config = argv[1];
    }
    client = new nezha::Client(config);
    client->Run();
    delete client;
}