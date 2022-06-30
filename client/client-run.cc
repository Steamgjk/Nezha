#include "client/client.h"
DEFINE_string(config, "nezhav2/config/nezha-client-config-0.yaml", "The config file for the client");
nezha::Client* client = NULL;
void Terminate(int para) {
    client->Terminate();
}
int main(int argc, char* argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = 1;
    signal(SIGINT, Terminate);
    client = new nezha::Client(FLAGS_config);
    client->Run();
    delete client;
}