#include "nezha/replica.h"
nezha::Replica* replica = NULL;
void Terminate(int para) {
    LOG(INFO) << "Terminating...";
    replica->Terminate();
}
int main(int argc, char* argv[]) {
    std::string config = "";
    if (argc >= 2) {
        config = argv[1];
    }
    signal(SIGINT, Terminate);
    replica = new nezha::Replica(config);
    replica->Run();
    LOG(INFO) << " Run Completed";
    delete replica;
    return 0;
}