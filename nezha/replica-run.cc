#include "nezha/replica.h"
nezha::Replica* replica = NULL;
void Terminate(int para) {
    replica->Terminate();
}
int main(int argc, char* argv[]) {
    std::string config = "";
    if (argc >= 2) {
        config = argv[1];
    }
    replica = new nezha::Replica(config);
    // replica->Run();
    delete replica;
    return 0;
}