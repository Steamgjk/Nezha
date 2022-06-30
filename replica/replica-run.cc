#include "replica/replica.h"
DEFINE_string(config, "nezhav2/config/nezha-replica-config-0.yaml", "The config file for the replica");
DEFINE_bool(isRecovering, false, "If this flag is true, then the replica will start recovery process once it is launched");

nezha::Replica* replica = NULL;
void Terminate(int para) {
    LOG(INFO) << "Terminating...";
    replica->Terminate();
}
int main(int argc, char* argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = 1;
    signal(SIGINT, Terminate);
    replica = new nezha::Replica(FLAGS_config, FLAGS_isRecovering);
    replica->Run();
    LOG(INFO) << " Run Completed";
    delete replica;
    return 0;
}