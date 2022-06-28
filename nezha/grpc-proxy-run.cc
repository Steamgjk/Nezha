#include "nezha/grpc-proxy.h"
DEFINE_string(config, "nezhav2/config/nezha-proxy-config-0.yaml", "The config file for the proxy");

using namespace nezha::kubeproxy;

nezha::GRPCProxy* proxy = NULL;

// void Terminate(int para) {
//     proxy->Terminate();
// }


void RunServer() {
    std::string server_address("127.0.0.1:50051");
    GreeterServiceImpl service;

    grpc::EnableDefaultHealthCheckService(true);
    grpc::reflection::InitProtoReflectionServerBuilderPlugin();
    ServerBuilder builder;
    // Listen on the given address without any authentication mechanism.
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    // Register "service" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *synchronous* service.
    builder.RegisterService(&service);
    // Finally assemble the server.
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;

    // Wait for the server to shutdown. Note that some other thread must be
    // responsible for shutting down the server for this call to ever return.
    server->Wait();
}

int main(int argc, char* argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = 1;
    // signal(SIGINT, Terminate);
    // proxy = new nezha::GRPCProxy(FLAGS_config);
    // proxy->Run();
    // delete proxy;

    // RunServer();
    ServerImpl server;
    server.Run();
}
