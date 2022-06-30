#include <stdio.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <ev.h>
#include <strings.h>
#include <chrono>
#include <unistd.h>
#include <arpa/inet.h>
#include <iostream>
#include <thread>
#include <vector>
#include <fcntl.h>
#include <fstream>
#include <glog/logging.h>
#include <junction/ConcurrentMap_Leapfrog.h>
#include <yaml-cpp/yaml.h>
#include <concurrentqueue.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include "proto/nezha-proto.pb.h"
#include "proto/kube-grpc-service.pb.h"
#include "proto/kube-grpc-service.grpc.pb.h"
#include "lib/utils.h"
#include "lib/address.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

using grpc::ServerAsyncResponseWriter;
using grpc::ServerCompletionQueue;
using grpc::ServerContext;


namespace nezha {
    namespace kubeproxy {
        // Logic and data behind the server's behavior.
        class GreeterServiceImpl final : public Greeter::Service {
            Status SayHello(ServerContext* context, const HelloRequest* request,
                HelloReply* reply) override {
                std::string prefix("Hello ");
                reply->set_message(prefix + request->name());
                return Status::OK;
            }
        };


        class ServerImpl final {
        public:
            ~ServerImpl() {
                server_->Shutdown();
                // Always shutdown the completion queue after the server.
                cq_->Shutdown();
            }

            // There is no shutdown handling in this code.
            void Run() {
                std::string server_address("0.0.0.0:50051");

                ServerBuilder builder;
                // Listen on the given address without any authentication mechanism.
                builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
                // Register "service_" as the instance through which we'll communicate with
                // clients. In this case it corresponds to an *asynchronous* service.
                builder.RegisterService(&service_);
                // Get hold of the completion queue used for the asynchronous communication
                // with the gRPC runtime.
                cq_ = builder.AddCompletionQueue();
                // Finally assemble the server.
                server_ = builder.BuildAndStart();
                std::cout << "Async Server listening on " << server_address << std::endl;

                // Proceed to the server's main loop.
                HandleRpcs();
            }

        private:
            // Class encompasing the state and logic needed to serve a request.
            class CallData {
            public:
                // Take in the "service" instance (in this case representing an asynchronous
                // server) and the completion queue "cq" used for asynchronous communication
                // with the gRPC runtime.
                CallData(Greeter::AsyncService* service, ServerCompletionQueue* cq)
                    : service_(service), cq_(cq), responder_(&ctx_), status_(CREATE) {
                    // Invoke the serving logic right away.
                    Proceed();
                }

                void Proceed() {
                    std::cout << "Status = " << status_ << std::endl;
                    if (status_ == CREATE) {
                        // Make this instance progress to the PROCESS state.
                        status_ = PROCESS;

                        // As part of the initial CREATE state, we *request* that the system
                        // start processing SayHello requests. In this request, "this" acts are
                        // the tag uniquely identifying the request (so that different CallData
                        // instances can serve different requests concurrently), in this case
                        // the memory address of this CallData instance.
                        service_->RequestSayHello(&ctx_, &request_, &responder_, cq_, cq_,
                            this);
                    }
                    else if (status_ == PROCESS) {
                        // Spawn a new CallData instance to serve new clients while we process
                        // the one for this CallData. The instance will deallocate itself as
                        // part of its FINISH state.
                        new CallData(service_, cq_);

                        // The actual processing.
                        std::string prefix("Hello ");
                        reply_.set_message(prefix + request_.name());

                        // And we are done! Let the gRPC runtime know we've finished, using the
                        // memory address of this instance as the uniquely identifying tag for
                        // the event.
                        status_ = FINISH;
                        responder_.Finish(reply_, Status::OK, this);
                    }
                    else {
                        GPR_ASSERT(status_ == FINISH);
                        // Once in the FINISH state, deallocate ourselves (CallData).
                        delete this;
                    }
                }

            private:
                // The means of communication with the gRPC runtime for an asynchronous
                // server.
                Greeter::AsyncService* service_;
                // The producer-consumer queue where for asynchronous server notifications.
                ServerCompletionQueue* cq_;
                // Context for the rpc, allowing to tweak aspects of it such as the use
                // of compression, authentication, as well as to send metadata back to the
                // client.
                ServerContext ctx_;

                // What we get from the client.
                HelloRequest request_;
                // What we send back to the client.
                HelloReply reply_;

                // The means to get back to the client.
                ServerAsyncResponseWriter<HelloReply> responder_;

                // Let's implement a tiny state machine with the following states.
                enum CallStatus { CREATE, PROCESS, FINISH };
                CallStatus status_;  // The current serving state.
            };

            // This can be run in multiple threads if needed.
            void HandleRpcs() {
                // Spawn a new CallData instance to serve new clients.
                new CallData(&service_, cq_.get());
                void* tag;  // uniquely identifies a request.
                bool ok;
                while (true) {
                    // Block waiting to read the next event from the completion queue. The
                    // event is uniquely identified by its tag, which in this case is the
                    // memory address of a CallData instance.
                    // The return value of Next should always be checked. This return value
                    // tells us whether there is any kind of event or cq_ is shutting down.
                    GPR_ASSERT(cq_->Next(&tag, &ok));
                    GPR_ASSERT(ok);
                    std::cout << "Proceed... " << std::endl;
                    static_cast<CallData*>(tag)->Proceed();
                }
            }

            std::unique_ptr<ServerCompletionQueue> cq_;
            Greeter::AsyncService service_;
            std::unique_ptr<Server> server_;
        };


    }

    using namespace nezha::proto;


    template<typename T1> using ConcurrentQueue = moodycamel::ConcurrentQueue<T1>;
    template<typename T1, typename T2> using ConcurrentMap = junction::ConcurrentMap_Leapfrog<T1, T2>;
    class GRPCProxy
    {
    private:
        std::map<std::string, std::thread*>threadPool_;
        YAML::Node proxyConfig_;
        void PrintConfig();
        void LaunchThreads();
        void CheckQuorum(const int id = -1);
        void ForwardRequests(const int id = -1);
        void CreateContext();
        void CalcLatencyBound();
        Reply* QuorumReady(std::map<uint32_t, Reply>& quorum);
        int CreateSocketFd(const std::string& sip = std::string(""), const int sport = -1);
        std::atomic<bool> stopForwarding_;
        std::atomic<bool> running_;
        std::vector<int> replyFds_;
        std::vector<int> forwardFds_;
        std::vector<int> requestReceiveFds_;
        std::vector<uint64_t> proxyIds_;
        std::atomic<uint32_t> latencyBound_;
        uint32_t maxOWD_; // Upper bound of the estimated latencyBound_
        ConcurrentQueue<std::pair<uint32_t, uint32_t>> owdQu_; // <replicaId, owd>


        int replicaNum_;
        int f_;
        int fastQuorum_;
        std::vector<std::vector<struct sockaddr_in*>> replicaAddrs_;
        ConcurrentMap<uint32_t, struct sockaddr_in*> clientAddrs_;
        ConcurrentMap<uint32_t, Reply*> committedReply_; // used as cache


    public:
        GRPCProxy(const std::string& configFile = std::string("../configs/nezha-proxy-config.yaml"));
        ~GRPCProxy();
        void Run();
        void Terminate();
    };

}