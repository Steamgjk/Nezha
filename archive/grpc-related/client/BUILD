load("@rules_proto//proto:defs.bzl", "proto_library")
load("@com_github_grpc_grpc//bazel:cc_grpc_library.bzl", "cc_grpc_library")
load("@com_github_grpc_grpc//bazel:grpc_build_system.bzl", "grpc_proto_library")


cc_library(
    name = "client_class",
    srcs = ["client.cc"],
    hdrs = ["client.h"],
    deps = [
        "//proto:nezha_cc_proto",
        "//lib:udp_socket_endpoint",
        "//lib:zipfian"
    ],
)


cc_binary(
    name = "nezha_client",
    srcs = ["client-run.cc"],
    deps = [
        ":client_class"
    ],
    copts = [
        "-I/usr/local/include"
    ],
    linkopts = [ "-L/usr/local/lib", "-lev", "-ldl", "-lturf", "-ljunction", "-lcrypto", "-lgflags",  "-lglog",  "-lyaml-cpp", "-pthread" ],

)



cc_binary(
    name = "nezha_grpc_client",
    srcs = ["grpc-client.cc"],
    deps = [
        "//proto:kube_grpc_service_grpc",
        "@com_github_grpc_grpc//:grpc++",
        "@com_github_grpc_grpc//:grpc++_reflection"
    ],
    copts = [
        "-I/usr/local/include",
    ],
    linkopts = [ "-L/usr/local/lib", "-L/usr/local/grpc/lib", 
                "-lev", "-ldl", "-lturf", "-ljunction", 
                 "-lcrypto", "-lgflags",  "-lglog",  "-lyaml-cpp", "-pthread" ],
)


