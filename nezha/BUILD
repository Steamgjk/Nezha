load("@rules_proto//proto:defs.bzl", "proto_library")
load("@com_github_grpc_grpc//bazel:cc_grpc_library.bzl", "cc_grpc_library")
load("@com_github_grpc_grpc//bazel:grpc_build_system.bzl", "grpc_proto_library")

proto_library(
    name = "nezha_proto",
    srcs = ["nezha-proto.proto"],
    visibility = ["//visibility:public"],
)

cc_proto_library(
    name = "nezha_cc_proto",
    deps = [":nezha_proto"],
    visibility = ["//visibility:public"],
)

proto_library(
    name = "kube_grpc_service_proto",
    srcs = ["kube-grpc-service.proto"],
    visibility = ["//visibility:public"],
)

cc_proto_library(
    name = "kube_grpc_service_cc",
    deps = [":kube_grpc_service_proto"],
    visibility = ["//visibility:public"],
)

cc_grpc_library(
    name = "kube_grpc_service_grpc",
    srcs = [":kube_grpc_service_proto"],
    grpc_only = True,
    deps = [":kube_grpc_service_cc"],
    visibility = ["//visibility:public"],
)





cc_library(
    name = "replica_class",
    srcs = ["replica.cc"],
    hdrs = ["replica.h"],
    deps = [
        ":nezha_cc_proto",
        "//lib:utils",
        "//lib:udp_socket_endpoint"
    ],

)


cc_library(
    name = "proxy_class",
    srcs = ["proxy.cc"],
    hdrs = ["proxy.h"],
    deps = [
        ":nezha_cc_proto",
        "//lib:utils",
        "//lib:address",
    ],
)


cc_library(
    name = "grpc_proxy_class",
    srcs = ["grpc-proxy.cc"],
    hdrs = ["grpc-proxy.h"],
    deps = [
        ":nezha_cc_proto",
        ":kube_grpc_service_grpc",
        "//lib:utils",
        "//lib:address",
        "@com_github_grpc_grpc//:grpc++",
        "@com_github_grpc_grpc//:grpc++_reflection"
    ],
)


cc_library(
    name = "client_class",
    srcs = ["client.cc"],
    hdrs = ["client.h"],
    deps = [
        ":nezha_cc_proto",
        "//lib:udp_socket_endpoint",
        "//lib:zipfian"
    ],
)


cc_binary(
    name = "nezha_replica",
    srcs = ["replica-run.cc"],
    deps = [
        ":replica_class"
    ],
    copts = [
        "-I/usr/local/include"
    ],
    linkopts = [ "-L/usr/local/lib", "-lev", "-ldl", "-lturf", "-ljunction", "-lcrypto", "-lgflags",  "-lglog",  "-lyaml-cpp", "-pthread" ],

)

cc_binary(
    name = "nezha_proxy",
    srcs = ["proxy-run.cc"],
    deps = [
        ":proxy_class"
    ],
    copts = [
        "-I/usr/local/include"
    ],
    linkopts = [ "-L/usr/local/lib", "-lev", "-ldl", "-lturf", "-ljunction", "-lcrypto", "-lgflags",  "-lglog",  "-lyaml-cpp", "-pthread" ],

)


cc_binary(
    name = "nezha_grpc_proxy",
    srcs = ["grpc-proxy-run.cc"],
    deps = [
        ":grpc_proxy_class"
    ],
    copts = [
        "-I/usr/local/include",
    ],
    linkopts = [ "-L/usr/local/lib", "-L/usr/local/grpc/lib", 
                "-lev", "-ldl", "-lturf", "-ljunction", 
                 "-lcrypto", "-lgflags",  "-lglog",  "-lyaml-cpp", "-pthread" ],
)


cc_binary(
    name = "nezha_grpc_client",
    srcs = ["grpc-client.cc"],
    deps = [
        ":grpc_proxy_class"
    ],
    copts = [
        "-I/usr/local/include",
    ],
    linkopts = [ "-L/usr/local/lib", "-L/usr/local/grpc/lib", 
                "-lev", "-ldl", "-lturf", "-ljunction", 
                 "-lcrypto", "-lgflags",  "-lglog",  "-lyaml-cpp", "-pthread" ],
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
