load("@rules_proto//proto:defs.bzl", "proto_library")


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
