load("@rules_proto//proto:defs.bzl", "proto_library")

cc_library(
    name = "zipfian",
    srcs = ["zipfian.h"],
    hdrs = ["zipfian.h"],
    visibility = ["//visibility:public"],
)


cc_library(
    name = "message_type",
    srcs = ["message_type.cc"],
    hdrs = ["message_type.h"],
    visibility = ["//visibility:public"],
)



cc_library(
    name = "address",
    srcs = ["address.cc"],
    hdrs = ["address.h"],
    visibility = ["//visibility:public"],
)


cc_library(
    name = "endpoint",
    srcs = ["endpoint.cc"],
    hdrs = ["endpoint.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":address",
        ":message_type"
    ],
)


cc_library(
    name = "tcp_socket_endpoint",
    srcs = ["tcp_socket_endpoint.cc"],
    hdrs = ["tcp_socket_endpoint.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":endpoint"
    ],
)



cc_library(
    name = "udp_socket_endpoint",
    srcs = ["udp_socket_endpoint.cc"],
    hdrs = ["udp_socket_endpoint.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":address",
        ":endpoint"
    ],
)

cc_library(
    name = "utils",
    srcs = ["utils.cc"],
    hdrs = ["utils.h"],
    deps = [
        ":tcp_socket_endpoint",
        ":udp_socket_endpoint"
    ],
    visibility = ["//visibility:public"],
)
