load("@rules_proto//proto:defs.bzl", "proto_library")

cc_library(
    name = "zipfian",
    srcs = ["zipfian.h"],
    hdrs = ["zipfian.h"],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "utils",
    srcs = ["utils.cc"],
    hdrs = ["utils.h"],
    visibility = ["//visibility:public"],
)


cc_library(
    name = "address",
    srcs = ["address.cc"],
    hdrs = ["address.h"],
    visibility = ["//visibility:public"],
)



cc_library(
    name = "udp_socket_endpoint",
    srcs = ["udp_socket_endpoint.cc"],
    hdrs = ["udp_socket_endpoint.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":address",
        ":utils"
    ],
)
