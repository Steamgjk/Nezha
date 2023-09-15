load("@rules_proto//proto:defs.bzl", "proto_library")

cc_library(
    name = "zipfian",
    srcs = ["zipfian.h"],
    hdrs = ["zipfian.h"],
    visibility = ["//visibility:public"],
)


cc_library(
    name = "common_type",
    srcs = ["common_type.h"],
    hdrs = ["common_type.h"],
    visibility = ["//visibility:public"],
)


cc_library(
    name = "common_struct",
    srcs = ["common_struct.h"],
    hdrs = ["common_struct.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":common_type",
    ],
)


cc_library(
    name = "address",
    srcs = ["address.cc"],
    hdrs = ["address.h"],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "message_handler",
    srcs = ["message_handler.h"],
    hdrs = ["message_handler.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":address",
        ":common_type",
    ],
)

cc_library(
    name = "timer",
    srcs = ["timer.h"],
    hdrs = ["timer.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":address",
        ":common_type",
    ],
)

cc_library(
    name = "endpoint",
    srcs = ["endpoint.cc"],
    hdrs = ["endpoint.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":address",
        ":common_struct",
        ":message_handler",
        ":timer",
        "@com_github_enki_libev//:libev",
        "@com_github_google_glog//:glog",
        "@com_google_protobuf//:protobuf",
        "@openssl//:openssl",
    ],
)




cc_library(
    name = "udp_socket_endpoint",
    srcs = ["udp_socket_endpoint.cc"],
    hdrs = ["udp_socket_endpoint.h"],
    visibility = ["//visibility:public"],
    deps = [
        ":address",
        ":endpoint",
        "@com_github_enki_libev//:libev",
        "@com_google_protobuf//:protobuf",
        "@openssl//:openssl",
    ],
)


cc_library(
    name = "utils",
    srcs = ["utils.cc"],
    hdrs = ["utils.h"],
    deps = [
        ":udp_socket_endpoint",
        "@com_github_cameron314_concurrentqueue//:concurrentqueue",
        "@com_github_preshing_junction//:libjunction",
        "@com_github_gflags_gflags//:gflags",
        "@com_github_google_glog//:glog",
        "@openssl//:openssl",
    ],
    visibility = ["//visibility:public"],
)

