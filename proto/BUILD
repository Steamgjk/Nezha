load("@rules_proto//proto:defs.bzl", "proto_library")


proto_library(
    name = "nezha_proto",
    srcs = ["nezha_proto.proto"],
    visibility = ["//visibility:public"],
)

cc_proto_library(
    name = "nezha_cc_proto",
    deps = [":nezha_proto"],
    visibility = ["//visibility:public"],
)

