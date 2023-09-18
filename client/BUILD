load("@rules_proto//proto:defs.bzl", "proto_library")

cc_library(
    name = "client_config",
    hdrs = ["client_config.h"],
    deps = [
        "@com_github_jbeder_yaml_cpp//:yaml-cpp",
    ],
)

cc_library(
    name = "client_class",
    srcs = ["client.cc"],
    hdrs = ["client.h"],
    deps = [
        "//proto:nezha_cc_proto",
        "//lib:zipfian",
        "//lib:utils",
        ":client_config",
    ],
)


cc_binary(
    name = "nezha_client",
    srcs = ["client_run.cc"],
    deps = [
        ":client_class",
    ],
)
