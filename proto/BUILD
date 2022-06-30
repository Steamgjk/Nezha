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