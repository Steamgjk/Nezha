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


### ETCD Adaptor ###


proto_library(
    name = "etcd_authpb_auth_proto",
    srcs = ["etcd-authpb-auth.proto"],
    deps = ["@gogoprotobuf//:gogo_proto"],
    visibility = ["//visibility:public"],
)

cc_proto_library(
    name = "etcd_authpb_auth_cc",
    deps = [":etcd_authpb_auth_proto"],
    visibility = ["//visibility:public"],
)

proto_library(
    name = "etcd_mvccpb_kv_proto",
    srcs = ["etcd-mvccpb-kv.proto"],
    deps = ["@gogoprotobuf//:gogo_proto"],
    visibility = ["//visibility:public"],
)

cc_proto_library(
    name = "etcd_mvccpb_kv_cc",
    deps = [":etcd_mvccpb_kv_proto"],
    visibility = ["//visibility:public"],
)


proto_library(
    name = "etcd_rpc_proto",
    srcs = ["etcd-rpc.proto"],
    deps = [":etcd_authpb_auth_proto",
            ":etcd_mvccpb_kv_proto",
            "@gogoprotobuf//:gogo_proto",
            "@googleapi//:annotations_proto"],
    visibility = ["//visibility:public"],
)

cc_proto_library(
    name = "etcd_rpc_cc",
    deps = [":etcd_rpc_proto"],
    visibility = ["//visibility:public"],
)


cc_grpc_library(
    name = "etcd_rpc_grpc",
    srcs = [":etcd_rpc_proto"],
    grpc_only = True,
    deps = [":etcd_rpc_cc"],
    visibility = ["//visibility:public"],
)




proto_library(
    name = "kube_grpc_service_proto",
    srcs = ["kube-grpc-service.proto"],
    # deps= [ ":thing_proto"],
    # deps= [ ":google_api_annotations_proto"],
    deps = ["@googleapi//:annotations_proto"],
    # deps = ["@com_github_gogo_googleapis//google/api:annotations_proto"],
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
