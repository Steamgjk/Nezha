cc_library(
    name = "proxy_config",
    hdrs = ["proxy_config.h"],
    deps = [
        "@com_github_jbeder_yaml_cpp//:yaml-cpp",
    ],
)

cc_library(
    name = "proxy_class",
    srcs = ["proxy.cc"],
    hdrs = ["proxy.h"],
    deps = [
        "//proto:nezha_cc_proto",
        "//lib:utils",
        "//lib:address",
        ":proxy_config",
    ],
)

cc_binary(
    name = "nezha_proxy",
    srcs = ["proxy_run.cc"],
    deps = [
        ":proxy_class",
    ],
)


