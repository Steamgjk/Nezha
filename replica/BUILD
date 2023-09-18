cc_library(
    name = "replica_config",
    hdrs = ["replica_config.h"],
    deps = [
        "@com_github_jbeder_yaml_cpp//:yaml-cpp",
    ],
)


cc_library(
    name = "replica_class",
    srcs = ["replica.cc"],
    hdrs = [
        "replica.h",
    ],
    deps = [
        ":replica_config",
        "//proto:nezha_cc_proto",
        "//lib:utils",
        "@com_github_preshing_junction//:libjunction",
        "@com_github_enki_libev//:libev",
        "@boost//:uuid",
    ],
)


cc_binary(
    name = "nezha_replica",
    srcs = ["replica_run.cc"],
    deps = [
        ":replica_class",
    ],
)
