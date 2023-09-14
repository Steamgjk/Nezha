cc_library(
    name = "replica_class",
    srcs = ["replica.cc"],
    hdrs = ["replica.h"],
    deps = [
        "//proto:nezha_cc_proto",
        "//lib:utils",
        "@com_github_jbeder_yaml_cpp//:yaml-cpp",
        "@com_github_preshing_junction//:libjunction",
        "@com_github_enki_libev//:libev"
    ],

)


cc_binary(
    name = "nezha_replica",
    srcs = ["replica_run.cc"],
    deps = [
        ":replica_class",
    ],
    # copts = [
    #     "-I/usr/local/include"
    # ],
    # linkopts = [ "-L/usr/local/lib", "-ldl",# "-lturf", "-ljunction", "-lev", "-ldl",  "-lyaml-cpp",
    #             "-lcrypto", "-lgflags",  "-lglog", "-pthread" ],

)
