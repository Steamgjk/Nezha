load("@rules_proto//proto:defs.bzl", "proto_library")


cc_library(
    name = "client_class",
    srcs = ["client.cc"],
    hdrs = ["client.h"],
    deps = [
        "//proto:nezha_cc_proto",
        "//lib:zipfian",
        "//lib:utils"
    ],
)


cc_binary(
    name = "nezha_client",
    srcs = ["client_run.cc"],
    deps = [
        ":client_class"
    ],
    copts = [
        "-I/usr/local/include"
    ],
    linkopts = [ "-L/usr/local/lib", "-lev", "-ldl", "-lturf", "-ljunction", 
                "-lcrypto", "-lgflags",  "-lglog",  "-lyaml-cpp", "-pthread" ],

)


