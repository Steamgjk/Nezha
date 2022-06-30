cc_library(
    name = "replica_class",
    srcs = ["replica.cc"],
    hdrs = ["replica.h"],
    deps = [
        "//proto:nezha_cc_proto",
        "//lib:utils",
        "//lib:udp_socket_endpoint"
    ],

)



cc_binary(
    name = "nezha_replica",
    srcs = ["replica-run.cc"],
    deps = [
        ":replica_class"
    ],
    copts = [
        "-I/usr/local/include"
    ],
    linkopts = [ "-L/usr/local/lib", "-lev", "-ldl", "-lturf", "-ljunction", "-lcrypto", "-lgflags",  "-lglog",  "-lyaml-cpp", "-pthread" ],

)
