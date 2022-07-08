cc_library(
    name = "proxy_class",
    srcs = ["proxy.cc"],
    hdrs = ["proxy.h"],
    deps = [
        "//proto:nezha_cc_proto",
        "//lib:utils",
        "//lib:address",
    ],
)

cc_binary(
    name = "nezha_proxy",
    srcs = ["proxy_run.cc"],
    deps = [
        ":proxy_class"
    ],
    copts = [
        "-I/usr/local/include"
    ],
    linkopts = [ "-L/usr/local/lib", "-lev", "-ldl", "-lturf", "-ljunction", 
                "-lcrypto", "-lgflags",  "-lglog",  "-lyaml-cpp", "-pthread" ],

)


