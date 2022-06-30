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


cc_library(
    name = "grpc_proxy_class",
    srcs = ["grpc-proxy.cc"],
    hdrs = ["grpc-proxy.h"],
    deps = [
        "//proto:nezha_cc_proto",
        "//proto:kube_grpc_service_grpc",
        "//lib:utils",
        "//lib:address",
        "@com_github_grpc_grpc//:grpc++",
        "@com_github_grpc_grpc//:grpc++_reflection"
    ],
)

cc_binary(
    name = "nezha_proxy",
    srcs = ["proxy-run.cc"],
    deps = [
        ":proxy_class"
    ],
    copts = [
        "-I/usr/local/include"
    ],
    linkopts = [ "-L/usr/local/lib", "-lev", "-ldl", "-lturf", "-ljunction", "-lcrypto", "-lgflags",  "-lglog",  "-lyaml-cpp", "-pthread" ],

)


cc_binary(
    name = "nezha_grpc_proxy",
    srcs = ["grpc-proxy-run.cc"],
    deps = [
        ":grpc_proxy_class"
    ],
    copts = [
        "-I/usr/local/include",
    ],
    linkopts = [ "-L/usr/local/lib", "-L/usr/local/grpc/lib", 
                "-lev", "-ldl", "-lturf", "-ljunction", 
                 "-lcrypto", "-lgflags",  "-lglog",  "-lyaml-cpp", "-pthread" ],
)


