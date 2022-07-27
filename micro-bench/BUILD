cc_binary(
    name = "bench_sender",
    srcs = ["bench_sender.cc"],
    deps = [
        "//proto:nezha_cc_proto",
        "//lib:utils",
        "//lib:address",
        "//lib:zipfian",
    ],
    copts = [
        "-I/usr/local/include"
    ],
    linkopts = [ "-L/usr/local/lib", "-lev", "-ldl", "-lturf", "-ljunction", 
                "-lcrypto", "-lgflags",  "-lglog",  "-lyaml-cpp", "-pthread" ],

)


cc_binary(
    name = "bench_receiver",
    srcs = ["bench_receiver.cc"],
    deps = [
        "//proto:nezha_cc_proto",
        "//lib:utils",
        "//lib:address",
        "//lib:zipfian",
    ],
    copts = [
        "-I/usr/local/include"
    ],
    linkopts = [ "-L/usr/local/lib", "-lev", "-ldl", "-lturf", "-ljunction", 
                "-lcrypto", "-lgflags",  "-lglog",  "-lyaml-cpp", "-pthread" ],

)




cc_binary(
    name = "analysis",
    srcs = ["analysis.cc"],
    deps = [
        "//proto:nezha_cc_proto",
        "//lib:utils",
        "//lib:address",
        "//lib:zipfian",
    ],
    copts = [
        "-I/usr/local/include"
    ],
    linkopts = [ "-L/usr/local/lib", "-lev", "-ldl", "-lturf", "-ljunction", 
                "-lcrypto", "-lgflags",  "-lglog",  "-lyaml-cpp", "-pthread" ],

)

