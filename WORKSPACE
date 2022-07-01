load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

http_archive(
    name = "rules_proto",
    sha256 = "e017528fd1c91c5a33f15493e3a398181a9e821a804eb7ff5acdd1d2d6c2b18d",
    strip_prefix = "rules_proto-4.0.0-3.20.0",
    urls = [
        "https://github.com/bazelbuild/rules_proto/archive/refs/tags/4.0.0-3.20.0.tar.gz",
    ],
)
load("@rules_proto//proto:repositories.bzl", "rules_proto_dependencies", "rules_proto_toolchains")
rules_proto_dependencies()
rules_proto_toolchains()


http_archive(
    name = "com_github_grpc_grpc",
    sha256 = "9f387689b7fdf6c003fd90ef55853107f89a2121792146770df5486f0199f400",
    urls = [
        "https://github.com/grpc/grpc/archive/refs/tags/v1.42.0.zip",
    ],
    strip_prefix = "grpc-1.42.0",
)
load("@com_github_grpc_grpc//bazel:grpc_deps.bzl", "grpc_deps")
grpc_deps()
load("@com_github_grpc_grpc//bazel:grpc_extra_deps.bzl", "grpc_extra_deps")
grpc_extra_deps()


http_archive(
    name = "googleapi",
    sha256 = "3ff2365822fb573cb1779ada5c2ac7899269cacd0836aef95ffe9d95779031f2",
    url = "https://github.com/googleapis/googleapis/archive/refs/tags/common-protos-1_3_1.zip", 
    strip_prefix = "googleapis-common-protos-1_3_1/",
    build_file="@//external:googleapi.BUILD"
)


http_archive(
    name = "etcd",
    sha256 = "580ce584dc7628efebb57f8c8240674918d334ad21e33186bbc5f6348f465bc1",
    url = "https://github.com/etcd-io/etcd/archive/refs/tags/v3.5.0.zip", 
    strip_prefix = "etcd-3.5.0/",
    build_file="@//external:etcd.BUILD"
)



http_archive(
    name = "gogoprotobuf",
    sha256 = "f89f8241af909ce3226562d135c25b28e656ae173337b3e58ede917aa26e1e3c",
    url = "https://github.com/gogo/protobuf/archive/refs/tags/v1.3.2.zip", 
    strip_prefix = "protobuf-1.3.2/",
    build_file="@//external:gogoprotobuf.BUILD"
)