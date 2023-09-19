#!/bin/bash
export FLAGS_alsologtostderr=1

echo "Launching replica 0..."
(./bazel-bin/replica/nezha_replica --config ./configs/local/nezha-replica-config-0.yam & )

echo "Launching replica 1..."
(./bazel-bin/replica/nezha_replica --config ./configs/local/nezha-replica-config-1.yaml  &)

echo "Launching replica 2..."
(./bazel-bin/replica/nezha_replica --config ./configs/local/nezha-replica-config-2.yaml &)

echo "Launching proxy..."
(./bazel-bin/proxy/nezha_proxy --config ./configs/local/nezha-proxy-config.yaml &)

echo "Launching client..."
./bazel-bin/client/nezha_client --config ./configs/local/nezha-client-config.yaml

# Kill replicas
trap 'trap - SIGTERM && kill 0' SIGINT SIGTERM EXIT

# TODO(Katie): This is currently only checking if at least one request succeeded. 
# It does not check if the client/replica/proxy failed for some reason
file="Client-Stats-1"
if [ -e "$file" ]; then
    line_count=$(wc -l < "$file")
    if [ "$line_count" -le 1 ]; then
        echo "File '$file' exists but has only one line."
        echo "No successful requests."
        exit 1 
    else
        echo "File '$file' exists and has more than one line."
        exit 0
    fi
else
    echo "File '$file' does not exist."
    exit 1
fi