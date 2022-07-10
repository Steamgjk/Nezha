# Nezhav2

Nezha: Deployable and High-Performance Consensus Using Synchronized Clocks [[preprint](https://arxiv.org/pdf/2206.03285.pdf)]



## Install Dependencies

```
# Ubuntu 20.04.4 LTS
sudo apt update
sudo apt install -y net-tools autoconf libtool build-essential pkg-config cmake libssl-dev libboost-all-dev

# install protobuf (3.20.1-rc1). Please follow the instructions at https://github.com/protocolbuffers/protobuf/blob/main/src/README.md 
# Install protobuf from source code (because apt install gives an old version)

# install yamlcpp. Please follow the instructions at https://github.com/jbeder/yaml-cpp

# install libev
git clone https://github.com/enki/libev.git
chmod -R 777 libev
cd libev && sudo ./autogen.sh 
./configure && make && sudo make install

# install junction and the turf
git clone https://github.com/preshing/junction.git
git clone https://github.com/preshing/turf.git
cd junction
mkdir build
cd build
cmake ..
make
# it will install turf and junction together in /usr/local/lib
sudo make install

# install glog (I recommend install from source, the apt install have some problems) https://github.com/google/glog
# install gflag 
sudo apt-get install libgflags-dev -y
```

## Clone Project

```
git clone  https://gitlab.com/steamgjk/nezhav2.git
```


## File Structure
The core part includes three modules (folders), i.e., 
- replica
- proxy
- client 

Each module is composed of three files: 
- a header file (e.g., nezha-replica.h), 
- a source implementation file (nezha-replica.cc), 
- a launching file (e.g., nezha-replica-run.cc). 

Each process reads an independent yaml file (e.g., nezha-replica-config-0.yaml) to get its full configuration, the sample configuration files are placed in the configs folder

Stale files and experiemental files are put into archive folder, and will be deleted finally.


## Build Nezha with Bazel

Since Bazel is becoming popular, we have migrated nezha from Makefile-based building system to the bazel building system. The bazel version in use is 5.2.0

```
    cd nezhav2 && bazel build //...
```


After building the project successfully, the executable files will be generated in the folder named bazel-bin



## All-in-One-Box Test

We briefly describe the commands to launch the all-in-one-box test.

The readers can also refer to [more detailed instructions](demo.md) to run the one-box demo.

```
# Launch one beefy machine (e.g., with 16 or 32 CPUs)
# Launch 3 replicas (All the configuration info is written in ONE yaml file)
GLOG_v=2 nezhav2/bazel-bin/replica/nezha_replica --config nezhav2/configs/nezha-replica-config-0.yaml
GLOG_v=2 nezhav2/bazel-bin/replica/nezha_replica --config nezhav2/configs/nezha-replica-config-1.yaml
GLOG_v=2 nezhav2/bazel-bin/replica/nezha_replica --config nezhav2/configs/nezha-replica-config-2.yaml

# GLOG_v is the flag provided by glog, which allows users to specify the verbose level of logs. It can be completely removed if the user does not want to see too many logs

# Launch 1 proxy
GLOG_v=2 nezhav2/bazel-bin/proxy/nezha_proxy --config nezhav2/configs/nezha-proxy-config.yaml

# Lauch 1 client
GLOG_v=2 nezhav2/bazel-bin/client/nezha_client  --config nezhav2/configs/nezha-client-config.yaml


# Kill 1 replica (the leader, Replica-0), Crtl+C 

# We can see the remaining 2 replicas do the view change.
# The 2 replicas enter a new view (viewId=1), with Replica-1 as the leader

# Relaunch Replica-0 to rejoin as one follower
GLOG_v=2 nezhav2/bazel-bin/replica/nezha_replica --config nezhav2/configs/nezha-replica-config-0.yaml --isRecovering true

# Add the flag --isRecovering true to indicate it is recovering

```




## Important Configuration Parameters
### Replica
- replica-ips must include 2f+1 ips
- replica-id starts from 0 to 2f
- index-transfer-batch, request-key-transfer-batch, request-transfer-batch. The values of the three <em>batch parameters</em> should be carefully chosen in order not to overflow the [maximum size of UDP packets](https://stackoverflow.com/questions/1098897/what-is-the-largest-safe-udp-packet-size-on-the-internet). 

### Client
- is-openloop: We support two types of clients, i.e., open-loop clients and closed-loop clients. When this flag is true, --poission-rate becomes meaningful.
- skew-factor and key-number decides the workload, which further affects the commutativity optimization

### Proxy
- shard-num decides how many threads will be launched. 1 shard includes 1 forwarding thread to forward client requests to replicas and 1 replying thread to receive and replies from replicas and does quorum check
- max-owd  is used in the clamping function to estimate one-way delay, more details are described in Sec 4 [Adpative latency bound] of the paper.

## Performance Benchmark
To be continued


## Authors and Acknowledgment
Show your appreciation to those who have contributed to the project.

## License
For open source projects, say how it is licensed.

## Project Status
I have just completed some basic tests in distributed and one-box setting (3 replicas + 1 proxy + 1 client). The normal workflow, leader failure and election, replica rejoin work fine. Performance tests have not been conducted, because more bugs are expected to show up as the test cases become more complex. I will continue to 

(1) Conduct more tests to make the functionality robust, and then go on to measure the performance

(2) Clean the codebase and add more optimizations

