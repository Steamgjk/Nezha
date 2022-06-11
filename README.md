# Nezhav2

Nezha: Deployable and High-Performance Consensus Using Synchronized Clocks [[preprint](https://arxiv.org/pdf/2206.03285.pdf)]

## Getting started

To make it easy for you to get started with GitLab, here's a list of recommended next steps.

Already a pro? Just edit this README.md and make it your own. Want to make it easy? [Use the template at the bottom](#editing-this-readme)!

## Clone Project


```
cd existing_repo
git remote add origin https://gitlab.com/steamgjk/nezhav2.git
git branch -M main
git push -uf origin main
```

## Install Dependencies

```
# Ubuntu 20.04.4 LTS
sudo apt update
sudo apt install -y net-tools autoconf libtool build-essential pkg-config cmake libssl-dev libboost-all-dev

# install protobuf. Please make install libprotoc 3.20.1-rc1 from source code instead of using apt install. Please follow the instructions at https://github.com/protocolbuffers/protobuf/blob/main/src/README.md 

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

# install glog (install from source, the apt install have some problems) https://github.com/google/glog
# install gflag 
sudo apt-get install libgflags-dev -y
```

## File Structure
The core part is the nezha folder, which includes three processes, i.e., replica, proxy and client. 

Each process is composed of three files: a class header (e.g., nezha-replica.h), a class source code (nezha-replica.cc), a launching file (e.g., nezha-replica-run.cc). Each process reads an independent yaml file (e.g., nezha-replica-config-0.yaml) to get its full configuration

Stale files and experiemental files are put into archive folder, and will be deleted finally.


## All-in-One-Box Test

```
# Launch one beefy machine (e.g.g 16 or 32 CPUs)
# Launch 3 replicas (All the configuration info is written in ONE yaml file)
GLOG_v=7 GLOG_logtostderr=1 nezhav2/.bin/nezha-replica --config nezhav2/configs/nezha-replica-config-0.yaml
GLOG_v=2 GLOG_logtostderr=1 nezhav2/.bin/nezha-replica --config nezhav2/configs/nezha-replica-config-1.yaml
GLOG_v=2 GLOG_logtostderr=1 nezhav2/.bin/nezha-replica --config nezhav2/configs/nezha-replica-config-2.yaml

# Launch 1 proxy
GLOG_logtostderr=1 nezhav2/.bin/nezha-proxy --config nezhav2/configs/nezha-proxy-config.yaml

# Lauch 1 client
GLOG_v=2 GLOG_logtostderr=1  nezhav2/.bin/nezha-client  --config nezhav2/configs/nezha-client-config.yaml


```


## Authors and acknowledgment
Show your appreciation to those who have contributed to the project.

## License
For open source projects, say how it is licensed.

## Project status
I have just completed some basic tests in distributed and one-box setting (3 replicas + 1 proxy + 1 open-loop client). The normal workflow, leader failure and election, replica rejoin work fine. Performance tests have not been conducted, because more bugs are expected to show up as the test cases become more complex. I will continue to 

(1) Conduct more tests to make the functionality robust, and then go on to measure the performance

(2) Clean the codebase and add more optimizations

