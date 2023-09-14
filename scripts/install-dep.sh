#!/bin/bash

# Ubuntu 20.04.4 LTS 
# Install Essentials
sudo apt update
sudo apt install -y net-tools autoconf libtool build-essential pkg-config cmake libboost-all-dev # libssl-dev
# sudo apt install -y libgflags-dev  libgoogle-glog-dev  
# We are using libprotoc 3.6.1, newer version should also work
sudo apt install -y protobuf-compiler
protoc --version


# Install yaml-cpp
# git clone https://github.com/jbeder/yaml-cpp.git
# cd yaml-cpp && mkdir build && cd build 
# cmake ..
# make && sudo make install
# cd $HOME

# # Install libev
# git clone https://github.com/enki/libev.git
# chmod -R 777 libev
# cd libev && sudo ./autogen.sh 
# ./configure && make && sudo make install
# cd $HOME


# Install concurrent queue
## It is a single-file library, we only need concurrentqueue.h
# git clone https://github.com/cameron314/concurrentqueue.git
# sudo cp concurrentqueue/concurrentqueue.h /usr/local/include/
# cd $HOME


# Install junction and turf
# git clone https://github.com/preshing/junction.git
# git clone https://github.com/preshing/turf.git
# cd junction
# mkdir build
# cd build
# cmake ..
# make
# # it will install turf and junction together in /usr/local/lib
# sudo make install
# cd $HOME


# Install bazel 5.2.0
# Please follow the instructions at https://bazel.build/install/ubuntu#install-on-ubuntu, 
# or simply run the following commands

sudo apt install -y apt-transport-https curl gnupg
curl -fsSL https://bazel.build/bazel-release.pub.gpg | gpg --dearmor >bazel-archive-keyring.gpg
sudo mv bazel-archive-keyring.gpg /usr/share/keyrings
echo "deb [arch=amd64 signed-by=/usr/share/keyrings/bazel-archive-keyring.gpg] https://storage.googleapis.com/bazel-apt stable jdk1.8" | sudo tee /etc/apt/sources.list.d/bazel.list
sudo apt update
sudo apt install -y bazel-5.2.0
sudo mv /usr/bin/bazel-5.2.0 /usr/bin/bazel
bazel --version
cd $HOME


# Refresh Dependency Links
# After all libs above have been installed, run the following command to create the necessary links
sudo ldconfig 
