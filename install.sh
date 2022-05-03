sudo apt update
sudo apt install -y net-tools autoconf libtool build-essential protobuf-compiler pkg-config
git clone https://github.com/enki/libev.git
chmod -R 777 libev
cd libev && sudo ./autogen.sh 
./configure && make && sudo make install