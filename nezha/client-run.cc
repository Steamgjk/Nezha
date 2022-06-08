#include "nezha/client.h"
int main(int argc, char* argv[]) {
    nezha::Client* client = new nezha::Client();
    delete client;
}