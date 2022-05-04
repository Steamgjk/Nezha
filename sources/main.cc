#include <stdio.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <ev.h>
#include <strings.h>
#include <chrono>
#include <unistd.h>
#include <arpa/inet.h>
#include <iostream>
#include "sources/main-proto.pb.h"
#define USE_UDP
#define BUFFER_SIZE 1024

void accept_cb(struct ev_loop* loop, struct ev_io* watcher, int revents);
void read_cb(struct ev_loop* loop, struct ev_io* watcher, int revents);
void timeout_cb(struct ev_loop* loop, struct ev_timer* w, int revents);
uint32_t messageCnt = 0;
uint64_t sta, ed;

// Get Current Microsecond Timestamp
inline uint64_t GetMicrosecondTimestamp(int errorVar = 0) {
	auto tse = std::chrono::system_clock::now().time_since_epoch();
	return std::chrono::duration_cast<std::chrono::microseconds>(tse).count() +
		errorVar;
}

int main(int argc, char* argv[])
{
	if (argc < 3) {
		perror("Para insufficient");
		return 0;
	}
	struct ev_loop* loop = ev_default_loop(0);
	int sd;
	struct sockaddr_in addr;
	// int addr_len = sizeof(addr);


#ifndef USE_UDP
	struct ev_io w_accept;
	// Create server socket
	if ((sd = socket(PF_INET, SOCK_STREAM, 0)) < 0)
	{
		perror("socket error");
		return -1;
	}
#else 
	struct ev_io w_client;
	if ((sd = socket(PF_INET, SOCK_DGRAM, 0)) < 0)
	{
		perror("socket error");
		return -1;
	}
#endif 
	bzero(&addr, sizeof(addr));
	addr.sin_family = AF_INET;
	addr.sin_port = htons(std::atoi(argv[2]));
	addr.sin_addr.s_addr = inet_addr(argv[1]);

	// Bind socket to address
	if (bind(sd, (struct sockaddr*)&addr, sizeof(addr)) != 0)
	{
		perror("bind error");
	}


	ev_timer timer;
	timer.repeat = 1;
	timer.data = new int(0);
	ev_init(&timer, timeout_cb);

	ev_timer_again(loop, &timer);
#ifndef USE_UDP
	// Start listing on the socket
	if (listen(sd, 2) < 0)
	{
		perror("listen error");
		return -1;
	}

	// Initialize and start a watcher to accepts client requests
	ev_io_init(&w_accept, accept_cb, sd, EV_READ);
	ev_io_start(loop, &w_accept);
#else 
	w_client.data = &timer;
	ev_io_init(&w_client, read_cb, sd, EV_READ);
	ev_io_start(loop, &w_client);
#endif

	// Start infinite loop
	ev_run(loop, 0);

	return 0;
}

void timeout_cb(struct ev_loop* loop, struct ev_timer* w, int revents) {

	int* cnt = (int*)(w->data);
	std::cout << "Print " << *cnt << std::endl;
	(*cnt)++;
	// if ((*cnt) == 10) {
	// 	ev_timer_stop(loop, w);
	// }
}

void accept_cb(struct ev_loop* loop, struct ev_io* watcher, int revents)
{
	struct sockaddr_in client_addr;
	socklen_t client_len = sizeof(client_addr);
	int client_sd;
	struct ev_io* w_client = (struct ev_io*)malloc(sizeof(struct ev_io));

	if (EV_ERROR & revents)
	{
		perror("got invalid event");
		return;
	}

	// Accept client request
	client_sd = accept(watcher->fd, (struct sockaddr*)&client_addr, &client_len);

	if (client_sd < 0)
	{
		perror("accept error");
		return;
	}

	// Initialize and start watcher to read client requests
	ev_io_init(w_client, read_cb, client_sd, EV_READ);
	ev_io_start(loop, w_client);
}

/* Read client message */
void read_cb(struct ev_loop* loop, struct ev_io* watcher, int revents) {
	char buffer[BUFFER_SIZE];
	int read;

	if (EV_ERROR & revents)
	{
		perror("got invalid event");
		return;
	}

#ifndef USE_UDP
	// Receive message from client socket
	read = recv(watcher->fd, buffer, BUFFER_SIZE, 0);
#else 
	struct sockaddr_in addr;
	socklen_t addrlen;
	read = recvfrom(watcher->fd, buffer, BUFFER_SIZE, 0,
		(struct sockaddr*)&addr, &addrlen);
#endif 
	if (read < 0)
	{
		perror("read error");
		return;
	}

	if (read == 0)
	{
		// Stop and free watchet if client socket is closing
		ev_io_stop(loop, watcher);
		free(watcher);
		perror("peer might closing");
		return;
	}
	else
	{
		messageCnt++;
		if (messageCnt == 1) {
			sta = GetMicrosecondTimestamp();
		}
		printf("messageCnt=%u readLen=%d\n", messageCnt, read);
		if (messageCnt % 100000 == 0) {
			ed = GetMicrosecondTimestamp();

			printf("messageCnt=%u readLen=%d tp=%f\n",
				messageCnt, read, 100000 / ((ed - sta) * 1e-6));
			sta = ed;
		}
		// printf("message:%s", buffer);
		if (messageCnt == 3) {
			std::cout << "stop timer " << std::endl;
			ev_timer_stop(loop, (struct ev_timer*)(watcher->data));
		}
	}

	// Send message bach to the client
	/* send(watcher->fd, buffer, read, 0); */
	// bzero(buffer, read);
}
