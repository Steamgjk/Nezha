#include <stdio.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <event2/bufferevent.h>
#include <event2/buffer.h>
#include <event2/listener.h>
#include <event2/util.h>
#include <event2/event.h>
#include <strings.h>
#include <chrono>
#include <unistd.h>
#include <arpa/inet.h>


#define USE_UDP
#define BUFFER_SIZE 1024


void DoRead(evutil_socket_t fd, short events, void* arg);
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

	int sd;
	struct sockaddr_in addr;
	int addr_len = sizeof(addr);

	if ((sd = socket(PF_INET, SOCK_DGRAM, 0)) < 0)
	{
		perror("socket error");
		return -1;
	}

	bzero(&addr, sizeof(addr));
	addr.sin_family = AF_INET;
	addr.sin_port = htons(std::atoi(argv[2]));
	addr.sin_addr.s_addr = inet_addr(argv[1]);

	// Bind socket to address
	if (bind(sd, (struct sockaddr*)&addr, sizeof(addr)) != 0)
	{
		perror("bind error");
	}

	struct event_base* base = event_base_new();
	struct event* signal_event = event_new(base, sd, EV_READ | EV_PERSIST, DoRead, NULL);
	event_add(signal_event, NULL);
	event_base_dispatch(base);

	return 0;
}


/* Read client message */
void DoRead(evutil_socket_t fd, short events, void* arg) {
	char buffer[BUFFER_SIZE];
	int read;
	struct sockaddr_in addr;
	socklen_t addrlen;
	read = recvfrom(fd, buffer, BUFFER_SIZE, 0,
		(struct sockaddr*)&addr, &addrlen);

	if (read < 0)
	{
		perror("read error");
		return;
	}


	{
		messageCnt++;
		if (messageCnt == 1) {
			sta = GetMicrosecondTimestamp();
		}
		// printf("messageCnt=%u readLen=%d\n", messageCnt, read);
		if (messageCnt % 100000 == 0) {
			ed = GetMicrosecondTimestamp();

			printf("messageCnt=%u readLen=%d tp=%f\n",
				messageCnt, read, 100000 / ((ed - sta) * 1e-6));
			sta = ed;
		}
		// printf("message:%s", buffer);
	}

	// Send message bach to the client
	/* send(watcher->fd, buffer, read, 0); */
	// bzero(buffer, read);
}
