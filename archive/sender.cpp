#include <stdio.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <ev.h>
#include <strings.h>
#include <chrono>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>

#define USE_UDP
int BUFFER_SIZE = 128;


// Get Current Microsecond Timestamp
inline uint64_t GetMicrosecondTimestamp(int errorVar = 0) {
	auto tse = std::chrono::system_clock::now().time_since_epoch();
	return std::chrono::duration_cast<std::chrono::microseconds>(tse).count() +
		errorVar;
}

int main(int argc, char* argv[])
{
	if (argc < 4) {
		perror("Para insufficient");
		return 0;
	}
	BUFFER_SIZE = std::atoi(argv[3]);
	printf("begin...\n");
	int sockfd = 0;
	// Creating socket file descriptor
	if ((sockfd = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
		perror("socket creation failed");
		exit(EXIT_FAILURE);
	}
	struct sockaddr_in     servaddr;
	memset(&servaddr, 0, sizeof(servaddr));

	// Filling server information
	servaddr.sin_family = AF_INET;
	servaddr.sin_port = htons(std::atoi(argv[2]));
	servaddr.sin_addr.s_addr = inet_addr(argv[1]);

	int n, len;
	char buffer[BUFFER_SIZE];
	for (int i = 0; i < BUFFER_SIZE; i++) {
		buffer[i] = 'a';
	}
	printf("sending...\n");
	uint64_t cnt = 0;
	while (cnt < 10000000ul) {
		sendto(sockfd, buffer, BUFFER_SIZE, 0,
			(const struct sockaddr*)&servaddr,
			sizeof(servaddr));
		cnt++;
	}
	return 0;
}

