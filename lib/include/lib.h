
#pragma once

#include <cstdint>
#include "utils.h"
#include <arpa/inet.h>
#include <string.h>
#include <stdlib.h>
#include <iostream>
#include <map>
#include "protocol.h"

/* Maximum segment size, change as you see fit */
#define MAX_DATA_SIZE 512
#define MAX_SEGMENT_SIZE (MAX_DATA_SIZE + sizeof(poli_tcp_data_hdr))
#define CHECKSUM_SIZE 4
#define MAX_PAYLOAD_SIZE (MAX_DATA_SIZE - CHECKSUM_SIZE)

#define MAX_CONNECTIONS 32
#define ACK 1
#define SYN 2
#define SYN_ACK 3


struct window_segment {
    char data[MAX_SEGMENT_SIZE];
    int len;
    bool acked;
    uint64_t last_sent_ms;
};


/* Protocol control block. Used track different parameters about a connection. 
 * Will need to be extenden to solve the homework with other parameters such as
 * last_ack or status depending on how you implement your protocol. */
struct connection {
    /* common window for both the sender and receiver. */
    /* list window: A window representation */
    int sockfd; /* socket used for this connection */
    int conn_id; /* connection identifier */
    struct sockaddr_in servaddr; /* used to identify the destination */
    pthread_mutex_t con_lock; /* Used for syncronization with the handler thread and read/send calls.*/
    int timerfd; /* timer fd associated with this connection */

    /* TODO. Parameters used only by the sender */
    int max_window_seq; /* Used to store the max number of packets that can be inflight, since we can
                           have many more packets in our window */
    uint32_t send_base;
    uint32_t next_seq_num;
    std::map<uint32_t, struct window_segment> send_window;

    /* TODO. Parameters used only by the client */
    uint32_t recv_base;
    std::map<uint32_t, struct window_segment> recv_window;
    
    /*uint8_t type; //Ne definim 1 = SYN, 2 = ACK 3 = SYN + ACK
    uint32_t len;
    uint16_t seq;
    char buffer[MAX_DATA_SIZE]; */

};

/* ########## API that we expose to the application ########### */

/* Equivalent of listen. Ran by the server to waits for a connection from a
 * client. Returns a connection id. Blocking untill it receives a connection
 * request */
int wait4connect(uint32_t ip, uint16_t port);
/* Equivalent of connect. Used by the client to connect to a server. */
int setup_connection(uint32_t ip, uint16_t port);
/* Equivalent to recv. Blocking if there is no data to be written in buffer */
int recv_data(int connectionid, char *buffer, int len);
/* Equivalent to send. Used by the client to send a stream of bytes as segments */
int send_data(int conn_id, char *buffer, int len);
/* Used to initialize your protocol on the receiver side. */
void init_receiver(int recv_buffer_bytes);
/* Used to initialize your protocol on the sender side */
void init_sender(int speed, int delay);

/* ######### Internal API used by sender and receiver ########### */
int recv_message_or_timeout(char *buff, size_t len, int *conn_id);
