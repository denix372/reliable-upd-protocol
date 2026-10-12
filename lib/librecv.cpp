#include <pthread.h>
#include <cstdlib>
#include <map>
#include <cstdint>
#include "lib.h"
#include "utils.h"
#include "protocol.h"
#include <poll.h>
#include <cassert>
#include <sys/timerfd.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <sys/time.h>
using namespace std;

std::map<int, struct connection *> cons;
pthread_mutex_t cons_lock = PTHREAD_MUTEX_INITIALIZER;

struct pollfd data_fds[MAX_CONNECTIONS];
/* Used for timers per connection */
struct pollfd timer_fds[MAX_CONNECTIONS];
int fdmax = 0;
int global_conn_id = 0;
static int listen_sockfd = -1;
#define HS_TIMEOUT_US 200000

static uint32_t checksum32(const uint8_t *data, size_t len)
{
    uint32_t sum = 0;
    if (data == NULL || len == 0)
        return 0;
    for (size_t i = 0; i < len; ++i)
        sum += data[i];
    uint32_t tmp = sum;
    tmp = (tmp & 0xffff) + (tmp >> 16);
    tmp = (tmp & 0xffff) + (tmp >> 16);
    sum = tmp;
    if (sum == 0)
        sum = 0;
    return sum;
}

static void set_timeout(int sockfd, int usec)
{
    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = usec;
    if (usec < 0)
        tv.tv_usec = 0;
    if (tv.tv_usec > 1000000) {
        tv.tv_sec = tv.tv_usec / 1000000;
        tv.tv_usec = tv.tv_usec % 1000000;
    }
    int rc = setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    if (rc < 0)
        return;
}

static bool valid_data(const char *segment, int n, struct poli_tcp_data_hdr **hdr_out)
{
    if (n <= (int)sizeof(struct poli_tcp_ctrl_hdr))
        return false;

    struct poli_tcp_data_hdr *hdr = (struct poli_tcp_data_hdr *)segment;
    if (hdr->len > MAX_PAYLOAD_SIZE)
        return false;
    if (n < (int)(sizeof(struct poli_tcp_data_hdr) + hdr->len + CHECKSUM_SIZE))
        return false;

    uint32_t recv_csum = 0;
    memcpy(&recv_csum, segment + sizeof(struct poli_tcp_data_hdr) + hdr->len, CHECKSUM_SIZE);
    uint32_t calc_csum = checksum32((const uint8_t *)segment,
                                    sizeof(struct poli_tcp_data_hdr) + hdr->len);
    if (recv_csum != calc_csum)
        return false;

    *hdr_out = hdr;
    return true;
}

static void process_segment(struct connection *con, int conn_id, const char *segment,
                            int n, const struct poli_tcp_data_hdr *hdr)
{
    uint32_t seq = hdr->seq_num;
    if (seq >= con->recv_base + con->max_window_seq)
        return;

    if (seq >= con->recv_base) {
        if (con->recv_window.find(seq) == con->recv_window.end()) {
            struct window_segment new_seg;
            new_seg.len = hdr->len;
            memcpy(new_seg.data, segment, n);
            con->recv_window[seq] = new_seg;
        }
    }

    struct poli_tcp_ctrl_hdr ack_hdr;
    memset(&ack_hdr, 0, sizeof(ack_hdr));
    ack_hdr.protocol_id = POLI_PROTOCOL_ID;
    ack_hdr.conn_id = conn_id;
    ack_hdr.type = ACK;
    ack_hdr.ack_num = seq;
    sendto(con->sockfd, &ack_hdr, sizeof(ack_hdr), 0,
           (struct sockaddr *)&con->servaddr, sizeof(con->servaddr));
}

int recv_data(int conn_id, char *buffer, int len)
{
    pthread_mutex_lock(&cons_lock);
    auto it = cons.find(conn_id);
    struct connection *con = (it == cons.end()) ? NULL : it->second;
    pthread_mutex_unlock(&cons_lock);
    if (!con)
        return -1;

    pthread_mutex_lock(&con->con_lock);

    if (con->recv_window.find(con->recv_base) != con->recv_window.end()) {
        struct window_segment seg = con->recv_window[con->recv_base];
        struct poli_tcp_data_hdr *hdr = (struct poli_tcp_data_hdr *)seg.data;

        int to_copy = min((int)hdr->len, len);
        if (to_copy > 0)
            memcpy(buffer, seg.data + sizeof(struct poli_tcp_data_hdr), to_copy);

        con->recv_window.erase(con->recv_base);
        con->recv_base++;

        pthread_mutex_unlock(&con->con_lock);

        if (to_copy == 0)
            return 0;
        return to_copy;
    }

    pthread_mutex_unlock(&con->con_lock);
    usleep(1000);
    return 0;
}

void *receiver_handler(void *arg)
{

    char segment[MAX_SEGMENT_SIZE];
    int res;
    DEBUG_PRINT("Starting recviver handler\n");

    while (1) {

        int conn_id = -1;
        do {
            res = recv_message_or_timeout(segment, MAX_SEGMENT_SIZE, &conn_id);
        } while(res == -14);

        struct poli_tcp_data_hdr *hdr = NULL;
        if (!valid_data(segment, res, &hdr))
            continue;

        int data_conn_id = hdr->conn_id;
        pthread_mutex_lock(&cons_lock);
        auto it = cons.find(data_conn_id);
        struct connection *con = (it == cons.end()) ? NULL : it->second;
        pthread_mutex_unlock(&cons_lock);
        if (!con)
            continue;
        pthread_mutex_lock(&con->con_lock);
        process_segment(con, data_conn_id, segment, res, hdr);
        pthread_mutex_unlock(&con->con_lock);
    }
}

int wait4connect(uint32_t ip, uint16_t port)
{
    /* TODO: Implement the Three Way Handshake on the receiver part. This blocks
     * until a connection is established. */

    struct connection *con = new connection();
    int conn_id = fdmax;
    con->max_window_seq = 100;
    con->recv_base = 0;
    pthread_mutex_init(&con->con_lock, NULL);

    /* This can be used to set a timer on a socket, useful once we received a
     * SYN. You may want to disable by setting the time to 0 (tv_sec = 0,
     * tv_usec = 0)
    struct timeval tv;
    tv.tv_sec = 2;
    tv.tv_usec = 100000;
    if (setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO,&tv,sizeof(tv)) < 0) {
        perror("Error");
    } */

    /* Receive SYN on the listening socket. Create a new data socket and send
     * the data port number via SYN-ACK to the client. */
    if (listen_sockfd == -1) {
        listen_sockfd = socket(AF_INET, SOCK_DGRAM, 0);
        int optval = 1;
        setsockopt(listen_sockfd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval));

        struct sockaddr_in listen_addr;
        memset(&listen_addr, 0, sizeof(listen_addr));
        listen_addr.sin_family = AF_INET;
        listen_addr.sin_addr.s_addr = ip;
        listen_addr.sin_port = port;

        bind(listen_sockfd, (const struct sockaddr *)&listen_addr, sizeof(listen_addr));
    }

    //1. PRIMESC SYN
    struct poli_tcp_ctrl_hdr hdr;
    memset(&hdr, 0, sizeof(hdr));
    socklen_t slen = sizeof(con->servaddr);
    do {
        recvfrom(listen_sockfd, &hdr, sizeof(hdr), 0, (struct sockaddr*)&con->servaddr, &slen);
    } while (hdr.type != SYN || hdr.protocol_id != POLI_PROTOCOL_ID);
    cout << "[SERVER] AM PRIMIT SYN " << endl;

    con->sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    struct sockaddr_in data_addr;
    memset(&data_addr, 0, sizeof(data_addr));
    data_addr.sin_family = AF_INET;
    data_addr.sin_addr.s_addr = ip;
    data_addr.sin_port = 0;
    bind(con->sockfd, (const struct sockaddr *)&data_addr, sizeof(data_addr));
    socklen_t data_len = sizeof(data_addr);
    getsockname(con->sockfd, (struct sockaddr *)&data_addr, &data_len);

    //2. Trimit SYN + ACK
    struct poli_tcp_ctrl_hdr syn_ack;
    memset(&syn_ack, 0, sizeof(syn_ack));
    syn_ack.protocol_id = POLI_PROTOCOL_ID;
    syn_ack.conn_id = conn_id;
    syn_ack.type = SYN_ACK;
    syn_ack.ack_num = 0;
    syn_ack.recv_window = data_addr.sin_port;
    sendto(listen_sockfd, &syn_ack, sizeof(syn_ack), 0,
                (struct sockaddr *)&con->servaddr, sizeof(con->servaddr));
    cout << "[SERVER] AM TRIMIS SYN + ACK" << endl;

    //3. Primesc ACK on data socket (retry on timeout)
    set_timeout(con->sockfd, HS_TIMEOUT_US);

    char segment[MAX_SEGMENT_SIZE];
    while (1) {
        slen = sizeof(con->servaddr);
        int n = recvfrom(con->sockfd, segment, sizeof(segment), 0,
                         (struct sockaddr*)&con->servaddr, &slen);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                sendto(listen_sockfd, &syn_ack, sizeof(syn_ack), 0,
                            (struct sockaddr *)&con->servaddr, sizeof(con->servaddr));
                continue;
            }
            continue;
        }

        if (n == (int)sizeof(struct poli_tcp_ctrl_hdr)) {
            struct poli_tcp_ctrl_hdr *ack_hdr = (struct poli_tcp_ctrl_hdr *)segment;
            if (ack_hdr->type == ACK && ack_hdr->protocol_id == POLI_PROTOCOL_ID)
                break;
            continue;
        }

        struct poli_tcp_data_hdr *data_hdr = NULL;
        if (!valid_data(segment, n, &data_hdr))
            continue;
        if (data_hdr->conn_id != (uint8_t)conn_id)
            continue;

        process_segment(con, conn_id, segment, n, data_hdr);
        break;
    }
    cout << "[SERVER] AM PRIMIT ACK" << endl;


     pthread_mutex_lock(&cons_lock);
     cons.insert({conn_id, con});
     pthread_mutex_unlock(&cons_lock);

     /* Since we can have multiple connection, we want to know if data is available
         on the socket used by a given connection. We use POLL for this */
     data_fds[fdmax].fd = con->sockfd;    
     data_fds[fdmax].events = POLLIN;    
    
     /* This creates a timer and sets it to trigger every 1 sec. We use this
         to know if a timeout has happend on a connection */
     timer_fds[fdmax].fd = timerfd_create(CLOCK_REALTIME,  0);    
     timer_fds[fdmax].events = POLLIN;    
     con->timerfd = timer_fds[fdmax].fd;
     struct itimerspec spec;     
     spec.it_value.tv_sec = 0;    
     spec.it_value.tv_nsec = 20000000;    
     spec.it_interval.tv_sec = 0;    
     spec.it_interval.tv_nsec = 20000000;    
     timerfd_settime(timer_fds[fdmax].fd, 0, &spec, NULL);    
     fdmax++;    

    DEBUG_PRINT("Connection established!");

    return conn_id;
}

void init_receiver(int recv_buffer_bytes)
{
    pthread_t thread1;
    int ret;

    /* TODO: Create the connection socket and bind it to 8031 */

    ret = pthread_create( &thread1, NULL, receiver_handler, NULL);
    assert(ret == 0);

    //1. cream socket ul
    int sockfd;
    struct sockaddr_in servaddr;
    if ((sockfd = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
        perror("socket creation failed");
        exit(EXIT_FAILURE);
    }

    //2. scriem datele unde trimitem
    memset(&servaddr, 0, sizeof(servaddr));
    servaddr.sin_family = AF_INET; // IPv4
    // 0.0.0.0, basically match any IP
    servaddr.sin_addr.s_addr = INADDR_ANY;
    servaddr.sin_port = htons(8031);

    //3. facem bind pe portul speficicat
    bind(sockfd, (const struct sockaddr *)&servaddr, sizeof(servaddr));
    //DIE(rc < 0, "bind failed");
}
