#include <pthread.h>
#include <cstdlib>
#include <map>
#include <cstdint>
#include "lib.h"
#include "utils.h"
#include "protocol.h"
#include <cassert>
#include <poll.h>
#include <sys/timerfd.h>
#include <unistd.h>
#include <time.h>
#include <errno.h>
#include <sys/time.h>

using namespace std;

std::map<int, struct connection *> cons;
pthread_mutex_t cons_lock = PTHREAD_MUTEX_INITIALIZER;

struct pollfd data_fds[MAX_CONNECTIONS];
/* Used for timers per connection */
struct pollfd timer_fds[MAX_CONNECTIONS];
int fdmax = 0;
int g_retx_timeout_ms = 100;

static uint64_t now_ms()
{
    struct timespec ts;
    uint64_t ms = 0;
    int rc = clock_gettime(CLOCK_MONOTONIC, &ts);
    if (rc == 0) {
        ms = (uint64_t)ts.tv_sec * 1000 + (uint64_t)(ts.tv_nsec / 1000000);
    } else {
        ms = (uint64_t)time(NULL) * 1000;
    }
    if (ms == 0)
        ms = 1;
    return ms;
}

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
    setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
}


static void build_segment(struct window_segment *seg, int conn_id, uint32_t seq,
                          const char *buf, int len)
{
    seg->len = len;
    seg->acked = false;
    seg->last_sent_ms = now_ms();

    struct poli_tcp_data_hdr *hdr = (struct poli_tcp_data_hdr *)seg->data;
    hdr->protocol_id = POLI_PROTOCOL_ID;
    hdr->conn_id = conn_id;
    hdr->type = 4;
    hdr->seq_num = seq;
    hdr->len = len;

    memcpy(seg->data + sizeof(struct poli_tcp_data_hdr), buf, len);

    uint32_t csum = checksum32((const uint8_t *)seg->data,
                               sizeof(struct poli_tcp_data_hdr) + len);
    memcpy(seg->data + sizeof(struct poli_tcp_data_hdr) + len, &csum, CHECKSUM_SIZE);
}

static void process_ack(struct connection *con, uint32_t ack_seq)
{
    if (con->send_window.empty())
        return;
    if (ack_seq < con->send_base)
        return;
    auto it = con->send_window.find(ack_seq);
    if (it != con->send_window.end())
        it->second.acked = true;

    while (con->send_window.find(con->send_base) != con->send_window.end() &&
           con->send_window[con->send_base].acked) {
        con->send_window.erase(con->send_base);
        con->send_base++;
    }

    if (con->send_base > con->next_seq_num)
        con->send_base = con->next_seq_num;
}

static void resend_window(struct connection *con)
{
    uint64_t now = now_ms();
    int resent = 0;
    for (uint32_t seq = con->send_base; seq < con->next_seq_num; ++seq) {
        auto it = con->send_window.find(seq);
        if (it == con->send_window.end())
            continue;
        struct window_segment &seg = it->second;
        if (seg.acked)
            continue;
        if ((now - seg.last_sent_ms) < (uint64_t)g_retx_timeout_ms)
            continue;
        sendto(con->sockfd, seg.data,
               sizeof(struct poli_tcp_data_hdr) + seg.len + CHECKSUM_SIZE, 0,
               (struct sockaddr *)&con->servaddr, sizeof(con->servaddr));
        seg.last_sent_ms = now;
        resent++;
        if ((resent % 4) == 0)
            usleep(100);
    }
}

int send_data(int conn_id, char *buffer, int len)
{
    // int size = 0;
    pthread_mutex_lock(&cons_lock);
    auto it = cons.find(conn_id);
    if (it == cons.end()) {
        pthread_mutex_unlock(&cons_lock);
        return -1;
    }
    struct connection *con = it->second;
    pthread_mutex_unlock(&cons_lock);
    
    int bytes_processed = 0;
    
    while (bytes_processed < len) {
        pthread_mutex_lock(&con->con_lock);
        
        if (con->next_seq_num < con->send_base + con->max_window_seq) {
            
            int chunk_size = min((int)MAX_PAYLOAD_SIZE, len - bytes_processed);
            
            struct window_segment new_segment;
            build_segment(&new_segment, conn_id, con->next_seq_num, buffer + bytes_processed, chunk_size);

            con->send_window[con->next_seq_num] = new_segment;
                 sendto(con->sockfd, con->send_window[con->next_seq_num].data,
                     sizeof(struct poli_tcp_data_hdr) + chunk_size + CHECKSUM_SIZE, 0,
                     (struct sockaddr *)&con->servaddr, sizeof(con->servaddr));
            DEBUG_PRINT("Trimit seq %u\n", con->next_seq_num);
            con->next_seq_num++;
            bytes_processed += chunk_size;

            pthread_mutex_unlock(&con->con_lock);
        } else {
            pthread_mutex_unlock(&con->con_lock);
            usleep(1000);
        }
    }
    return bytes_processed;
}

void *sender_handler(void *arg)
{
    int res = 0;
    char buf[MAX_SEGMENT_SIZE];

    while (1) {

        if (cons.size() == 0) {
            usleep(10000); // 10ms
            continue;
        }
        int conn_id = -1;
        res = recv_message_or_timeout(buf, MAX_SEGMENT_SIZE, &conn_id);

        // CAZUL 1: TIMEOUT (res == -1) - Retransmitem pachetele pierdute
        if (res == -1) {
           if (conn_id != -1) {
                pthread_mutex_lock(&cons_lock);
                auto it = cons.find(conn_id);
                struct connection *con = (it == cons.end()) ? NULL : it->second;
                pthread_mutex_unlock(&cons_lock);
                if (!con)
                    continue;
                pthread_mutex_lock(&con->con_lock);
                resend_window(con);

                pthread_mutex_unlock(&con->con_lock);
            }
            continue;
        }

        // Nici un eveniment pe socket/timer
        if (res == -14)
            continue;
        // CAZUL 2: am primit ceva res > 10
        if (res > 0 && conn_id != -1) {
            pthread_mutex_lock(&cons_lock);
            auto it = cons.find(conn_id);
            struct connection *con = (it == cons.end()) ? NULL : it->second;
            pthread_mutex_unlock(&cons_lock);
            if (!con)
                continue;
            
            if (res == sizeof(struct poli_tcp_ctrl_hdr)) {
                struct poli_tcp_ctrl_hdr *hdr = (struct poli_tcp_ctrl_hdr *)buf;
                
                if (hdr->type == ACK) {
                    pthread_mutex_lock(&con->con_lock);
                    
                    process_ack(con, hdr->ack_num);
                    
                    pthread_mutex_unlock(&con->con_lock);
                }
            }
        }
    }
    
    return NULL;
}

int setup_connection(uint32_t ip, uint16_t port)
{
    /* Implement the sender part of the Three Way Handshake. Blocks
    until the connection is established */

    //struct connection *con = (struct connection *)malloc(sizeof(struct connection));
    struct connection *con = new connection();
    int conn_id = fdmax;
    con->max_window_seq = 100;
    con->send_base = 0;
    con->next_seq_num = 0;
    con->sockfd = socket(AF_INET, SOCK_DGRAM, 0);

    /* // This can be used to set a timer on a socket 
    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = 200000;
    setsockopt(con->sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        perror("Error");
    } */

    /* We will send the SYN on 8031. Then we will receive a SYN-ACK with the connection
     * port. We can use con->sockfd for both cases, but we will need to update server_addr
     * with the port received via SYN-ACK */
    memset(&con->servaddr, 0, sizeof(con->servaddr));
    con->servaddr.sin_family = AF_INET;
    con->servaddr.sin_addr.s_addr = ip;
    con->servaddr.sin_port = port;

    set_timeout(con->sockfd, 200000);

    //1. Trimit SYN, retry until SYN-ACK
    struct poli_tcp_ctrl_hdr hdr;
    socklen_t slen = sizeof(con->servaddr);
    while (1) {
        memset(&hdr, 0, sizeof(hdr));
        hdr.protocol_id = POLI_PROTOCOL_ID;
        hdr.conn_id = conn_id;
        hdr.type = SYN;
        hdr.ack_num = 0;
        hdr.recv_window = 0;
        sendto(con->sockfd, &hdr, sizeof(hdr), 0,
                    (struct sockaddr *)&con->servaddr, sizeof(con->servaddr));

        int n = recvfrom(con->sockfd, &hdr, sizeof(hdr), 0,
                         (struct sockaddr*)&con->servaddr, &slen);
        if (n >= (int)sizeof(hdr) && hdr.type == SYN_ACK && hdr.protocol_id == POLI_PROTOCOL_ID)
            break;
        if (errno != EAGAIN && errno != EWOULDBLOCK)
            continue;
    }
    cout << "[CLIENT] AM PRIMIT SYN + ACK" << endl;

    conn_id = hdr.conn_id;
    con->conn_id = conn_id;
    con->servaddr.sin_port = hdr.recv_window;

    //3. Trimit ACK
    memset(&hdr, 0, sizeof(hdr));
    hdr.protocol_id = POLI_PROTOCOL_ID;
    hdr.conn_id = conn_id;
    hdr.type = ACK;
    hdr.ack_num = 0;
    hdr.recv_window = 0;
    sendto(con->sockfd, &hdr, sizeof(hdr), 0,
                (struct sockaddr *)&con->servaddr, sizeof(con->servaddr));
    cout << "[CLIENT] AM TRIMIS ACK" << endl;

    pthread_mutex_init(&con->con_lock, NULL);
    pthread_mutex_lock(&cons_lock);
    cons.insert({conn_id, con});
    pthread_mutex_unlock(&cons_lock);

    /* Since we can have multiple connection, we want to know if data is available
       on the socket used by a given connection. We use POLL for this */
    data_fds[fdmax].fd = con->sockfd;    
    data_fds[fdmax].events = POLLIN;    
    
    /* This creates a timer and sets it to trigger every 1 sec. We use this
        to know if a timeout has happend on our connection */
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

void init_sender(int speed, int delay)
{
    pthread_t thread1;
    int ret;

    g_retx_timeout_ms = TIMEOUT_SEND(delay);

    /* Create a thread that will*/
    ret = pthread_create( &thread1, NULL, sender_handler, NULL);
    assert(ret == 0);
}
