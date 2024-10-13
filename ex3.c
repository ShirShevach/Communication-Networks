/*
 * Copyright (c) 2005 Topspin Communications.  All rights reserved.
 * Copyright (c) 2006 Cisco Systems.  All rights reserved.
 *
 * This software is available to you under a choice of one of two
 * licenses.  You may choose to be licensed under the terms of the GNU
 * General Public License (GPL) Version 2, available from the file
 * COPYING in the main directory of this source tree, or the
 * OpenIB.org BSD license below:
 *
 *     Redistribution and use in source and binary forms, with or
 *     without modification, are permitted provided that the following
 *     conditions are met:
 *
 *      - Redistributions of source code must retain the above
 *        copyright notice, this list of conditions and the following
 *        disclaimer.
 *
 *      - Redistributions in binary form must reproduce the above
 *        copyright notice, this list of conditions and the following
 *        disclaimer in the documentation and/or other materials
 *        provided with the distribution.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/param.h>
#include <sys/time.h>
#include <getopt.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <time.h>
#include <infiniband/verbs.h>

#define WC_BATCH (1) // Batch size for completion queue polling
#define WARMUP (96)
#define NUM_MSG (96)
#define MAX_EAGER_SIZE (4 * 1024)
#define DELIMITER ('$')
#define MAX_NUM_KEYS (30)
#define MAX_MSG_SIZE (1048576)
#define RX_DEPTH (100)
#define TX_DEPTH (100)
#define MIN_ROUTS (10)
#define CLIENT_NUM (1)
#define BUFS_NUM (96)
#define OCCUPIED (8)
#define AFTER_RENDEVU (2)
#define AFTER_EAGER (2)
#define RESEND (-2)

enum
{
    CLIENT_SET_EAGER = 1,
    CLIENT_SET_RENDEVU = 2,
    SERVER_SET_RENDEVU = 3,
    CLIENT_GET = 4,
    SERVER_GET_EAGER = 5,
    SERVER_GET_RENDEVU = 6,
    UNLOCK_ENTRY = 7,
};

// Enumeration for work request IDs to differentiate between send and receive operations
enum
{
    PINGPONG_RECV_WRID = 1, // Work Request ID for receive operation
    PINGPONG_SEND_WRID = 2, // Work Request ID for send operation
};

enum
{
    GET = 1,
    SET = 2,
};

struct entry
{
    char *key;
    char *value;
    int in_set;
};

static int page_size; // System page size used for memory alignment

// Structure to hold the context of InfiniBand resources
struct pingpong_context
{
    struct ibv_context *context;      // IB device context
    struct ibv_comp_channel *channel; // Completion event channel
    struct ibv_pd *pd;                // Protection domain
    struct ibv_mr *mr;                // Memory region
    struct ibv_mr *mrs[BUFS_NUM];                // Memory region
    struct ibv_cq *cq;                // Completion queue
    struct ibv_qp *qp;                // Queue pair
    void *buf;                        // Data buffer
    void* bufs[BUFS_NUM];
    int size;                         // Size of the buffer
    int rx_depth;                     // Receive queue depth
    int routs;                        // Number of outstanding receive requests
    struct ibv_port_attr portinfo;    // Port attributes
    struct ibv_mr *value_mr;                // Memory region we add
    int buf_index;
    int client_id;

};

// Structure to hold the connection information (LID, QPN, PSN, GID)
struct pingpong_dest
{
    int lid;           // Local Identifier for IB port
    int qpn;           // Queue Pair Number
    int psn;           // Packet Sequence Number
    union ibv_gid gid; // Global Identifier for IB port
};

// Function to convert MTU size to enum value
enum ibv_mtu pp_mtu_to_enum(int mtu)
{
    switch (mtu)
    {
    case 256:
        return IBV_MTU_256;
    case 512:
        return IBV_MTU_512;
    case 1024:
        return IBV_MTU_1024;
    case 2048:
        return IBV_MTU_2048;
    case 4096:
        return IBV_MTU_4096;
    default:
        return -1;
    }
}

// Function to get the local LID (Local Identifier)
uint16_t pp_get_local_lid(struct ibv_context *context, int port)
{
    struct ibv_port_attr attr;
    if (ibv_query_port(context, port, &attr))
        return 0;
    return attr.lid;
}

// Function to query port information and store it in 'attr'
int pp_get_port_info(struct ibv_context *context, int port, struct ibv_port_attr *attr)
{
    return ibv_query_port(context, port, attr);
}

// Function to convert a string GID to an ibv_gid structure
void wire_gid_to_gid(const char *wgid, union ibv_gid *gid)
{
    char tmp[9];
    uint32_t v32;
    int i;
    for (tmp[8] = 0, i = 0; i < 4; ++i)
    {
        memcpy(tmp, wgid + i * 8, 8);
        sscanf(tmp, "%x", &v32);
        *(uint32_t *)(&gid->raw[i * 4]) = ntohl(v32);
    }
}

// Function to convert an ibv_gid structure to a string GID
void gid_to_wire_gid(const union ibv_gid *gid, char wgid[])
{
    int i;
    for (i = 0; i < 4; ++i)
        sprintf(&wgid[i * 8], "%08x", htonl(*(uint32_t *)(gid->raw + i * 4)));
}

// Function to establish a connection between QPs (Queue Pairs)
static int pp_connect_ctx(struct pingpong_context *ctx, int port, int my_psn,
                          enum ibv_mtu mtu, int sl,
                          struct pingpong_dest *dest, int sgid_idx)
{
    struct ibv_qp_attr attr = {
        .qp_state = IBV_QPS_RTR,  // Set QP state to Ready to Receive (RTR)
        .path_mtu = mtu,          // Set path MTU
        .dest_qp_num = dest->qpn, // Set destination QP number
        .rq_psn = dest->psn,      // Set receive PSN
        .max_dest_rd_atomic = 1,  // Max outstanding reads
        .min_rnr_timer = 12,      // RNR NAK timer
        .ah_attr = {
            .is_global = 0,
            .dlid = dest->lid,  // Set destination LID
            .sl = sl,           // Set service level
            .src_path_bits = 0, // Set source path bits
            .port_num = port    // Set port number
        }};

    if (dest->gid.global.interface_id)
    {
        attr.ah_attr.is_global = 1;
        attr.ah_attr.grh.hop_limit = 1;
        attr.ah_attr.grh.dgid = dest->gid;
        attr.ah_attr.grh.sgid_index = sgid_idx;
    }
    if (ibv_modify_qp(ctx->qp, &attr,
                      IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU |
                          IBV_QP_DEST_QPN | IBV_QP_RQ_PSN |
                          IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER))
    {
        fprintf(stderr, "Failed to modify QP to RTR\n");
        return 1;
    }

    attr.qp_state = IBV_QPS_RTS; // Set QP state to Ready to Send (RTS)
    attr.timeout = 14;           // Timeout for retries
    attr.retry_cnt = 7;          // Number of retries
    attr.rnr_retry = 7;          // RNR retries
    attr.sq_psn = my_psn;        // Set send PSN
    attr.max_rd_atomic = 1;      // Max outstanding reads

    if (ibv_modify_qp(ctx->qp, &attr,
                      IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
                          IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC))
    {
        fprintf(stderr, "Failed to modify QP to RTS\n");
        return 1;
    }

    return 0;
}

// Function for client to exchange connection information with server
static struct pingpong_dest *pp_client_exch_dest(const char *servername, int port,
                                                 const struct pingpong_dest *my_dest)
{
    struct addrinfo *res, *t;
    struct addrinfo hints = {
        .ai_family = AF_INET,      // IPv4
        .ai_socktype = SOCK_STREAM // TCP
    };
    char *service;
    char msg[sizeof "0000:000000:000000:00000000000000000000000000000000"];
    int n;
    int sockfd = -1;
    struct pingpong_dest *rem_dest = NULL;
    char gid[33];

    if (asprintf(&service, "%d", port) < 0)
        return NULL;

    n = getaddrinfo(servername, service, &hints, &res);
    if (n < 0)
    {
        fprintf(stderr, "%s for %s:%d\n", gai_strerror(n), servername, port);
        free(service);
        return NULL;
    }

    for (t = res; t; t = t->ai_next)
    {
        sockfd = socket(t->ai_family, t->ai_socktype, t->ai_protocol);
        if (sockfd >= 0)
        {
            if (!connect(sockfd, t->ai_addr, t->ai_addrlen))
                break;
            close(sockfd);
            sockfd = -1;
        }
    }

    freeaddrinfo(res);
    free(service);

    if (sockfd < 0)
    {
        fprintf(stderr, "Couldn't connect to %s:%d\n", servername, port);
        return NULL;
    }

    gid_to_wire_gid(&my_dest->gid, gid);
    sprintf(msg, "%04x:%06x:%06x:%s", my_dest->lid, my_dest->qpn, my_dest->psn, gid);
    if (write(sockfd, msg, sizeof msg) != sizeof msg)
    {
        fprintf(stderr, "Couldn't send local address\n");
        goto out;
    }

    if (read(sockfd, msg, sizeof msg) != sizeof msg)
    {
        perror("client read");
        fprintf(stderr, "Couldn't read remote address\n");
        goto out;
    }

    write(sockfd, "done", sizeof "done");

    rem_dest = malloc(sizeof *rem_dest);
    if (!rem_dest)
        goto out;

    sscanf(msg, "%x:%x:%x:%s", &rem_dest->lid, &rem_dest->qpn, &rem_dest->psn, gid);
    wire_gid_to_gid(gid, &rem_dest->gid);

out:
    close(sockfd);
    return rem_dest;
}

// Function for server to exchange connection information with client
static struct pingpong_dest *pp_server_exch_dest(struct pingpong_context *ctx,
                                                 int ib_port, enum ibv_mtu mtu,
                                                 int port, int sl,
                                                 const struct pingpong_dest *my_dest,
                                                 int sgid_idx)
{
    struct addrinfo *res, *t;
    struct addrinfo hints = {
        .ai_flags = AI_PASSIVE,
        .ai_family = AF_INET,
        .ai_socktype = SOCK_STREAM};
    char *service;
    char msg[sizeof "0000:000000:000000:00000000000000000000000000000000"];
    int n;
    int sockfd = -1, connfd;
    struct pingpong_dest *rem_dest = NULL;
    char gid[33];

    if (asprintf(&service, "%d", port) < 0)
        return NULL;

    n = getaddrinfo(NULL, service, &hints, &res);
    if (n < 0)
    {
        fprintf(stderr, "%s for port %d\n", gai_strerror(n), port);
        free(service);
        return NULL;
    }

    for (t = res; t; t = t->ai_next)
    {
        sockfd = socket(t->ai_family, t->ai_socktype, t->ai_protocol);
        if (sockfd >= 0)
        {
            n = 1;
            setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &n, sizeof n);
            if (!bind(sockfd, t->ai_addr, t->ai_addrlen))
                break;
            close(sockfd);
            sockfd = -1;
        }
    }

    freeaddrinfo(res);
    free(service);

    if (sockfd < 0)
    {
        fprintf(stderr, "Couldn't listen to port %d\n", port);
        return NULL;
    }

    listen(sockfd, 1);
    connfd = accept(sockfd, NULL, 0);
    close(sockfd);
    if (connfd < 0)
    {
        fprintf(stderr, "accept() failed\n");
        return NULL;
    }

    n = read(connfd, msg, sizeof msg);
    if (n != sizeof msg)
    {
        perror("server read");
        fprintf(stderr, "%d/%d: Couldn't read remote address\n", n, (int)sizeof msg);
        goto out;
    }

    rem_dest = malloc(sizeof *rem_dest);
    if (!rem_dest)
        goto out;

    sscanf(msg, "%x:%x:%x:%s", &rem_dest->lid, &rem_dest->qpn, &rem_dest->psn, gid);
    wire_gid_to_gid(gid, &rem_dest->gid);

    if (pp_connect_ctx(ctx, ib_port, my_dest->psn, mtu, sl, rem_dest, sgid_idx))
    {
        fprintf(stderr, "Couldn't connect to remote QP\n");
        free(rem_dest);
        rem_dest = NULL;
        goto out;
    }

    gid_to_wire_gid(&my_dest->gid, gid);
    sprintf(msg, "%04x:%06x:%06x:%s", my_dest->lid, my_dest->qpn, my_dest->psn, gid);
    if (write(connfd, msg, sizeof msg) != sizeof msg)
    {
        fprintf(stderr, "Couldn't send local address\n");
        free(rem_dest);
        rem_dest = NULL;
        goto out;
    }

    read(connfd, msg, sizeof msg);

out:
    close(connfd);
    return rem_dest;
}

// Function to initialize the InfiniBand context and resources
static struct pingpong_context *pp_init_ctx(struct ibv_device *ib_dev, int size,
                                            int rx_depth, int tx_depth, int port,
                                            int use_event, int is_server)
{
    struct pingpong_context *ctx;

    ctx = calloc(1, sizeof *ctx);
    if (!ctx)
        return NULL;

    ctx->size = size;
    ctx->rx_depth = rx_depth;
    ctx->routs = rx_depth;

    // Allocate memory for the data buffer, aligned to the system page size
    ctx->buf = malloc(roundup(size, page_size));
    if (!ctx->buf)
    {
        fprintf(stderr, "Couldn't allocate work buf.\n");
        return NULL;
    }

    // Allocate memory for the data buffer array - we add
    for (int i = 0; i < BUFS_NUM; i++) {
        ctx->bufs[i] = malloc(roundup(size, page_size));  // Allocate rounded-up buffer size
        if (!ctx->bufs[i]) {
            fprintf(stderr, "Couldn't allocate work buffer for index %d.\n", i);
            // Optionally: free previously allocated buffers to avoid memory leaks
            for (int j = 0; j < i; j++) {
                free(ctx->bufs[j]);
            }
            free(ctx->bufs);  // Free the array of pointers itself
            return NULL;
        }
    }


    memset(ctx->buf, 0x7b + is_server, size); // Initialize buffer with a pattern

    // Open the InfiniBand device context
    ctx->context = ibv_open_device(ib_dev);
    if (!ctx->context)
    {
        fprintf(stderr, "Couldn't get context for %s\n",
                ibv_get_device_name(ib_dev));
        return NULL;
    }

    // Create a completion event channel if event-driven mode is enabled
    if (use_event)
    {
        ctx->channel = ibv_create_comp_channel(ctx->context);
        if (!ctx->channel)
        {
            fprintf(stderr, "Couldn't create completion channel\n");
            return NULL;
        }
    }
    else
        ctx->channel = NULL;

    // Allocate a protection domain (PD)
    ctx->pd = ibv_alloc_pd(ctx->context);
    if (!ctx->pd)
    {
        fprintf(stderr, "Couldn't allocate PD\n");
        return NULL;
    }

    // Register the memory region (MR)
    ctx->mr = ibv_reg_mr(ctx->pd, ctx->buf, size, IBV_ACCESS_LOCAL_WRITE);
    if (!ctx->mr)
    {
        fprintf(stderr, "error with ibv_reg_mr: %s\n", strerror(errno));
        return -1;
    }
    for(int i = 0; i < BUFS_NUM; i++){
        ctx->mrs[i] = ibv_reg_mr(ctx->pd, ctx->bufs[i], MAX_EAGER_SIZE*2, IBV_ACCESS_LOCAL_WRITE);
        if (!ctx->mrs[i])
        {
            fprintf(stderr, "error with ibv_reg_mr: %s\n", strerror(errno));
            return -1;
        }
    }


    // Create a completion queue (CQ)
    ctx->cq = ibv_create_cq(ctx->context, rx_depth + tx_depth, NULL, ctx->channel, 0);
    if (!ctx->cq)
    {
        fprintf(stderr, "Couldn't create CQ\n");
        return NULL;
    }

    // Initialize a queue pair (QP)
    struct ibv_qp_init_attr attr = {
        .send_cq = ctx->cq,
        .recv_cq = ctx->cq,
        .cap = {
            .max_send_wr = tx_depth,
            .max_recv_wr = rx_depth,
            .max_send_sge = 1,
            .max_recv_sge = 1},
        .qp_type = IBV_QPT_RC // Reliable connection (RC) QP type
    };

    ctx->qp = ibv_create_qp(ctx->pd, &attr);
    if (!ctx->qp)
    {
        fprintf(stderr, "Couldn't create QP\n");
        return NULL;
    }

    // Set QP state to INIT (initialization)
    struct ibv_qp_attr qp_attr = {
        .qp_state = IBV_QPS_INIT,
        .pkey_index = 0,
        .port_num = port,
        .qp_access_flags = IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE};

    if (ibv_modify_qp(ctx->qp, &qp_attr, IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS))
    {
        fprintf(stderr, "Failed to modify QP to INIT\n");
        return NULL;
    }

    return ctx;
}

// Function to clean up and destroy the InfiniBand context and resources
int pp_close_ctx(struct pingpong_context *ctx)
{
    if (ibv_destroy_qp(ctx->qp))
    {
        fprintf(stderr, "Couldn't destroy QP\n");
        return 1;
    }

    if (ibv_destroy_cq(ctx->cq))
    {
        fprintf(stderr, "Couldn't destroy CQ\n");
        return 1;
    }

    if (ibv_dereg_mr(ctx->mr))
    {
        fprintf(stderr, "Couldn't deregister MR\n");
        return 1;
    }

    for(int i = 0; i<BUFS_NUM; i++){
        if (ibv_dereg_mr(ctx->mrs[i]))
        {
            fprintf(stderr, "Couldn't deregister MR\n");
            return 1;
        }
    }

    if (ibv_dealloc_pd(ctx->pd))
    {
//        fprintf(stderr, "Couldn't deallocate PD\n");
        return 1;
    }

    if (ctx->channel)
    {
        if (ibv_destroy_comp_channel(ctx->channel))
        {
            fprintf(stderr, "Couldn't destroy completion channel\n");
            return 1;
        }
    }

    if (ibv_close_device(ctx->context))
    {
        fprintf(stderr, "Couldn't release context\n");
        return 1;
    }

    free(ctx->buf);
    free(ctx);

    return 0;
}

int global_index[2] = {0, 0};
// Function to post receive requests to the receive queue
static int pp_post_recv(struct pingpong_context *ctx, int n)
{
    int i;

    for (i = 0; i < n; ++i) {

        struct ibv_sge list = {
                .addr = (uintptr_t) ctx->bufs[global_index[ctx->client_id]],
                .length = MAX_MSG_SIZE,
                .lkey = ctx->mrs[global_index[ctx->client_id]]->lkey
        };
        struct ibv_recv_wr wr = {
                .wr_id = PINGPONG_RECV_WRID, // Work request ID for receive
                .sg_list = &list,
                .num_sge = 1,
                .next = NULL
        };
        struct ibv_recv_wr *bad_wr;
        global_index[ctx->client_id] = (global_index[ctx->client_id] + 1) % BUFS_NUM;
        if (ibv_post_recv(ctx->qp, &wr, &bad_wr))
            break;
    }
    return i;
}


// Function to post receive requests to the receive queue
static int pp_post_recv_client(struct pingpong_context *ctx, int n)
{
    struct ibv_sge list = {
            .addr = (uintptr_t)ctx->buf,
            .length = MAX_MSG_SIZE,
            .lkey = ctx->mr->lkey
    };
    struct ibv_recv_wr wr = {
            .wr_id = PINGPONG_RECV_WRID, // Work request ID for receive
            .sg_list = &list,
            .num_sge = 1,
            .next = NULL
    };
    struct ibv_recv_wr *bad_wr;
    int i;

    for (i = 0; i < n; ++i)
        if (ibv_post_recv(ctx->qp, &wr, &bad_wr))
            break;

    return i;
}

static int pp_post_send(struct pingpong_context *ctx)
{
    struct ibv_sge list = {
            .addr	= (uint64_t)ctx->buf,
            .length = ctx->size,
            .lkey	= ctx->mr->lkey
    };

    struct ibv_send_wr *bad_wr, wr = {
            .wr_id	    = PINGPONG_SEND_WRID,
            .sg_list    = &list,
            .num_sge    = 1,
            .opcode     = IBV_WR_SEND,
            .send_flags = IBV_SEND_SIGNALED,
            .next       = NULL
    };

    return ibv_post_send(ctx->qp, &wr, &bad_wr);
}

static int rdma_operation(struct pingpong_context *ctx, uint32_t remote_key, uint64_t remote_address, int ibv_wr_opcode,
                          struct ibv_mr *mr)
{
    struct ibv_sge list = {
        .addr = (uint64_t)mr->addr,
        .length = mr->length,
        .lkey = mr->lkey};

    struct ibv_send_wr *bad_wr, wr = {
                                    .wr_id = PINGPONG_SEND_WRID,
                                    .sg_list = &list,
                                    .num_sge = 1,
                                    .opcode = ibv_wr_opcode,
                                    .send_flags = IBV_SEND_SIGNALED,
                                    .next = NULL,
                                    .wr.rdma.rkey = remote_key,
                                    .wr.rdma.remote_addr = remote_address};

    return ibv_post_send(ctx->qp, &wr, &bad_wr);
}

// Function to wait for completions of send/receive operations
int pp_wait_completions(struct pingpong_context *ctx, int iters)
{
    int rcnt = 0, scnt = 0;
    while (rcnt + scnt < iters)
    {
        struct ibv_wc wc[WC_BATCH]; // Array for work completions
        int ne, i;

        do
        {
            ne = ibv_poll_cq(ctx->cq, WC_BATCH, wc); // Poll the completion queue
            if (ne < 0)
            {
                fprintf(stderr, "poll CwcQ failed %d\n", ne);
                return -1;
            }

        } while (ne < 1);

        for (i = 0; i < ne; ++i)
        {
            if (wc[i].status != IBV_WC_SUCCESS)
            {
                fprintf(stderr, "Failed status %s (%d) for wr_id %d\n",
                        ibv_wc_status_str(wc[i].status),
                        wc[i].status, (int)wc[i].wr_id);
                return -1;
            }

            switch ((int)wc[i].wr_id)
            {
                case PINGPONG_SEND_WRID:

                ++scnt; // Increment send count
                    break;

                case PINGPONG_RECV_WRID:
                    if (--ctx->routs <= MIN_ROUTS)
                    {
                        ctx->routs += pp_post_recv(ctx, ctx->rx_depth - ctx->routs);
                        if (ctx->routs < ctx->rx_depth)
                        {
                            fprintf(stderr, "Couldn't post receive (%d)\n", ctx->routs);
                            return -1;
                        }
                    }
                    ++rcnt; // Increment receive count
                    break;

                default:
                    fprintf(stderr, "Completion for unknown wr_id %d\n", (int)wc[i].wr_id);
                    return -1;
            }
        }
    }
    return rcnt + scnt;
}


// Function to wait for completions of send/receive operations
int pp_wait_completions_client(struct pingpong_context *ctx, int iters)
{
    int rcnt = 0, scnt = 0;
    while (rcnt + scnt < iters)
    {
        struct ibv_wc wc[WC_BATCH]; // Array for work completions
        int ne, i;

        do
        {
            ne = ibv_poll_cq(ctx->cq, WC_BATCH, wc); // Poll the completion queue
            if (ne < 0)
            {
                fprintf(stderr, "poll CwcQ failed %d\n", ne);
                return -1;
            }

        } while (ne < 1);

        for (i = 0; i < ne; ++i)
        {
            if (wc[i].status != IBV_WC_SUCCESS)
            {
                fprintf(stderr, "Failed status %s (%d) for wr_id %d\n",
                        ibv_wc_status_str(wc[i].status),
                        wc[i].status, (int)wc[i].wr_id);
                return -1;
            }

            switch ((int)wc[i].wr_id)
            {
                case PINGPONG_SEND_WRID:

                    ++scnt; // Increment send count
                    break;

                case PINGPONG_RECV_WRID:
                    if (--ctx->routs <= MIN_ROUTS)
                    {
                        ctx->routs += pp_post_recv_client(ctx, ctx->rx_depth - ctx->routs);
                        if (ctx->routs < ctx->rx_depth)
                        {
                            fprintf(stderr, "Couldn't post receive (%d)\n", ctx->routs);
                            return -1;
                        }
                    }
                    ++rcnt; // Increment receive count
                    break;

                default:
                    fprintf(stderr, "Completion for unknown wr_id %d\n", (int)wc[i].wr_id);
                    return -1;
            }
        }
    }
    return rcnt + scnt;
}

// Function to display usage information
static void usage(const char *argv0)
{
    printf("Usage:\n");
    printf("  %s            start a server and wait for connection\n", argv0);
    printf("  %s <host>     connect to server at <host>\n", argv0);
    printf("\n");
    printf("Options:\n");
    printf("  -p, --port=<port>      listen on/connect to port <port> (default 18515)\n");
    printf("  -d, --ib-dev=<dev>     use IB device <dev> (default first device found)\n");
    printf("  -i, --ib-port=<port>   use port <port> of IB device (default 1)\n");
    printf("  -s, --size=<size>      size of message to exchange (default 4096)\n");
    printf("  -m, --mtu=<size>       path MTU (default 1024)\n");
    printf("  -r, --rx-depth=<dep>   number of receives to post at a time (default 500)\n");
    printf("  -n, --iters=<iters>    number of exchanges (default 1000)\n");
    printf("  -l, --sl=<sl>          service level value\n");
    printf("  -e, --events           sleep on CQ events (default poll)\n");
    printf("  -g, --gid-idx=<gid index> local port gid index\n");
}

typedef struct
{
    struct pingpong_context *ctx; // RDMA context, managing communication and memory resources
    struct ibv_device **dev_list;
    struct pingpong_dest *rem_dest;
} kv_handle;

int kv_open(char *servername, void **handler)
{
    struct ibv_device      **dev_list;
    struct ibv_device       *ib_dev;
    struct pingpong_dest     my_dest;
    struct pingpong_dest    *rem_dest;
    char                    *ib_devname = NULL;
    int                      port = 12355;
    int                      ib_port = 1;
    enum ibv_mtu             mtu = IBV_MTU_2048;
    int                      rx_depth = RX_DEPTH;
    int                      tx_depth = TX_DEPTH;
    int                      use_event = 0;
    int                      size = MAX_EAGER_SIZE;
    int                      sl = 0;
    int                      gidx = -1;
    char                     gid[33];

    srand48(getpid() * time(NULL));
    *handler = malloc(sizeof(kv_handle*));

    page_size = sysconf(_SC_PAGESIZE);

    dev_list = ibv_get_device_list(NULL);
    if (!dev_list) {
        perror("Failed to get IB devices list");
        return 1;
    }

    if (!ib_devname) {
        ib_dev = *dev_list;
        if (!ib_dev) {
            fprintf(stderr, "No IB devices found\n");
            return 1;
        }
    } else {
        int i;
        for (i = 0; dev_list[i]; ++i)
            if (!strcmp(ibv_get_device_name(dev_list[i]), ib_devname))
                break;
        ib_dev = dev_list[i];
        if (!ib_dev) {
            fprintf(stderr, "IB device %s not found\n", ib_devname);
            return 1;
        }
    }
    kv_handle **handle = (kv_handle**) handler;
    (*handle)->ctx = pp_init_ctx(ib_dev, size, rx_depth, tx_depth, ib_port, use_event, !servername);
    struct pingpong_context** ctx = &((*handle)->ctx);
    if (!*ctx)
        return 1;

    (*ctx)->routs = pp_post_recv_client((*ctx), (*ctx)->rx_depth);
    if ((*ctx)->routs < (*ctx)->rx_depth) {
        fprintf(stderr, "Couldn't post receive (%d)\n", (*ctx)->routs);
        return 1;
    }

    if (use_event)
        if (ibv_req_notify_cq((*ctx)->cq, 0)) {
            fprintf(stderr, "Couldn't request CQ notification\n");
            return 1;
        }


    if (pp_get_port_info((*ctx)->context, ib_port, &(*ctx)->portinfo)) {
        fprintf(stderr, "Couldn't get port info\n");
        return 1;
    }

    my_dest.lid = (*ctx)->portinfo.lid;
    if ((*ctx)->portinfo.link_layer == IBV_LINK_LAYER_INFINIBAND && !my_dest.lid) {
        fprintf(stderr, "Couldn't get local LID\n");
        return 1;
    }

    if (gidx >= 0) {
        if (ibv_query_gid((*ctx)->context, ib_port, gidx, &my_dest.gid)) {
            fprintf(stderr, "Could not get local gid for gid index %d\n", gidx);
            return 1;
        }
    } else
        memset(&my_dest.gid, 0, sizeof my_dest.gid);

    my_dest.qpn = (*ctx)->qp->qp_num;
    my_dest.psn = lrand48() & 0xffffff;
    inet_ntop(AF_INET6, &my_dest.gid, gid, sizeof gid);
    printf("  local address:  LID 0x%04x, QPN 0x%06x, PSN 0x%06x, GID %s\n",
           my_dest.lid, my_dest.qpn, my_dest.psn, gid);


    rem_dest = pp_client_exch_dest(servername, port, &my_dest);

    if (!rem_dest)
        return 1;

    inet_ntop(AF_INET6, &rem_dest->gid, gid, sizeof gid);
    printf("  remote address: LID 0x%04x, QPN 0x%06x, PSN 0x%06x, GID %s\n",
           rem_dest->lid, rem_dest->qpn, rem_dest->psn, gid);

    if (pp_connect_ctx((*ctx), ib_port, my_dest.psn, mtu, sl, rem_dest, gidx))
        return 1;

    (*handle)->ctx = (*ctx);
    (*handle)->dev_list = dev_list;
    (*handle)->rem_dest = rem_dest;
    return 0;
}


void kv_release(char *value)
{
    free(value);
}

void client_ack_after_get_rendevu(void *handle, int current_client, const char *key) {

    kv_handle *handler = (kv_handle *)handle;
    struct pingpong_context *context = handler->ctx;
    // ACK 9$number_client for the server to dereg it's memory region
    memset(context->buf, 0, MAX_EAGER_SIZE);
    snprintf(context->buf, MAX_EAGER_SIZE, "9$%d$%s", current_client, key);
    context->size = strlen(context->buf)+1;
    pp_post_send(context);
    pp_wait_completions_client(context, 1);
}

int kv_get(void *handle, const char *key, char **value)
{

    kv_handle *handler = (kv_handle *)handle;
    struct pingpong_context *context = handler->ctx;
    memset(context->buf, 0, MAX_EAGER_SIZE);
    // "4key"
    snprintf(context->buf, MAX_EAGER_SIZE, "%d%s", CLIENT_GET, key);
    context->size = strlen(context->buf) +1;
    pp_post_send(context);
    pp_wait_completions_client(context, 2); // waiting for the msg to send and waiting for answer from the server.
    if (atoi(context->buf) == OCCUPIED) {
        return RESEND;
    }

    // eager protocol
    if (atoi(context->buf) == SERVER_GET_EAGER)
    {
        // "5value"
        *value = (char *)calloc(MAX_EAGER_SIZE, sizeof(char));
        if (!(*value))
        {
            fprintf(stderr, "Error in calloc\n");
            return 1;
        }
        sscanf(context->buf + 1, "%s", *value);
        memcpy(*value, context->buf + 1, MAX_EAGER_SIZE);
        return AFTER_EAGER;

    } // rendevu protocol
    else if (atoi(context->buf) == SERVER_GET_RENDEVU)
    {
        char* original_buf = context->buf;
        // "6valueSize$remoteKey$remoteAddress"
        int valueSize;
        uint32_t remote_key;
        uint64_t remote_address;
        int current_client;
        sscanf(context->buf + 1, "$%d$%u$%llu$%d", &valueSize, &remote_key, &remote_address, &current_client);
        *value = (char *)calloc(valueSize+1, sizeof(char));
        if (!(*value))
        {
            fprintf(stderr, "Error in calloc\n");
            return 1;
        }
        context->buf = *value;
        context->value_mr = ibv_reg_mr(context->pd, *value, valueSize+1, IBV_ACCESS_LOCAL_WRITE|IBV_ACCESS_REMOTE_WRITE|IBV_ACCESS_REMOTE_READ);
        int res = rdma_operation(context, remote_key, remote_address, IBV_WR_RDMA_READ, context->value_mr); // read
        // Wait for the RDMA read operation to complete.
        pp_wait_completions(context, 1);
        ibv_dereg_mr(context->value_mr);
        context->buf = original_buf;
        memset(context->buf, 0, MAX_EAGER_SIZE);
        context->size = MAX_EAGER_SIZE;
        return current_client;

    }
}

void client_ack_after_set_rendevu(void *handle, const char *key) {
    kv_handle *handler = (kv_handle *)handle;
    struct pingpong_context *context = handler->ctx;
    // 7$key
    memset(context->buf, 0, strlen(context->buf));
    snprintf(context->buf, MAX_EAGER_SIZE, "%d$%s", UNLOCK_ENTRY, key);
    context->size = strlen(context->buf);
    pp_post_send(context);
    pp_wait_completions_client(context, 1); // WAITING FOR THE MSG TO SEND
}

int kv_set(void *handle, const char *key, const char *value)
{
    kv_handle *handler = (kv_handle *)handle;
    struct pingpong_context *context = handler->ctx;
    memset(context->buf, 0, strlen(context->buf));

    // eager protocol
    if (strlen(value) < MAX_EAGER_SIZE)
    {
        // "1key$value"
        snprintf(context->buf, MAX_EAGER_SIZE, "%d%s%c%s", CLIENT_SET_EAGER, key, DELIMITER, value);
        context->size = strlen(context->buf) + 1;
        if(pp_post_send(context)) {
            printf("Error in pp_post_send kv_set\n");
            return -1;
        }
        int res = pp_wait_completions_client(context, 1); // completion for the send msg above (no ACK)
        if (res < 0){
            printf("Error in pp_wait_completions_client kv_set\n");
            return -1;
        }
    }

    // rendevu protocol
    else
    {
        // "2key$valueSize"
        while (1) {
            snprintf(context->buf, MAX_EAGER_SIZE, "%d%s%c%lu", CLIENT_SET_RENDEVU, key, DELIMITER, strlen(value)+1);
            context->size = strlen(context->buf) + 1;
            pp_post_send(context);
            pp_wait_completions_client(context, 1); // WAITING FOR THE MSG TO SEND

            context->size = MAX_EAGER_SIZE;
            pp_wait_completions_client(context, 1); // getting remote address and remote key.
            if (atoi(context->buf) != OCCUPIED ) {
                break;
            } else {
                return RESEND;
            }
        }
        // "3$remoteKey$remoteAddress"
        if (atoi(context->buf) == SERVER_SET_RENDEVU)
        {
            uint32_t remote_key = 0;
            uint64_t remote_address = 0;
            sscanf(context->buf + 1, "$%u$%lu", &remote_key, &remote_address);

            // Register memory and perform RDMA write
            context->value_mr = ibv_reg_mr(context->pd, (void *) value, strlen(value) +1,
                                           IBV_ACCESS_LOCAL_WRITE|IBV_ACCESS_REMOTE_WRITE|IBV_ACCESS_REMOTE_READ);
            if (!context->value_mr)
            {
                fprintf(stderr, "error with ibv_reg_mr: %s\n", strerror(errno));
                return -1;
            }
            rdma_operation(context, remote_key, remote_address, IBV_WR_RDMA_WRITE, context->value_mr);
            pp_wait_completions_client(context, 1); // WAITING FOR THE MSG TO SEND (THE RDMA MSG)
            return AFTER_RENDEVU;
        } else {
            fprintf(stderr, "Error with the first char: atoi(context->buf): %d\n", atoi(context->buf));
        }
    }
    return 0;
}

int measuring_throughput_set(int operation, void *handle, char * value)
{
    kv_handle *handler = (kv_handle *)handle;
    struct timeval start;
    struct timeval end;
    char key[5];
    int k = 0;
    int counter = 0;
    for (int size = 1; size <= MAX_MSG_SIZE; size *= 2)
    {
        // define key
        snprintf(key, sizeof(key), "A%d", k);
        k++;
        // define value
        memset(value, 'B', size);
        // warmup phase:
        for (int i = 0; i < WARMUP; ++i) {
            int res = RESEND;
            while (res == RESEND) {
                res = kv_set(handler, key, value);
                counter ++;
                if (counter % BUFS_NUM == 0) {
                    pp_wait_completions_client(handler->ctx, 1);
                }
                if (res == -1) {
                    printf("Error in kv_set\n");
                    return -1;
                }
                if (res == AFTER_RENDEVU) {
                    client_ack_after_set_rendevu(handler, key);
                    counter ++;
                    if (counter % BUFS_NUM == 0) {
                        pp_wait_completions_client(handler->ctx, 1);
                    }
                }

            }
        }

        // actual measurement phase:
        gettimeofday(&start, NULL);
        for (int i = 0; i < NUM_MSG; ++i)
        {
            int res = RESEND;
            while (res == RESEND) {
                res = kv_set(handler, key, value);
                counter ++;
                if (counter % BUFS_NUM == 0) {
                    pp_wait_completions_client(handler->ctx, 1);
                }
                if (res == -1) {
                    printf("Error in kv_set\n");
                    return -1;
                }
                if (res == AFTER_RENDEVU) {
                    client_ack_after_set_rendevu(handler, key);
                    counter ++;
                    if (counter % BUFS_NUM == 0) {
                        pp_wait_completions_client(handler->ctx, 1);
                    }
                }

            }
        }
        gettimeofday(&end, NULL);
        double elapsed_time = (double)(end.tv_sec * 1000000 - start.tv_sec * 1000000) +
                              (double)(end.tv_usec - start.tv_usec);
        double tp = (size / elapsed_time) * NUM_MSG;
        printf("%u\t%.2f\tbytes\\microseconds\n", size, tp);
    }
}

int measuring_throughput_get(int operation, void *handle)
{
    kv_handle *handler = (kv_handle *)handle;
    struct timeval start;
    struct timeval end;
    char key[10];

    int k = 0;
    int counter = 0;
    char *value = NULL;

    for (int size = 1; size <= MAX_MSG_SIZE; size *= 2)
    {
        // define key
        snprintf(key, sizeof(key), "A%d", k);
        k++;

        // warmup phase:

        for (int i = 0; i < WARMUP; ++i)
        {
            int res = RESEND;
            while (res == RESEND) {
                res = kv_get(handle, key, &value);
                counter ++;
                if (counter % BUFS_NUM == 0) {
                    pp_wait_completions_client(handler->ctx, 1);
                }
                if (res == -1) {
                    printf("Error in kv_set\n");
                    return -1;
                }
                if (res >= 0 && res != AFTER_EAGER) {
                    client_ack_after_get_rendevu(handler, res, key);
                    counter ++;
                    if (counter % BUFS_NUM == 0) {
                        pp_wait_completions_client(handler->ctx, 1);
                    }
                }
            }
        }

        // actual measurement phase:
        gettimeofday(&start, NULL);

        for (int i = 0; i < NUM_MSG; ++i)
        {
            int res = RESEND;
            while (res == RESEND) {
                res = kv_get(handle, key, &value);
                counter ++;
                if (counter % BUFS_NUM == 0) {
                    pp_wait_completions_client(handler->ctx, 1);
                }
                if (res == -1) {
                    printf("Error in kv_set\n");
                    return -1;
                }
                if (res >= 0 && res != AFTER_EAGER) {
                    client_ack_after_get_rendevu(handler, res, key);
                    counter ++;
                    if (counter % BUFS_NUM == 0) {
                        pp_wait_completions_client(handler->ctx, 1);
                    }
                }
            }
        }
        gettimeofday(&end, NULL);
        double time_passed = (double)(end.tv_sec * 1000000 - start.tv_sec * 1000000) +
                              (double)(end.tv_usec - start.tv_usec);
        double tp = (size / time_passed) * NUM_MSG;
        printf("%u\t%.2f\tbytes\\microseconds\n", size, tp);
    }
}

// Function to initialize the dictionary
struct entry **init_dictionary()
{
    struct entry **dictionary = malloc(MAX_NUM_KEYS * sizeof(struct entry *));
    for (int i = 0; i < MAX_NUM_KEYS; i++)
    {
        dictionary[i] = NULL;
    }
    return dictionary;
}

// Function to print the dictionary
void print_dictionary(struct entry **dictionary)
{
    printf("DICTIONARY: {\n");
    for (int i = 0; i < MAX_NUM_KEYS; i++)
    {
        if (dictionary[i] != NULL)
        {
            printf("Key: %s, len_Value: %lu, in_set: %d\n", dictionary[i]->key, strlen(dictionary[i]->value), dictionary[i]->in_set);
        }
    }
    printf("}\n");
}

int is_in_set(struct entry **dictionary, char *key) {
    for (int i = 0; i < MAX_NUM_KEYS; i++)
    {
        if (dictionary[i] != NULL && strcmp(dictionary[i]->key, key) == 0)
        {
            return dictionary[i]->in_set;
        }
    }
    return 0; // Key not found
}

void lock_in_set(struct entry **dictionary, char* key) {
    for (int i = 0; i < MAX_NUM_KEYS; i++)
    {
        if (dictionary[i] != NULL && strcmp(dictionary[i]->key, key) == 0)
        {
            dictionary[i]->in_set = 1;
            return;
        }
    }
}

void unlock_in_set(struct entry **dictionary, char *key) {
    for (int i = 0; i < MAX_NUM_KEYS; i++)
    {
        if (dictionary[i] != NULL && strcmp(dictionary[i]->key, key) == 0)
        {
            dictionary[i]->in_set = 0;
            return;
        }
    }
}

int handle_unlock_entry(struct pingpong_context *ctx, struct entry **dictionary, int buf_index) {
    char *new_key = calloc(MAX_EAGER_SIZE, sizeof(char));
    sscanf(ctx->bufs[buf_index] + 1, "%s", new_key);
    unlock_in_set(dictionary, new_key);
    return 0;
}

// Function to set a key-value pair in the dictionary
void set_key_value(struct entry **dictionary, char **pointer_key, char **pointer_value)
{
    for (int i = 0; i < MAX_NUM_KEYS; i++)
    {
        if (dictionary[i] != NULL && strcmp(dictionary[i]->key, *pointer_key) == 0)
        {
            // Key already exists, update the value
            free(dictionary[i]->value);
//            free(dictionary[i]->key);
            dictionary[i]->value = *pointer_value;
//            dictionary[i]->key = *pointer_key;
            dictionary[i]->in_set = 0;

            print_dictionary(dictionary);
            return;
        }
        else if (dictionary[i] == NULL)
        {
            // Found an empty spot, create a new entry
            dictionary[i] = malloc(sizeof(struct entry));
            dictionary[i]->key = *pointer_key;
            dictionary[i]->value = *pointer_value;
            dictionary[i]->in_set = 0;
            print_dictionary(dictionary);
            return;
        }
    }
    printf("Dictionary is full, cannot add more entries.\n");
}


// Function to get a value by key
char *get_value(struct entry **dictionary, const char *key)
{
    for (int i = 0; i < MAX_NUM_KEYS; i++)
    {
        if (dictionary[i] != NULL && strcmp(dictionary[i]->key, key) == 0)
        {
            return dictionary[i]->value;
        }
    }
    return NULL; // Key not found
}

// Function to free the dictionary
void free_dictionary(struct entry **dictionary)
{
    for (int i = 0; i < MAX_NUM_KEYS; i++)
    {
        if (dictionary[i] != NULL)
        {
            free(dictionary[i]->key);
            free(dictionary[i]->value);
            free(dictionary[i]);
        }
    }
    free(dictionary);
}

int handle_set_eager(struct pingpong_context *ctx, struct entry **dictionary, int buf_index, int msg_num)
{
    // 1key$value
    char *new_key = calloc(MAX_EAGER_SIZE, sizeof(char));
    char *new_value = calloc(MAX_EAGER_SIZE, sizeof(char));

    if (sscanf((ctx->bufs[buf_index]) + 1, "%4096[^$]$%4096s", new_key, new_value) == 2)
    {
        set_key_value(dictionary, &new_key, &new_value);
        return 0;
    }
    else
    {
        fprintf(stderr, "Error in the format of set eager\n");
        return -1;
    }
}

int handle_set_rendevu(struct pingpong_context *ctx, struct entry **dictionary, int buf_index)
{
    // 2key$valueSize
    char *new_key = calloc(MAX_EAGER_SIZE, sizeof(char));
    if (!new_key) {
        fprintf(stderr, "Error with data allocation in set_rendevu new_key\n");
        return -1;
    }

    size_t value_size;
    if (sscanf(ctx->bufs[buf_index] + 1, "%4095[^$]$%lu", new_key, &value_size) == 2)
    {
        if (is_in_set(dictionary, new_key) == 1) {
            memset(ctx->buf, 0, MAX_EAGER_SIZE);
            snprintf(ctx->buf, MAX_EAGER_SIZE, "%d", OCCUPIED);
            ctx->size = strlen(ctx->buf) + 1;
            pp_post_send(ctx);
            pp_wait_completions(ctx, 1); // waiting until the msg above arrived
            ctx->size = MAX_EAGER_SIZE;
            return OCCUPIED;
        }
        char *new_value = calloc(value_size, sizeof(char));
        if (!new_value)
        {
            fprintf(stderr, "Error with data allocation in set_rendevu\n");
            return -1;
        }
        set_key_value(dictionary, &new_key, &new_value);

        // lock in_set
        lock_in_set(dictionary, new_key);

        ctx->value_mr = ibv_reg_mr(ctx->pd, new_value, value_size, IBV_ACCESS_REMOTE_WRITE|IBV_ACCESS_LOCAL_WRITE|IBV_ACCESS_REMOTE_READ);
        if (!ctx->value_mr)
        {
            fprintf(stderr, "error with ibv_reg_mr: %s\n", strerror(errno));
            free(new_value);
            return -1;
        }
        memset(ctx->buf, 0, MAX_EAGER_SIZE);
        snprintf(ctx->buf, MAX_EAGER_SIZE, "%d$%u$%lu", SERVER_SET_RENDEVU, ctx->value_mr->rkey, (u_int64_t)ctx->value_mr->addr);
        ctx->size = strlen(ctx->buf) + 1;
        pp_post_send(ctx);
        pp_wait_completions(ctx, 1); // waiting until the msg above arrived
        ctx->size = MAX_EAGER_SIZE;
        memset(ctx->buf, 0, MAX_EAGER_SIZE);
    }
    else
    {
        fprintf(stderr, "Error in the format of set rendevu\n");
        return -1;
    }
}

int handle_get(struct pingpong_context *ctx, struct entry **dictionary, int buf_index, int current_client)
{
    char new_key[MAX_EAGER_SIZE];
    // "4key"
    sscanf(ctx->bufs[buf_index] + 1, "%s", new_key);
    char * value = get_value(dictionary, new_key);
    if (!value)
    {
        fprintf(stderr, "key=%s doesn't exists in the dictionary\n", new_key);
        return -1;
    }
    if (strlen(value) < MAX_EAGER_SIZE)
    {
        memset(ctx->buf, 0, MAX_EAGER_SIZE);
        snprintf(ctx->buf, MAX_EAGER_SIZE, "%d%s", SERVER_GET_EAGER, value);
        ctx->size = strlen(ctx->buf) + 1;
        pp_post_send(ctx);
        pp_wait_completions(ctx, 1); // THIS IS WAIT FOT THE SEND COMPLETION (WAITING THAT THE MSG WILL SEND)
        memset(ctx->buf, 0, MAX_EAGER_SIZE);

    }
    else
    {
        if (is_in_set(dictionary, new_key) == 1) {
            memset(ctx->buf, 0, MAX_EAGER_SIZE);
            snprintf(ctx->buf, MAX_EAGER_SIZE, "%d", OCCUPIED);
            ctx->size = strlen(ctx->buf) + 1;
            pp_post_send(ctx);
            pp_wait_completions(ctx, 1); // waiting until the msg above arrived
            ctx->size = MAX_EAGER_SIZE;
            return OCCUPIED;
        }
        lock_in_set(dictionary, new_key);

        if(!ctx->value_mr)
        {
            ibv_dereg_mr(ctx->value_mr);;
        }
        ctx->value_mr = ibv_reg_mr(ctx->pd, value, strlen(value) +1, IBV_ACCESS_REMOTE_READ);
        if (!ctx->value_mr)
        {
            fprintf(stderr, "error with ibv_reg_mr\n");
            free(value);
            return -1;
        }
        memset(ctx->buf, 0, MAX_EAGER_SIZE);
        snprintf(ctx->buf,  MAX_EAGER_SIZE, "%d$%lu$%u$%llu$%d", SERVER_GET_RENDEVU, strlen(value), ctx->value_mr->rkey, ctx->value_mr->addr, current_client);
        ctx->size = strlen(ctx->buf) + 1;
        pp_post_send(ctx);
        pp_wait_completions(ctx, 1); // waiting for the above msg to send
        memset(ctx->buf, 0, MAX_EAGER_SIZE);

    }
}

int handle_client_msg(struct pingpong_context *ctx, struct entry **dictionary, int buf_index, int msg_num, int current_client)
{
    int indicator = atoi(ctx->bufs[buf_index]);
    if (indicator == CLIENT_SET_EAGER)
    {
        return handle_set_eager(ctx, dictionary, buf_index, msg_num);
    }
    else if (indicator == CLIENT_SET_RENDEVU)
    {
        return handle_set_rendevu(ctx, dictionary, buf_index);
    }
    else if (indicator == CLIENT_GET)
    {
        return handle_get(ctx, dictionary, buf_index, current_client);
    }
    else if (indicator == UNLOCK_ENTRY) {
        return handle_unlock_entry(ctx, dictionary, buf_index);
    }
    return -1;
}

int kv_close(void *handle)
{
    kv_handle *handler = (kv_handle *)handle;
    pp_close_ctx(handler->ctx);
    return 0;
}


int main(int argc, char *argv[])
{
    // extract the arguments
    char *servername = NULL;
    if (optind == argc - 1)
        servername = strdup(argv[optind]);
    else if (optind < argc)
    {
        usage(argv[0]);
        return 1;
    }

    // client connect to the server
    kv_handle handler;
    kv_handle *p_handler = &handler;
    if (servername)
    { // client side
        char* value = (char*) calloc(MAX_MSG_SIZE, sizeof(char));
        if (!value)
        {
            fprintf(stderr, "error in value allocation in main\n");
            return 1;
        }
        kv_open(servername, (void*) &p_handler);
        printf("before measuring_throughput_set\n");
        measuring_throughput_set(SET, p_handler, value);
        printf("after measuring_throughput_set\n");
        printf("before measuring_throughput_get\n");
        measuring_throughput_get(GET, p_handler);
        printf("after measuring_throughput_get\n");
        kv_release(value);
        kv_close(p_handler);
    }
    else
    {
        printf("we are in the server!\n");
        struct pingpong_context* ctxs[CLIENT_NUM];
        for (int i = 0; i < CLIENT_NUM; i++) {
            new_client_connect(&ctxs[i], i);
        }

        printf("connect client\n");

        // Create a dictionary to store data
        struct entry **dictionary = init_dictionary();

        // Wait for a message from a client and determine which client sent it
        int res = 0;
        int current_client = 0;
        int recv_index[2] = {0, 0};
        int counter[2] = {0, 0};
#include <unistd.h>  // For sleep()
        while (1)
        {
            res = pp_wait_completions(ctxs[current_client], 1);

            if (res > 0)
            {
                char * copy_buf = ctxs[current_client]->bufs[recv_index[current_client]];

                if (atoi(copy_buf) == 9) {
                    int client_to_release_mr;
                    char *new_key = calloc(MAX_EAGER_SIZE, sizeof(char));

                    sscanf(copy_buf + 1, "$%d$%s", &client_to_release_mr, new_key);
                    ibv_dereg_mr(ctxs[client_to_release_mr]->value_mr);
                    unlock_in_set(dictionary, new_key);

                } else if (atoi(copy_buf) == UNLOCK_ENTRY) {
                    char *new_key = calloc(MAX_EAGER_SIZE, sizeof(char));
                    sscanf(copy_buf + 1, "$%3s", new_key);
                    unlock_in_set(dictionary, new_key);

                } else {
                    // Parse the message from the current client
                    int result = handle_client_msg(ctxs[current_client], dictionary, recv_index[current_client], counter[current_client], current_client);
                }

                recv_index[current_client]  = (recv_index[current_client] + 1) % BUFS_NUM;
                res = 0;
                counter[current_client] ++;
                if (counter[current_client] % BUFS_NUM == 0) {
                    memset(ctxs[current_client]->buf, 0, strlen(ctxs[current_client]->buf));
                    snprintf(ctxs[current_client]->buf, strlen(ctxs[current_client]->buf), "ACK");
                    ctxs[current_client]->size = 4;
                    pp_post_send(ctxs[current_client]);
                    pp_wait_completions(ctxs[current_client], 1);
                }
            }
        // print_dictionary(dictionary);
            current_client = (current_client + 1) % CLIENT_NUM;
        }
        // free the dictionary
        free_dictionary(dictionary);
    }
}

int new_client_connect(struct pingpong_context **ctx, int client_id) {
    struct ibv_device **dev_list;   // List of available IB devices
    struct ibv_device *ib_dev;      // Selected IB device
    struct pingpong_dest my_dest;   // Local connection information
    struct pingpong_dest *rem_dest; // Remote connection information
    char *ib_devname = NULL;
    char *servername;
    int port = 12355;                // Default port
    int ib_port = 1;                 // Default IB port
    enum ibv_mtu mtu = IBV_MTU_2048; // Default MTU size
    int rx_depth = 100;              // Default receive queue depth
    int tx_depth = 100;              // Default send queue depth
    int use_event = 0;               // Default event mode (polling)
    int size = 1024 * 4;             // Default message size
    int sl = 0;                      // Default service level
    int gidx = -1;                   // Default GID index
    char gid[33];                    // GID string buffer
    srand48(getpid() * time(NULL));

    page_size = sysconf(_SC_PAGESIZE);

    dev_list = ibv_get_device_list(NULL);
    if (!dev_list) {
        perror("Failed to get IB devices list");
        return 1;
    }

    if (!ib_devname) {
        ib_dev = *dev_list;
        if (!ib_dev) {
            fprintf(stderr, "No IB devices found\n");
            return 1;
        }
    } else {
        int i;
        for (i = 0; dev_list[i]; ++i)
            if (!strcmp(ibv_get_device_name(dev_list[i]), ib_devname))
                break;
        ib_dev = dev_list[i];
        if (!ib_dev) {
            fprintf(stderr, "IB device %s not found\n", ib_devname);
            return 1;
        }
    }

    *ctx = pp_init_ctx(ib_dev, size, rx_depth, tx_depth, ib_port, use_event, !servername);
    if (!*ctx)
        return 1;


    (*ctx)->client_id = client_id;


    (*ctx)->routs = pp_post_recv((*ctx), (*ctx)->rx_depth);
    if ((*ctx)->routs < (*ctx)->rx_depth) {
        fprintf(stderr, "Couldn't post receive (%d)\n", (*ctx)->routs);
        return 1;
    }

    if (use_event)
        if (ibv_req_notify_cq((*ctx)->cq, 0)) {
            fprintf(stderr, "Couldn't request CQ notification\n");
            return 1;
        }


    if (pp_get_port_info((*ctx)->context, ib_port, &(*ctx)->portinfo)) {
        fprintf(stderr, "Couldn't get port info\n");
        return 1;
    }

    my_dest.lid = (*ctx)->portinfo.lid;
    if ((*ctx)->portinfo.link_layer == IBV_LINK_LAYER_INFINIBAND && !my_dest.lid) {
        fprintf(stderr, "Couldn't get local LID\n");
        return 1;
    }

    if (gidx >= 0) {
        if (ibv_query_gid((*ctx)->context, ib_port, gidx, &my_dest.gid)) {
            fprintf(stderr, "Could not get local gid for gid index %d\n", gidx);
            return 1;
        }
    } else
        memset(&my_dest.gid, 0, sizeof my_dest.gid);

    my_dest.qpn = (*ctx)->qp->qp_num;
    my_dest.psn = lrand48() & 0xffffff;
    inet_ntop(AF_INET6, &my_dest.gid, gid, sizeof gid);
    printf("  local address:  LID 0x%04x, QPN 0x%06x, PSN 0x%06x, GID %s\n",
           my_dest.lid, my_dest.qpn, my_dest.psn, gid);


    rem_dest = pp_server_exch_dest((*ctx), ib_port, mtu, port, sl, &my_dest, gidx);

    if (!rem_dest)
        return 1;

    inet_ntop(AF_INET6, &rem_dest->gid, gid, sizeof gid);
    printf("  remote address: LID 0x%04x, QPN 0x%06x, PSN 0x%06x, GID %s\n",
           rem_dest->lid, rem_dest->qpn, rem_dest->psn, gid);

    return 0;
}