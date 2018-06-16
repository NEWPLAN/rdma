#include <fcntl.h>
#include <libgen.h>

#include "common.h"
#include "messages.h"
#include <time.h>
#include <sys/time.h>
#include <stdarg.h>

struct client_context
{
	char *buffer;
	struct ibv_mr *buffer_mr;

	struct message *msg;
	struct ibv_mr *msg_mr;

	uint64_t peer_addr;
	uint32_t peer_rkey;

	//int fd;
	const char *file_name;
};

void log_info(const char *format, ...)
{
	char now_time[32];
	char s[1024];
	char content[1024];
	//char *ptr = content;
	struct tm *tmnow;
	struct timeval tv;
	bzero(content, 1024);
	va_list arg;
	va_start(arg, format);
	vsprintf(s, format, arg);
	va_end(arg);

	gettimeofday(&tv, NULL);
	tmnow = localtime(&tv.tv_sec);

	sprintf(now_time, "%04d/%02d/%02d %02d:%02d:%02d:%06ld ",
	        tmnow->tm_year + 1900, tmnow->tm_mon + 1, tmnow->tm_mday, tmnow->tm_hour,
	        tmnow->tm_min, tmnow->tm_sec, tv.tv_usec);

	sprintf(content, "%s %s", now_time, s);
	printf("%s", content);
}

static void write_remote(struct rdma_cm_id *id, uint32_t len)
{
	struct client_context *ctx = (struct client_context *)id->context;

	struct ibv_send_wr wr, *bad_wr = NULL;
	struct ibv_sge sge;

	memset(&wr, 0, sizeof(wr));

	wr.wr_id = (uintptr_t)id;
	wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
	wr.send_flags = IBV_SEND_SIGNALED;
	wr.imm_data = htonl(len);
	wr.wr.rdma.remote_addr = ctx->peer_addr;
	wr.wr.rdma.rkey = ctx->peer_rkey;

	if (len)
	{
		wr.sg_list = &sge;
		wr.num_sge = 1;

		sge.addr = (uintptr_t)ctx->buffer;
		sge.length = len;
		sge.lkey = ctx->buffer_mr->lkey;
	}

	TEST_NZ(ibv_post_send(id->qp, &wr, &bad_wr));
}

static void post_receive(struct rdma_cm_id *id)
{
	struct client_context *ctx = (struct client_context *)id->context;

	struct ibv_recv_wr wr, *bad_wr = NULL;
	struct ibv_sge sge;

	memset(&wr, 0, sizeof(wr));

	wr.wr_id = (uintptr_t)id;
	wr.sg_list = &sge;
	wr.num_sge = 1;

	sge.addr = (uintptr_t)ctx->msg;
	sge.length = sizeof(*ctx->msg);
	sge.lkey = ctx->msg_mr->lkey;

	TEST_NZ(ibv_post_recv(id->qp, &wr, &bad_wr));
}

static void send_next_chunk(struct rdma_cm_id *id)
{
	struct client_context *ctx = (struct client_context *)id->context;

	ssize_t size = BUFFER_SIZE;

	//size = read(ctx->fd, ctx->buffer, BUFFER_SIZE);

	if (size == -1)
		rc_die("read() failed\n");

	write_remote(id, size);
}

static void send_file_name(struct rdma_cm_id *id)
{
	struct client_context *ctx = (struct client_context *)id->context;

	strcpy(ctx->buffer, ctx->file_name);

	write_remote(id, strlen(ctx->file_name) + 1);
}

static void on_pre_conn(struct rdma_cm_id *id)
{
	struct client_context *ctx = (struct client_context *)id->context;

	posix_memalign((void **)&ctx->buffer, sysconf(_SC_PAGESIZE), BUFFER_SIZE);
	TEST_Z(ctx->buffer_mr = ibv_reg_mr(rc_get_pd(), ctx->buffer, BUFFER_SIZE, IBV_ACCESS_LOCAL_WRITE));

	posix_memalign((void **)&ctx->msg, sysconf(_SC_PAGESIZE), sizeof(*ctx->msg));
	TEST_Z(ctx->msg_mr = ibv_reg_mr(rc_get_pd(), ctx->msg, sizeof(*ctx->msg), IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE));

	post_receive(id);
}
int first = 1;
struct timeval start_, now_;
static void on_completion(struct ibv_wc *wc)
{
	struct rdma_cm_id *id = (struct rdma_cm_id *)(uintptr_t)(wc->wr_id);
	struct client_context *ctx = (struct client_context *)id->context;

	if (wc->opcode & IBV_WC_RECV)
	{
		if (ctx->msg->id == MSG_MR)
		{
			ctx->peer_addr = ctx->msg->data.mr.addr;
			ctx->peer_rkey = ctx->msg->data.mr.rkey;

			printf("received MR, sending file name\n");
			send_file_name(id);
		}
		else if (ctx->msg->id == MSG_READY)
		{
			//printf("received READY, sending chunk\n");
			send_next_chunk(id);
			{
				static long long count = 0;
				if (first)
				{
					gettimeofday(&start_, NULL);
					//tstart = clock();
					first = 0;
				}
				if ((++count) % 1000 == 0)
				{
#define netbyte 1000
					//clock_t tend = clock();
					gettimeofday(&now_, NULL);
					//float time_cost = (tend - tstart) / CLOCKS_PER_SEC;
					float time_cost = (now_.tv_usec - start_.tv_usec) / 1000000.0 + now_.tv_sec - start_.tv_sec;
					printf("time cost: %f s, count = %ll\n", time_cost, count);
					log_info("rate: %f bps, %f Kbps, %f Mbps, %f Gbps\n",
					         8.0 * BUFFER_SIZE * count / time_cost,
					         8.0 * BUFFER_SIZE * count / netbyte / time_cost,
					         8.0 * BUFFER_SIZE * count / netbyte / netbyte / time_cost,
					         8.0 * BUFFER_SIZE * count / netbyte / netbyte / netbyte / time_cost
					        );
				}
			}
		}
		else if (ctx->msg->id == MSG_DONE)
		{
			printf("received DONE, disconnecting\n");
			rc_disconnect(id);
			return;
		}

		post_receive(id);
	}
}

int main(int argc, char **argv)
{
	struct client_context ctx;

	if (argc != 3)
	{
		fprintf(stderr, "usage: %s <server-address> <file-name>\n", argv[0]);
		return 1;
	}

	ctx.file_name = basename(argv[2]);
	/*
	ctx.fd = open(argv[2], O_RDONLY);

	if (ctx.fd == -1) {
	  fprintf(stderr, "unable to open input file \"%s\"\n", ctx.file_name);
	  return 1;
	}
	*/

	rc_init(
	    on_pre_conn,
	    NULL, // on connect
	    on_completion,
	    NULL); // on disconnect

	rc_client_loop(argv[1], DEFAULT_PORT, &ctx);

	//close(ctx.fd);

	return 0;
}

