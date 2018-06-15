#include "rdma.h"

#define __RDMA_SLOW__ 1

#include <rdma/rdma_cma.h>

//50 M for default size;
const size_t BUFFER_SIZE = 5 * 1 * 1024 + 1;
#define TIMEOUT_IN_MS 500
#define TEST_NZ(x)                                               \
	do                                                           \
	{                                                            \
		if ((x))                                                 \
			rc_die("error: " #x " failed (returned non-zero)."); \
	} while (0)
#define TEST_Z(x)                                                 \
	do                                                            \
	{                                                             \
		if (!(x))                                                 \
			rc_die("error: " #x " failed (returned zero/null)."); \
	} while (0)
#define MIN_CQE 10

#include <string>
#include <vector>
#include <iostream>
#include <thread>
#include <atomic>
#include <unordered_map>

#include <cassert>
#include <cstdlib>
#include <cstring>
#include <cstdio>

#include <unistd.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <signal.h>
#include <errno.h>
#include <sys/time.h>
#include <stdlib.h>

#define IS_CLIENT false
#define IS_SERVER true

#define clip_bit(num, mask) ((num) & (~(1 << (7 - (mask))))) | ((num) ^ (1 << (7 - (mask))))

static void rc_die(const char *reason)
{
	extern int errno;
	fprintf(stderr, "%s\nstrerror= %s\n", reason, strerror(errno));
	exit(-1);
}

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

/*
12.12.11.XXX
*/
/*for read remote*/
static void post_send(struct rdma_cm_id *id, ibv_wr_opcode opcode)
{
	struct context *new_ctx = (struct context *)id->context;
	struct ibv_sge sge;
	struct ibv_send_wr sr, *bad_wr = NULL;

	/* prepare the scatter/gather entry */
	memset(&sge, 0, sizeof(sge));
	sge.addr = (uintptr_t)new_ctx->bitmap[1];
	sge.length = MAX_CONCURRENCY;
	sge.lkey = new_ctx->bitmap_mr[1]->lkey;

	/* prepare the send work request */
	memset(&sr, 0, sizeof(sr));
	sr.next = NULL;
	sr.wr_id = (uintptr_t)id;
	;
	sr.sg_list = &sge;
	sr.num_sge = 1;
	sr.opcode = opcode;
	sr.send_flags = IBV_SEND_SIGNALED;
	sr.wr.rdma.remote_addr = new_ctx->peer_bitmap_addr;
	sr.wr.rdma.rkey = new_ctx->peer_bitmap_rkey;
	/* there is a Receive Request in the responder side, so we won't get any into RNR flow */
	TEST_NZ(ibv_post_send(id->qp, &sr, &bad_wr));
}

static void _write_remote(struct rdma_cm_id *id, uint32_t len, uint32_t index, ibv_wr_opcode opcode)
{
	struct context *new_ctx = (struct context *)id->context;

	struct ibv_send_wr wr, *bad_wr = NULL;
	struct ibv_sge sge;

	memset(&wr, 0, sizeof(wr));

	//wr.wr_id = (uintptr_t)id;
	wr.wr_id = index;

	wr.opcode = opcode;
	wr.send_flags = IBV_SEND_SIGNALED;
	wr.imm_data = index; //htonl(index);
	wr.wr.rdma.remote_addr = new_ctx->peer_addr[index];
	wr.wr.rdma.rkey = new_ctx->peer_rkey[index];

	if (len)
	{
		wr.sg_list = &sge;
		wr.num_sge = 1;

		sge.addr = (uintptr_t)new_ctx->buffer[index];
		sge.length = len;
		sge.lkey = new_ctx->buffer_mr[index]->lkey;
	}

	TEST_NZ(ibv_post_send(id->qp, &wr, &bad_wr));
}

static void _post_receive(struct rdma_cm_id *id, uint32_t index)
{
	struct ibv_recv_wr wr, *bad_wr = NULL;
	memset(&wr, 0, sizeof(wr));
	wr.wr_id = (uint64_t)id;
	wr.sg_list = NULL;
	wr.num_sge = 0;
	TEST_NZ(ibv_post_recv(id->qp, &wr, &bad_wr));
}
static void _ack_remote(struct rdma_cm_id *id, uint32_t index)
{
	struct context *new_ctx = (struct context *)id->context;

	struct ibv_send_wr wr, *bad_wr = NULL;
	struct ibv_sge sge;

	memset(&wr, 0, sizeof(wr));

	wr.wr_id = (uintptr_t)id;

	wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
	wr.send_flags = IBV_SEND_SIGNALED;
	wr.imm_data = index; //htonl(index);
	wr.wr.rdma.remote_addr = new_ctx->peer_addr[index];
	wr.wr.rdma.rkey = new_ctx->peer_rkey[index];

	new_ctx->ack[index]->index = index;

	{
		wr.sg_list = &sge;
		wr.num_sge = 1;

		sge.addr = (uintptr_t)new_ctx->ack[index];
		sge.length = sizeof(_ack_);
		sge.lkey = new_ctx->ack_mr[index]->lkey;
	}

	TEST_NZ(ibv_post_send(id->qp, &wr, &bad_wr));
}

/*map 0 is used for peer read and bitmap 1 is used to store the peer info*/
static std::vector<int> recv_handle_bitmap(struct context *ctx)
{
	std::vector<int> available;
	for (int index = 0; index < MAX_CONCURRENCY; index++)
	{
		if (ctx->bitmap[0][index] != ctx->bitmap[1][index])
			available.push_back(index);
	}
	return available;

	// for (int index = 0; index < (MAX_CONCURRENCY+7)/8-1; index++)
	// {
	// 	unsigned char res = ctx->bitmap[0][index] ^ ctx->bitmap[1][index];
	// 	if (res == 0)continue;

	// 	for (int bit = 0; bit < 8; bit++)
	// 	{
	// 		if (res & (0x1 << (7-bit))) available.push_back(index * 8 + bit);
	// 	}
	// }
	// unsigned char res = ctx->bitmap[0][ MAX_CONCURRENCY/8-1 ] ^ ctx->bitmap[1][ MAX_CONCURRENCY/8-1];
	// for (int bit = 0; bit < MAX_CONCURRENCY%8; bit++)
	// 	{
	// 		if (res & (0x1 << (7-bit)))
	// 			available.push_back(MAX_CONCURRENCY+bit);

	// 	}

	// return available;
}

#define clip_bit(num, mask) ((num) & (~(1 << (7 - (mask))))) | ((num) ^ (1 << (7 - (mask))))

static void update_bitmap(struct context *ctx, int index)
{
	ctx->bitmap[0][index]=(ctx->bitmap[0][index]+1)%2;
	//ctx->bitmap[0][index / 8] = clip_bit(ctx->bitmap[0][index / 8], index % 8);
	return;
}


static void *concurrency_recv_by_RDMA(struct ibv_wc *wc, uint32_t &recv_len)
{
	struct rdma_cm_id *id = (struct rdma_cm_id *)(uintptr_t)wc->wr_id;
	struct context *ctx = (struct context *)id->context;
	void *_data = nullptr;

	switch (wc->opcode)
	{
	case IBV_WC_RECV_RDMA_WITH_IMM:
	{
		//log_info("recv with IBV_WC_RECV_RDMA_WITH_IMM\n");
		//log_info("imm_data is %d\n", wc->imm_data);
		//uint32_t size = ntohl(wc->imm_data);
		uint32_t index = wc->imm_data;
		uint32_t size = *((uint32_t *)(ctx->buffer[index]));
		char *recv_data_ptr = ctx->buffer[index] + sizeof(uint32_t);

		recv_len = size;
		_data = (void *)std::malloc(sizeof(char) * size);

		if (_data == nullptr)
		{
			printf("fatal error in recv data malloc!!!!\n");
			exit(-1);
		}
		std::memcpy(_data, recv_data_ptr, size);

		_post_receive(id, wc->imm_data);
		_ack_remote(id, wc->imm_data);
		log_info("recv data: %s\n", _data);
		break;
	}
	case IBV_WC_RECV:
	{
		if (MSG_MR == ctx->k_exch[1]->id)
		{
			log_info("recv MD5 is %llu\n", ctx->k_exch[1]->md5);
			log_info("imm_data is %d\n", wc->imm_data);
			for (int index = 0; index < MAX_CONCURRENCY; index++)
			{
				ctx->peer_addr[index] = ctx->k_exch[1]->key_info[index].addr;
				ctx->peer_rkey[index] = ctx->k_exch[1]->key_info[index].rkey;
				struct sockaddr_in *client_addr = (struct sockaddr_in *)rdma_get_peer_addr(id);
				printf("client[%s,%d] to ", inet_ntoa(client_addr->sin_addr), client_addr->sin_port);
				printf("server ack %d: %p  ", index, ctx->peer_addr[index]);
				printf("my buffer addr: %d %p\n", index, ctx->buffer_mr[index]->addr);
			}
			ctx->peer_bitmap_addr = ctx->k_exch[1]->bitmap.addr;
			ctx->peer_bitmap_rkey = ctx->k_exch[1]->bitmap.rkey;
			{
				printf("peer bitmap addr : %p\npeer bitmap rkey: %u\n", ctx->peer_bitmap_addr, ctx->peer_bitmap_rkey);
			}
			post_send(id, IBV_WR_RDMA_READ); //query peer bitmap for update
		}
		break;
	}
	case IBV_WC_RDMA_WRITE:
	{
		log_info("IBV_WC_RDMA_WRITE\n");
		break;
	}
	case IBV_WC_RDMA_READ:
	{
		log_info("IBV_WC_RDMA_READ peer message\n");
		printf("\nPeer bitmap\n");
		for(int index=0;index<MAX_CONCURRENCY;index++)
		{
			printf("%x ",ctx->bitmap[1][index]);
		}
		printf("\nLocal bitmap\n");
		for(int index=0;index<MAX_CONCURRENCY;index++)
		{
			printf("%x ",ctx->bitmap[0][index]);
		}
		printf("\n");
		std::vector<int> available = recv_handle_bitmap(ctx);
		if (available.size() == 0)
		{
			log_info("current pipline is busing sleep for next query, will sleep 1 seconds\n");
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
		}
		else
		{
			printf("\navailable data\n");
			for (auto &index : available)
			{
				//write_tensor(id, index);
				//send_tensor(id, index);
				//update_bitmap(ctx,wc->wr_id);
				std::cout<<" "<<index;
			}
			printf("\n");
			for (auto &index : available)
			{
				uint32_t size = *((uint32_t *)(ctx->buffer[index]));
				char *recv_data_ptr = ctx->buffer[index] + sizeof(uint32_t);
				void* _data = (void *)std::malloc(sizeof(char) * size + 1);
				
				if (_data == nullptr)
				{
					printf("fatal error in recv data malloc!!!!\n");
					exit(-1);
				}

				memset(_data,0,size + 1);
				std::memcpy(_data, recv_data_ptr, size);
				update_bitmap(ctx,index);
				log_info("Recv data: %s\n", _data);
				std::free((char*)_data);

			}
		}
		std::cout<<"\nsending thread will be blocked for 1 seconds"<<std::endl;
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
		post_send(id, IBV_WR_RDMA_READ); //query peer bitmap for update
		break;
	}
	case IBV_WC_SEND:
	{
		//log_info("IBV_WC_SEND\n");
		break;
	}
	default:
		break;
	}
	return _data;
}

static void *send_tensor(struct rdma_cm_id *id, uint32_t index)
{
	struct context *ctx = (struct context *)id->context;

	std::string msg = "Hello, World : index " + std::to_string(index);
	/*encode msg_length and buffer*/
	uint32_t msg_len = msg.length();

	if ((msg_len + sizeof(uint32_t)) > BUFFER_SIZE)
	{
		perror("fatal error, send msg length is too long\n");
		exit(-1);
	}

	char *_buff = ctx->buffer[index];
	std::memcpy(_buff, (char *)(&msg_len), sizeof(uint32_t));
	_buff += sizeof(uint32_t);
	std::memcpy(_buff, msg.c_str(), msg_len);
	_write_remote(id, msg_len + sizeof(uint32_t), index, IBV_WR_RDMA_WRITE_WITH_IMM);
	log_info("send data: %s\n",msg.c_str());
	return NULL;
}
#include <random>
 std::random_device rd;
static void * write_tensor(struct rdma_cm_id *id, uint32_t index)
{
	struct context *ctx = (struct context *)id->context;

	std::string msg = "Hello, World : index " + std::to_string(index) +", random : "+ std::to_string(rd());
	/*encode msg_length and buffer*/
	uint32_t msg_len = msg.length();

	if ((msg_len + sizeof(uint32_t)) > BUFFER_SIZE)
	{
		perror("fatal error, send msg length is too long\n");
		exit(-1);
	}

	char *_buff = ctx->buffer[index];
	std::memcpy(_buff, (char *)(&msg_len), sizeof(uint32_t));
	_buff += sizeof(uint32_t);
	std::memcpy(_buff, msg.c_str(), msg_len);
	_write_remote(id, msg_len + sizeof(uint32_t), index, IBV_WR_RDMA_WRITE);
	log_info("write data: %s\n",msg.c_str());
	return NULL;
}

static std::vector<int> send_handle_bitmap(struct context *ctx)
{
	std::vector<int> available;
	for (int index = 0; index < MAX_CONCURRENCY; index++)
	{
		if (ctx->bitmap[0][index] == ctx->bitmap[1][index])
			available.push_back(index);
	}
	return available;
	// std::vector<int> available;
	// bool empty = true;
	// for (int index = 0; index < (MAX_CONCURRENCY + 7 / 8) - 1; index++)
	// {
	// 	unsigned char res = ctx->bitmap[0][index] ^ ctx->bitmap[1][index];
	// 	if (res == 0Xff)
	// 		continue;
	// 	empty = false;

	// 	for (int bit = 0; bit < 8; bit++)
	// 	{
	// 		if (!(res & (0x1 << bit))) //相同,表明可写
	// 		{
	// 			ctx->bitmap[0][index] = (ctx->bitmap[0][index] & (~(0x1 << bit))) |
	// 									(~(ctx->bitmap[0][index] & (0x1 << bit)));
	// 			available.push_back(index * 8 + (8 - bit));
	// 		}
	// 	}
	// }
	// return available;
}


static void *concurrency_send_by_RDMA(struct rdma_cm_id *id, struct ibv_wc *wc, int &mem_used)
{
	//struct rdma_cm_id *id = (struct rdma_cm_id *)(uintptr_t)wc->wr_id;
	struct context *ctx = (struct context *)id->context;

	switch (wc->opcode)
	{
	case IBV_WC_RECV_RDMA_WITH_IMM:
	{
		//log_info("recv with IBV_WC_RECV_RDMA_WITH_IMM\n");
		//log_info("imm_data is %d\n", wc->imm_data);
		_post_receive(id, wc->imm_data);
		//send_tensor(id, wc->imm_data);
		break;
	}
	case IBV_WC_RECV:
	{//at start stage, read peer virtual memory info.
		if (MSG_MR == ctx->k_exch[1]->id)
		{
			log_info("recv MD5 is %llu\n", ctx->k_exch[1]->md5);
			for (int index = 0; index < MAX_CONCURRENCY; index++)
			{
				//reserved the (buffer)key info from server.
				ctx->peer_addr[index] = ctx->k_exch[1]->key_info[index].addr;
				ctx->peer_rkey[index] = ctx->k_exch[1]->key_info[index].rkey;
				struct sockaddr_in *client_addr = (struct sockaddr_in *)rdma_get_peer_addr(id);
				printf("server[%s,%d] to ", inet_ntoa(client_addr->sin_addr), client_addr->sin_port);
				printf("client buffer %d: %p\n", index, ctx->peer_addr[index]);
				printf("my ach addr: %d %p\n", index, ctx->ack_mr[index]->addr);
			}
			ctx->peer_bitmap_addr = ctx->k_exch[1]->bitmap.addr;
			ctx->peer_bitmap_rkey = ctx->k_exch[1]->bitmap.rkey;
			{
				printf("peer bitmap addr : %p\npeer bitmap rkey: %u\n", ctx->peer_bitmap_addr, ctx->peer_bitmap_rkey);
			}
			/**send one tensor...**/
			//send_tensor(id, 0);
			post_send(id, IBV_WR_RDMA_READ); //read from peer bitmap
			mem_used++;
		}
		break;
	}
	case IBV_WC_RDMA_WRITE:
	{
		log_info("IBV_WC_RDMA_WRITE SUCCESS with id = %u\n",wc->wr_id);
		update_bitmap(ctx, wc->wr_id);
		break;
	}
	case IBV_WC_RDMA_READ:
	{
		log_info("IBV_WC_RDMA_READ peer message\n");
		printf("\nPeer bitmap\n");
		for(int index=0;index<MAX_CONCURRENCY;index++)
		{
			printf("%x ",ctx->bitmap[1][index]);
		}
		printf("\nLocal bitmap\n");
		for(int index=0;index<MAX_CONCURRENCY;index++)
		{
			printf("%x ",ctx->bitmap[0][index]);
		}
		printf("\n");
		std::vector<int> available = send_handle_bitmap(ctx);
		if (available.size() == 0)
		{
			log_info("current pipline is busing sleep for next query, will sleep for 1 seconds\n");
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
		}
		else
		{
			printf("\navailable data\n");
			
			for (auto &index : available)
			{
				std::cout<<" "<<index;
			}
			printf("\n");
			for (auto &index : available)
			{
				write_tensor(id, index);
			}

		}

		std::cout<<"\nsending thread will be blocked for 1 seconds"<<std::endl;
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
		post_send(id, IBV_WR_RDMA_READ); //query peer bitmap for update
		break;
	}
	case IBV_WC_SEND:
	{
		//log_info("IBV_WC_SEND\n");
		break;
	}
	default:
		break;
	}
	return NULL;
}

static void *recv_poll_cq(void *_id)
{
	struct ibv_cq *cq = NULL;
	struct ibv_wc wc[MAX_CONCURRENCY * 2];
	struct rdma_cm_id *id = (rdma_cm_id *)_id;

	struct context *ctx = (struct context *)id->context;
	void *ev_ctx = NULL;

	while (true)
	{
		TEST_NZ(ibv_get_cq_event(ctx->comp_channel, &cq, &ev_ctx));
		ibv_ack_cq_events(cq, 1);
		TEST_NZ(ibv_req_notify_cq(cq, 0));

		int wc_num = ibv_poll_cq(cq, MAX_CONCURRENCY * 2, wc);

		for (int index = 0; index < wc_num; index++)
		{
			if (wc[index].status == IBV_WC_SUCCESS)
			{
				/*****here to modified recv* wc---->wc[index]****/
				//printf("in receive poll cq\n");
				void *recv_data = nullptr;
				uint32_t recv_len;
				recv_data = concurrency_recv_by_RDMA(&wc[index], recv_len);
			}
			else
			{
				printf("\nwc = %s\n", ibv_wc_status_str(wc[index].status));
				rc_die("poll_cq: status is not IBV_WC_SUCCESS");
			}
		}
	}
	return NULL;
}
static void *send_poll_cq(void *_id)
{
	struct ibv_cq *cq = NULL;
	struct ibv_wc wc[MAX_CONCURRENCY * 2];
	struct rdma_cm_id *id = (rdma_cm_id *)_id;

	struct context *ctx = (struct context *)id->context;
	void *ev_ctx = NULL;

	int mem_used = 0;

	while (1)
	{
		TEST_NZ(ibv_get_cq_event(ctx->comp_channel, &cq, &ev_ctx));
		ibv_ack_cq_events(cq, 1);
		TEST_NZ(ibv_req_notify_cq(cq, 0));

		int wc_num = ibv_poll_cq(cq, MAX_CONCURRENCY * 2, wc);

		if (wc_num < 0)
		{
			perror("fatal error in ibv_poll_cq, -1");
			exit(-1);
		}

		for (int index = 0; index < wc_num; index++)
		{
			if (wc[index].status == IBV_WC_SUCCESS)
			{
				concurrency_send_by_RDMA(id, &wc[index], mem_used);
			}
			else
			{
				printf("\nwc = %s\n", ibv_wc_status_str(wc[index].status));
				rc_die("poll_cq: status is not IBV_WC_SUCCESS");
			}
		}
		if (mem_used && 0)
		{
			printf("mem_used : %d\n", mem_used);
			//struct rdma_cm_id *id = (struct rdma_cm_id *)((wc[index])->wr_id);
			//struct context *ctx = (struct context *)id->context;
			for (mem_used; mem_used < MAX_CONCURRENCY; mem_used++)
			{
				send_tensor(id, mem_used);
			} /*send used next buffer*/
		}
	}
	return NULL;
}

static struct ibv_pd *rc_get_pd(struct rdma_cm_id *id)
{
	struct context *ctx = (struct context *)id->context;
	return ctx->pd;
}

static void _build_params(struct rdma_conn_param *params)
{
	memset(params, 0, sizeof(*params));
	params->initiator_depth = params->responder_resources = 1;
	params->rnr_retry_count = 7; /* infinite retry */
	params->retry_count = 7;
}

static void _build_context(struct rdma_cm_id *id, bool is_server)
{
	struct context *s_ctx = (struct context *)malloc(sizeof(struct context));
	s_ctx->ibv_ctx = id->verbs;
	TEST_Z(s_ctx->pd = ibv_alloc_pd(s_ctx->ibv_ctx));
	TEST_Z(s_ctx->comp_channel = ibv_create_comp_channel(s_ctx->ibv_ctx));
	TEST_Z(s_ctx->cq = ibv_create_cq(s_ctx->ibv_ctx, MAX_CONCURRENCY * 2 + 10, NULL, s_ctx->comp_channel, 0));
	TEST_NZ(ibv_req_notify_cq(s_ctx->cq, 0));
	id->context = (void *)s_ctx;
	if (is_server)
	{
		TEST_NZ(pthread_create(&s_ctx->cq_poller_thread, NULL, recv_poll_cq, (void *)id));
		id->context = (void *)s_ctx;
	}
}

static void _build_qp_attr(struct ibv_qp_init_attr *qp_attr, struct rdma_cm_id *id)
{
	struct context *ctx = (struct context *)id->context;
	memset(qp_attr, 0, sizeof(*qp_attr));
	qp_attr->send_cq = ctx->cq;
	qp_attr->recv_cq = ctx->cq;
	qp_attr->qp_type = IBV_QPT_RC;

	qp_attr->cap.max_send_wr = MAX_CONCURRENCY + 2 + 1;
	qp_attr->cap.max_recv_wr = MAX_CONCURRENCY + 2 + 1;
	qp_attr->cap.max_send_sge = 1;
	qp_attr->cap.max_recv_sge = 1;
}

static void _build_connection(struct rdma_cm_id *id, bool is_server)
{
	struct ibv_qp_init_attr qp_attr;
	_build_context(id, is_server);
	_build_qp_attr(&qp_attr, id);
	struct context *ctx = (struct context *)id->context;
	TEST_NZ(rdma_create_qp(id, ctx->pd, &qp_attr));
}

static void _on_pre_conn(struct rdma_cm_id *id)
{
	struct context *new_ctx = (struct context *)id->context;

	for (int index = 0; index < MAX_CONCURRENCY; index++)
	{
		posix_memalign((void **)(&(new_ctx->buffer[index])), sysconf(_SC_PAGESIZE), BUFFER_SIZE);
		TEST_Z(new_ctx->buffer_mr[index] = ibv_reg_mr(rc_get_pd(id), new_ctx->buffer[index], BUFFER_SIZE, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE));
		//printf("buffer %d :%p\n", index, new_ctx->buffer_mr[index]->addr);
		posix_memalign((void **)(&(new_ctx->ack[index])), sysconf(_SC_PAGESIZE), sizeof(_ack_));
		TEST_Z(new_ctx->ack_mr[index] = ibv_reg_mr(rc_get_pd(id), new_ctx->ack[index],
												   sizeof(_ack_), IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE));
		//printf("ack %d :%p\n", index, new_ctx->ack_mr[index]->addr);
	}
	log_info("register %d tx_buffer and rx_ack\n", MAX_CONCURRENCY);

	{
		posix_memalign((void **)(&(new_ctx->k_exch[0])), sysconf(_SC_PAGESIZE), sizeof(_key_exch));
		TEST_Z(new_ctx->k_exch_mr[0] = ibv_reg_mr(rc_get_pd(id), new_ctx->k_exch[0], sizeof(_key_exch), IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE));

		posix_memalign((void **)(&(new_ctx->k_exch[1])), sysconf(_SC_PAGESIZE), sizeof(_key_exch));
		TEST_Z(new_ctx->k_exch_mr[1] = ibv_reg_mr(rc_get_pd(id), new_ctx->k_exch[1], sizeof(_key_exch), IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE));
	}
	log_info("register rx_k_exch (index:0) and tx_k_exch (index:1)\n");

#ifdef _ENABLE_READ_
	for (int index = 0; index < 2; index++)
	{
		posix_memalign((void **)(&(new_ctx->bitmap[index])), sysconf(_SC_PAGESIZE), MAX_CONCURRENCY);
		TEST_Z(new_ctx->bitmap_mr[index] = ibv_reg_mr(rc_get_pd(id),
													  new_ctx->bitmap[index],
													  MAX_CONCURRENCY,
													  IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ));
		printf("bitmap %d :%p\n", index, new_ctx->bitmap_mr[index]->addr);
	}
	log_info("register bitmap (index:0 for remote read) and (index:1 for receive the peer data)\n");
	//memcpy(new_ctx->bitmap[0],"abc\0",MAX_CONCURRENCY);
	//memcpy(new_ctx->bitmap[1],"000\0",MAX_CONCURRENCY);
	memset(new_ctx->bitmap[0],0,MAX_CONCURRENCY);
	memset(new_ctx->bitmap[1],0,MAX_CONCURRENCY);
#endif
	struct ibv_recv_wr wr, *bad_wr = NULL;
	struct ibv_sge sge;

	memset(&wr, 0, sizeof(wr));

	wr.wr_id = (uintptr_t)id;
	wr.sg_list = &sge;
	wr.num_sge = 1;

	sge.addr = (uintptr_t)(new_ctx->k_exch[1]);
	sge.length = sizeof(_key_exch);
	sge.lkey = new_ctx->k_exch_mr[1]->lkey;

	TEST_NZ(ibv_post_recv(id->qp, &wr, &bad_wr));

	for (uint32_t index = 0; index < MAX_CONCURRENCY; index++)
	{
		//log_info("post recv index : %u\n", index);
		_post_receive(id, index);
	}
}

/**server on connection***/
static void _on_connection(struct rdma_cm_id *id, bool is_server)
{
	struct context *new_ctx = (struct context *)id->context;

	int index = 0;

	new_ctx->k_exch[0]->id = MSG_MR;
	if (is_server)
		new_ctx->k_exch[0]->md5 = 6666;
	else
		new_ctx->k_exch[0]->md5 = 5555;

	log_info("k_exch md5 is %llu\n", new_ctx->k_exch[0]->md5);
	if (is_server)
	{
		for (index = 0; index < MAX_CONCURRENCY; index++)
		{
			new_ctx->k_exch[0]->key_info[index].addr = (uintptr_t)(new_ctx->buffer_mr[index]->addr);
			new_ctx->k_exch[0]->key_info[index].rkey = (new_ctx->buffer_mr[index]->rkey);
		}
	}
	else
	{
		for (index = 0; index < MAX_CONCURRENCY; index++)
		{
			new_ctx->k_exch[0]->key_info[index].addr = (uintptr_t)(new_ctx->ack_mr[index]->addr);
			new_ctx->k_exch[0]->key_info[index].rkey = (new_ctx->ack_mr[index]->rkey);
		}
	}
	new_ctx->k_exch[0]->bitmap.addr = (uintptr_t)(new_ctx->bitmap_mr[0]->addr);
	new_ctx->k_exch[0]->bitmap.rkey = new_ctx->bitmap_mr[0]->rkey;

	//send to myself info to peer
	{
		struct ibv_send_wr wr, *bad_wr = NULL;
		struct ibv_sge sge;

		memset(&wr, 0, sizeof(wr));

		wr.wr_id = (uintptr_t)id;
		wr.opcode = IBV_WR_SEND;
		wr.sg_list = &sge;
		wr.num_sge = 1;
		wr.send_flags = IBV_SEND_SIGNALED;

		sge.addr = (uintptr_t)(new_ctx->k_exch[0]);
		sge.length = sizeof(_key_exch);
		sge.lkey = new_ctx->k_exch_mr[0]->lkey;

		TEST_NZ(ibv_post_send(id->qp, &wr, &bad_wr));
	}
	log_info("share my registed mem rx_buffer for peer write to\n");
}

static void _on_disconnect(struct rdma_cm_id *id)
{
	struct context *new_ctx = (struct context *)id->context;
	for (int index = 0; index < MAX_CONCURRENCY; index++)
	{
		ibv_dereg_mr(new_ctx->buffer_mr[index]);
		ibv_dereg_mr(new_ctx->ack_mr[index]);
		free(new_ctx->buffer[index]);
		free(new_ctx->ack[index]);
	}
	{
		ibv_dereg_mr(new_ctx->k_exch_mr[0]);
		ibv_dereg_mr(new_ctx->k_exch_mr[1]);
		free(new_ctx->k_exch[0]);
		free(new_ctx->k_exch[1]);
	}
	free(new_ctx);
}

static void recv_RDMA(Adapter &rdma_adapter)
{
	struct rdma_cm_event *event = NULL;
	struct rdma_conn_param cm_params;
	int connecting_client_cnt = 0;
	int client_counts = 1;
	printf("server is inited done (RDMA), waiting for %d client connecting....:)\n", client_counts);
	_build_params(&cm_params);

	while (rdma_get_cm_event(rdma_adapter.event_channel, &event) == 0)
	{
		struct rdma_cm_event event_copy;

		memcpy(&event_copy, event, sizeof(*event));
		rdma_ack_cm_event(event);

		if (event_copy.event == RDMA_CM_EVENT_CONNECT_REQUEST)
		{
			_build_connection(event_copy.id, IS_SERVER);
			_on_pre_conn(event_copy.id);
			TEST_NZ(rdma_accept(event_copy.id, &cm_params));
		}
		else if (event_copy.event == RDMA_CM_EVENT_ESTABLISHED)
		{
			_on_connection(event_copy.id, true);
			rdma_adapter.recv_rdma_cm_id.push_back(event_copy.id);

			struct sockaddr_in *client_addr = (struct sockaddr_in *)rdma_get_peer_addr(event_copy.id);
			printf("client[%s,%d] is connecting (RDMA) now... \n", inet_ntoa(client_addr->sin_addr), client_addr->sin_port);
			connecting_client_cnt++;
			if (connecting_client_cnt == client_counts)
				break;
		}
		else if (event_copy.event == RDMA_CM_EVENT_DISCONNECTED)
		{
			rdma_destroy_qp(event_copy.id);
			_on_disconnect(event_copy.id);
			rdma_destroy_id(event_copy.id);
			connecting_client_cnt--;
			if (connecting_client_cnt == 0)
				break;
		}
		else
		{
			rc_die("unknown event server\n");
		}
	}
	printf("%d clients have connected to my node (RDMA), ready to receiving loops\n", client_counts);

	while (1)
	{
		std::this_thread::sleep_for(std::chrono::seconds(10));
	}

	printf("RDMA recv loops exit now...\n");
	return;
}

static void rdma_server_init(Adapter &rdma_adapter)
{
	int init_loops = 0;
	struct sockaddr_in sin;
	printf("init a server (RDMA)....\n");
	memset(&sin, 0, sizeof(sin));
	sin.sin_family = AF_INET;						/*ipv4*/
	sin.sin_port = htons(rdma_adapter.server_port); /*server listen public ports*/
	sin.sin_addr.s_addr = INADDR_ANY;				/*listen any connects*/

	TEST_Z(rdma_adapter.event_channel = rdma_create_event_channel());
	TEST_NZ(rdma_create_id(rdma_adapter.event_channel, &rdma_adapter.listener, NULL, RDMA_PS_TCP));

	while (rdma_bind_addr(rdma_adapter.listener, (struct sockaddr *)&sin))
	{
		std::cerr << "server init failed (RDMA): error in bind socket, will try it again in 2 seconds..." << std::endl;
		if (init_loops > 10)
		{
			rdma_destroy_id(rdma_adapter.listener);
			rdma_destroy_event_channel(rdma_adapter.event_channel);
			exit(-1);
		}
		std::this_thread::sleep_for(std::chrono::seconds(2));
		init_loops++;
	}

	int client_counts = 1;
	if (rdma_listen(rdma_adapter.listener, client_counts))
	{
		std::cerr << "server init failed (RDMA): error in server listening" << std::endl;
		rdma_destroy_id(rdma_adapter.listener);
		rdma_destroy_event_channel(rdma_adapter.event_channel);
		exit(-1);
	}
	recv_RDMA(rdma_adapter);

	std::this_thread::sleep_for(std::chrono::seconds(1));
	return;
}

static void rdma_client_init(Adapter &rdma_adapter)
{
	std::cout << "client inited (RDMA) start" << std::endl;
	for (size_t index = 0; index < 1; index++)
	{
		struct rdma_cm_id *conn = NULL;
		struct rdma_event_channel *ec = NULL;
		std::string local_eth = rdma_adapter.client_ip; /*get each lev ip*/
		struct sockaddr_in ser_in, local_in;			/*server ip and local ip*/
		int connect_count = 0;
		memset(&ser_in, 0, sizeof(ser_in));
		memset(&local_in, 0, sizeof(local_in));

		/*bind remote socket*/
		ser_in.sin_family = AF_INET;
		ser_in.sin_port = htons(rdma_adapter.server_port); /*connect to public port remote*/
		inet_pton(AF_INET, rdma_adapter.server_ip.c_str(), &ser_in.sin_addr);

		/*bind local part*/
		local_in.sin_family = AF_INET;
		std::cout << local_eth.c_str() << "----->" << rdma_adapter.server_ip.c_str() << std::endl;
		inet_pton(AF_INET, local_eth.c_str(), &local_in.sin_addr);

		TEST_Z(ec = rdma_create_event_channel());
		TEST_NZ(rdma_create_id(ec, &conn, NULL, RDMA_PS_TCP));
		TEST_NZ(rdma_resolve_addr(conn, (struct sockaddr *)(&local_in), (struct sockaddr *)(&ser_in), TIMEOUT_IN_MS));

		struct rdma_cm_event *event = NULL;
		struct rdma_conn_param cm_params;

		_build_params(&cm_params);
		while (rdma_get_cm_event(ec, &event) == 0)
		{
			struct rdma_cm_event event_copy;
			memcpy(&event_copy, event, sizeof(*event));
			rdma_ack_cm_event(event);
			if (event_copy.event == RDMA_CM_EVENT_ADDR_RESOLVED)
			{
				_build_connection(event_copy.id, IS_CLIENT);
				_on_pre_conn(event_copy.id);
				TEST_NZ(rdma_resolve_route(event_copy.id, TIMEOUT_IN_MS));
			}
			else if (event_copy.event == RDMA_CM_EVENT_ROUTE_RESOLVED)
			{
				TEST_NZ(rdma_connect(event_copy.id, &cm_params));
			}
			else if (event_copy.event == RDMA_CM_EVENT_ESTABLISHED)
			{
				struct context *ctx = (struct context *)event_copy.id->context;
				TEST_NZ(pthread_create(&ctx->cq_poller_thread, NULL, send_poll_cq, (void *)(event_copy.id)));
				std::cout << local_eth << " has connected to server[ " << rdma_adapter.server_ip << " , " << rdma_adapter.server_port << " ]" << std::endl;

				{
					_on_connection(event_copy.id, false);
				}
				break;
			}
			else if (event_copy.event == RDMA_CM_EVENT_REJECTED)
			{
				std::this_thread::sleep_for(std::chrono::milliseconds(100));
				connect_count++;
				_on_disconnect(event_copy.id);
				rdma_destroy_qp(event_copy.id);
				rdma_destroy_id(event_copy.id);
				rdma_destroy_event_channel(ec);
				if (connect_count > 10 * 600) /*after 600 seconds, it will exit.*/
				{
					std::cerr << 600 << "seconds is passed, error in connect to server" << rdma_adapter.server_ip << ", check your network condition" << std::endl;
					exit(-1);
				}
				else
				{
					TEST_Z(ec = rdma_create_event_channel());
					TEST_NZ(rdma_create_id(ec, &conn, NULL, RDMA_PS_TCP));
					TEST_NZ(rdma_resolve_addr(conn, (struct sockaddr *)(&local_in), (struct sockaddr *)(&ser_in), TIMEOUT_IN_MS));
				}
			}
			else
			{
				printf("event = %d\n", event_copy.event);
				rc_die("unknown event client\n");
			}
		}
	}
	std::cout << "client inited done" << std::endl;
	while (1)
	{
		std::cout << "main thread sleep for 10 seconds" << std::endl;
		std::this_thread::sleep_for(std::chrono::seconds(10));
	}
}

void help(void)
{
	std::cout << "Useage:\n";
	std::cout << "For Server: ./rdma --server server_ip\n";
	std::cout << "For Client: ./rdma --server server_ip --client client_ip" << std::endl;
	return;
}

int main(int argc, char const *argv[])
{
	Adapter rdma_adapter;
	switch (argc)
	{
	case 3:
		rdma_adapter.set_server_ip(argv[2]);
		rdma_server_init(rdma_adapter);
	case 5:
		rdma_adapter.set_server_ip(argv[2]);
		rdma_adapter.set_client_ip(argv[4]);
		rdma_client_init(rdma_adapter);
	default:
		help();
		exit(-1);
		break;
	}
	return 0;
}