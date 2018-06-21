#ifndef __RDMA_H__
#define __RDMA_H__
#include <vector>
#include <string>
#include <pthread.h>
#define _ENABLE_READ_ 
#include <rdma/rdma_cma.h>

#define MAX_CONCURRENCY 16

typedef struct _data_list_
{
	char* data_ptr;
	struct _data_list_* next;
	uint32_t data_len;
} node_item;

typedef struct _rdma_pack_
{
	struct rdma_cm_id* rdma_id;
	node_item* nit;
} _rdma_thread_pack_;

enum message_id
{
	MSG_INVALID = 0,
	MSG_MR,
	MSG_READY,
	MSG_DONE
};


typedef struct _key_exchange_
{
	int id;
	uint64_t md5;
	struct _k_
	{
		uint64_t addr;
		uint32_t rkey;
	} key_info[MAX_CONCURRENCY], bitmap;

} _key_exch;

typedef struct _ack
{
	int index;
	uint64_t cost_time;
} _ack_;

struct context
{
	struct ibv_context *ibv_ctx;
	struct ibv_pd *pd;
	struct ibv_cq *cq;
	struct ibv_comp_channel *comp_channel;

	pthread_t cq_poller_thread;

	//register buffer for remote to write
	char *           buffer[MAX_CONCURRENCY];
	struct ibv_mr *  buffer_mr[MAX_CONCURRENCY];

	//register ack mem is used for write to remote
	_ack_*			 ack[MAX_CONCURRENCY];
	struct ibv_mr *  ack_mr[MAX_CONCURRENCY];

	//indicate current status of each peer exchange
	bool 			 is_busy[MAX_CONCURRENCY];
	// index 0: store for local as tx
	// index 1: used to recv the remote info
	_key_exch*       k_exch[2];
	struct ibv_mr*   k_exch_mr[2];

	/*store the peer addr and rkey*/
	uint64_t 		 peer_addr[MAX_CONCURRENCY];
	uint32_t 		 peer_rkey[MAX_CONCURRENCY];

#ifdef _ENABLE_READ_
	/**used for bitmap**/
	char *			 bitmap[2];
	struct ibv_mr*   bitmap_mr[2];/*expose index 0 to peer for reading*/
	uint64_t		 peer_bitmap_addr;
	uint32_t		 peer_bitmap_rkey;
#endif
};
#include <iostream>
struct Adapter
{
	unsigned short server_port;
	struct rdma_event_channel *event_channel;
	struct rdma_cm_id *listener;
	std::vector<rdma_cm_id*> recv_rdma_cm_id;

	std::string server_ip;
	std::string client_ip;
	Adapter()
	{
		server_port = 1234;
		server_ip = "192.168.1.1";
		client_ip = "192.168.1.2";
	}
	void set_server_ip(const char* _server_ip)
	{
		server_ip = _server_ip;
		std::cout << "Server IP : " << server_ip << std::endl;
	}
	void set_client_ip(const char* _client_ip)
	{
		client_ip = _client_ip;
		std::cout << "Client IP : " << client_ip << std::endl;
	}
};


#include <stdarg.h>
void log_info(const char *format, ...);
#endif