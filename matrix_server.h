#ifndef MATRIX_SERVER_H_
#define MATRIX_SERVER_H_

#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <cstring>
#include <sstream>
#include <math.h>
#include <errno.h>
#include <algorithm>
#include "cpp_zhtclient.h"
#include "matrix_client.h"
#include "matrix_util.h"
#include <queue>
#include <deque>
#include <vector>
#include <map>

#define SIXTY_KILOBYTES 61440
#define STRING_THRESHOLD SIXTY_KILOBYTES

typedef deque<TaskQueue_Item*> TaskQueue;
typedef deque<string> NodeList;
typedef pair<string, vector<string> > ComPair;
typedef deque<ComPair> CompQueue;

//typedef list<TaskQueue_Item*> TaskQueue;
//typedef list<string> NodeList;

using namespace std;

class Worker {
public:

	Worker();
	Worker(char *parameters[], NoVoHT *novoht);
	virtual ~Worker();

	string ip;

	int num_nodes;		// Number of computing nodes of Matrix
	int num_cores;		// Number of cores of a node
	int num_idle_cores;	// Number of idle cores of a node
	char neigh_mode;	// neighbors selection mechanism: 'static' or 'dynamic random'
	int num_neigh;		// Number of neighbors of a node
	int *neigh_index;	// Array to hold neighbors index for which to poll load
	int max_loaded_node;	// Neighbor index from which to steal task
	int selfIndex;		// Self Index in the membership list
	long poll_interval;	// The poll interval value for work stealing
	long poll_threshold;// Threshold beyond which polling interval should not double
	ZHTClient svrclient;	// for server to server communication
	struct coreInfo
	{
		int index;
		int cores_available;
	};
	
	int recv_tasks();
	int32_t get_max_load();
	void choose_neigh();
	int steal_task();
	int32_t get_load_info();
	int32_t get_monitoring_info();
	int32_t get_numtasks_to_steal();
	int get_complete_queue_info();
	
	int check_if_task_is_ready(string key);
	int move_task_to_ready_queue(TaskQueue_Item **qi);
	int insert_task_to_ready_queue(TaskQueue_Item **qi);

	int notify(ComPair &compair);

	int zht_ins_mul(Package &package);

	string zht_lookup(string key);
	int zht_insert(string str);
	int zht_remove(string key);
	int zht_append(string str);
	int zht_update(map<uint32_t, NodeList> &update_map, string field, uint32_t toid);

	int update(Package &package);
	int update_nodehistory(uint32_t currnode, string alltasks);
	int update_numwait(string alltasks);

	map<uint32_t, NodeList> get_map(TaskQueue &queue);
	map<uint32_t, NodeList> get_map(vector<string> &mqueue);
	
	//Number of cores information
	int32_t get_idle_core_info();
	void get_idle_cores_for_neighbors(struct coreInfo idle_cores_info_for_all_nodes[],int num_hpc_neigh);
	void choose_neighbor_for_hpc(int num_hpc_neigh);
	int steal_resources(int num_of_cores,Package recv_pkg,int num_hpc_neigh);
	void* Execute_HPC(void *);
	void Receive_HPC_Task(Package migratePackage);
	void Release_Resource();
	void InsertMap(Package executed_package);
	void Send_HPC_Results(vector< vector<string> > tokenize_string);
	int checkForSufficientCores(struct coreInfo idle_cores_info_for_all_nodes[],int num_of_cores,struct coreInfo selected_idle_cores[],int &num_selected_neigh,int &num_hpc_neigh);
	int *hpc_neigh_index;
	int hpc_num_neigh;
};

extern Worker *worker;
extern int ON;

extern ofstream fin_fp;
extern ofstream log_fp;
extern long msg_count[10];

extern long task_comp_count;

//extern queue<string*> insertq;
extern queue<string*> waitq;
extern queue<string*> insertq_new;
extern queue<int*> migrateq;
extern bitvec migratev;

extern queue<string*> notifyq;

//extern pthread_mutex_t iq_lock;
extern pthread_mutex_t waitq_lock;
extern pthread_mutex_t iq_new_lock;
extern pthread_mutex_t mq_lock;
extern pthread_mutex_t notq_lock;

//void work_steal_init(char *parameters[], NoVoHT *pmap);
void* worksteal(void* args);
void* check_wait_queue(void* args);
void* check_ready_queue(void* args);
void* check_ready_queue_for_migrated_tasks(void* args);

void* check_complete_queue(void* args);

void* HB_insertQ(void* args);
void* HB_insertQ_new(void* args);
void* HB_localinsertQ(void* args);
void* migrateTasks(void* args);
void* notQueue(void* args);

vector< vector<string> > tokenize(string input, char delim1, char delim2, int &num_vector, int &per_vector_count);

extern pthread_attr_t attr;
#endif 
