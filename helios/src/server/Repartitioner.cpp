#include "Repartitioner.hpp"
#include "omp.h"
#include <sys/syscall.h>
void Repartitioner::run(){
	printf("repartitioner_thread \n");
	printf(" thread lwpid = %u currentNode=%d\n", syscall(SYS_gettid),sid);
#pragma omp parallel for num_threads(num_servers)
	for(int i = 0; i < num_servers; i++){
//	int i = sid;
		std::cout << "sid=" << sid << "tgt_s:" << i << "Repartitioner::run" << std::endl;
		while(1){//queue()->endPrograms() != cfg->num_query_threads){
			sid_t obj = queue()->pop(i);
			if(obj == 0){
				//printf("No reassign task! wait...");
				//std::cout << "sid=" << sid << "No reassign task! wait..." << std::endl;
				sleep(10);
				continue;
			}
			//printf("data to partitioning %d\n", obj);
			int tgt_node = db[i]->reassign_evaluate(i, obj);
			if(tgt_node == i)
				continue;
			if(tgt_node == -1)// vertex is already reassigned
				continue;
			//printf("%d reassign %d to %d\n",i, obj, tgt_node);
			
			//int status = db[i]->reassign(i, obj, tgt_node);
			int status = db[i]->client_reassign(i, obj, tgt_node);
			std:cout << "status=" << status << std::endl;
			if(status == 0) //request lock fail
			{
				printf("%ld reassign lock fail\n", obj);
				//queue()->push(obj);
			}else if(status == -1){
				printf("Fatal error, %ld reassign error!\n", obj);
			}
			else if( status == -2){
				printf("Fatal error, %ld receive fails!\n", obj);
			}else if( status == -3){
				printf("Fatal error, %ld deduplicate fails!\n", obj);
			}else 
			{
				std::cout << "set_reaasignment_count begin" << std::endl;
				queue()->set_reaasignment_count(sid);
				std::cout << "set_reaasignment_count end" << std::endl;
				printf("Succeed: @%d %d reassign %ld to %d\n",sid, i, obj, tgt_node);
			}			
		}
	}
}

void Repartitioner::up_edge_weight(){
	printf("metadata_update_thread \n");
	printf(" thread lwpid = %u currentNode=%d\n", syscall(SYS_gettid),sid);
#pragma omp parallel for num_threads(num_servers)
	for(int i = 0; i < num_servers; i++){
	//int i = sid;
	std::cout << "sid=" << sid <<"tgt_s:"<< i << "Repartitioner::up_edge_weight begin" << std::endl;
		while(1){//queue()->endPrograms() != cfg->num_query_threads){
			WeightUpdateTask *task = queue()->pop_weight_task(i);
			if(task == NULL)
			{
				//printf("No update weight task! wait ... ");
				//std::cout << "sid" << sid << "No update weight task! wait ... " << std::endl;
				sleep(10);continue;
			}
			if(task->isUpdateEdge()){
				printf("begin update edgeweight\n");
				vector<triple_t> edges = task->getEdge();
				if(edges.size() > 0)
					db[i]->batch_up_edgeweight(i, task->getRoot_v(), edges);
				printf("end update edgeweight \n");
			}else{
				printf("begin update vertex weight\n");
				vector<sid_t> vs = task->getVertex();
				int incr_weight = task->getIncrWeight();
				if(vs.size() > 0){
					db[i]->batch_up_vertexweight(i, vs, incr_weight);
					queue()->push(i, vs);
				}
				printf("end update vertex weight\n");
			}
			delete task;

		}
	}
}

/*void Repartitioner::up_metadata_task(WeightUpdateTask* task){
	if(task == NULL)
	{
		return;
	}
	if(task->isUpdateEdge()){
		printf("begin update edgeweight\n");
		vector<triple_t> edges = task->getEdge();
		if(edges.size() > 0)
			db->batch_up_edgeweight(i, edges);
		printf("end update edgeweight \n");
	}else{
		printf("begin update vertex weight\n");
		vector<sid_t> vs = task->getVertex();
		int incr_weight = task->getIncrWeight();
		if(vs.size() > 0){
					db->batch_up_vertexweight(i, vs, incr_weight);
					queue()->push(i, vs);
				}
				printf("end update vertex weight\n");
			}
			delete task;

		}
	//}
}*/
