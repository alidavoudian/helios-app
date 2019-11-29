#ifndef REASSIGNQUEUE_HPP_
#define REASSIGNQUEUE_HPP_
#include <iostream>
#include <vector>
#include <set>
#include <pthread.h>
#include <tbb/concurrent_unordered_map.h>
#include "type.h"
#include "triple_t.hpp"
#include "WeightUpdateTask.hpp"

using namespace std;
class ReassignQueue{

	private:
		vector<set<sid_t>> objs;
		long long reassignment_count=0;
		int endProgram = 0;
		pthread_spinlock_t lock_reassign_count;
		pthread_spinlock_t* lock_queue;
		pthread_spinlock_t lock_status;

		vector<vector<WeightUpdateTask*>> weight_tasks;
		pthread_spinlock_t* tri_locks;
	public:
		ReassignQueue(int num_servers){
			pthread_spin_init(&lock_status, 0);
			//pthread_spin_init(&lock_queue, 0);
			//pthread_spin_init(&tri_locks, 0);
			pthread_spin_init(&lock_reassign_count, 0);

			weight_tasks.resize(num_servers);
			tri_locks = (pthread_spinlock_t*)malloc
				(sizeof(pthread_spinlock_t)*num_servers);
			for(int i = 0; i < num_servers; i++){
				pthread_spinlock_t l;
				pthread_spin_init(&l, 0);
				tri_locks[i] = l;
			}
			objs.resize(num_servers);
			lock_queue = (pthread_spinlock_t*)malloc
				(sizeof(pthread_spinlock_t)*num_servers);
			for(int i = 0; i < num_servers; i++){
				pthread_spinlock_t l;
				pthread_spin_init(&l, 0);
				lock_queue[i] = l;
			}

		}
		virtual ~ReassignQueue(){
			free((void*)lock_queue);
			free((void*)tri_locks);
		}

		void push(int sid, sid_t obj){
			pthread_spin_lock(&lock_queue[sid]);
			//objs.push_back(obj);
			objs[sid].insert(obj);
			pthread_spin_unlock(&lock_queue[sid]);
		}

		void push(int sid, vector<sid_t> obj){
			pthread_spin_lock(&lock_queue[sid]);
			for(int i = 0; i< obj.size(); i++)
				objs[sid].insert(obj[i]);
			//objs.insert(objs.end(),obj.begin(),obj.end());
			pthread_spin_unlock(&lock_queue[sid]);
		}

		void push_weight_task(int sid, WeightUpdateTask* task){
			pthread_spin_lock(&tri_locks[sid]);
			weight_tasks[sid].push_back(task);
			pthread_spin_unlock(&tri_locks[sid]);
		}
		WeightUpdateTask* pop_weight_task(int sid){
			WeightUpdateTask *task = NULL;
			pthread_spin_lock(&tri_locks[sid]);
			if(weight_tasks[sid].size() > 0){
				task = *weight_tasks[sid].begin();	
				weight_tasks[sid].erase(weight_tasks[sid].begin());
				if ((weight_tasks[sid].size()%10)==0)
				{
					std::cout <<"sid=" << sid <<  "weight_tasks=" << weight_tasks[sid].size() << std::endl;
				}
			}
			//vector<triple_t>().swap(edges_to_weight[sid]);
			pthread_spin_unlock(&tri_locks[sid]);
			return task;
		}

		sid_t pop(int sid){
			sid_t r = 0;
			pthread_spin_lock(&lock_queue[sid]);
			if(objs[sid].size() >0){	
				r = *(objs[sid]).begin();
				(objs[sid]).erase(objs[sid].begin());
				if (((objs[sid]).size()%10)==0)
				{
					std::cout << "sid=" << sid  << " objs.size=" << objs[sid].size() << std::endl;
				}
			}
			pthread_spin_unlock(&lock_queue[sid]);
			return r;		
		}

		void end1Program(){
			pthread_spin_lock(&lock_status);
			endProgram++;
			pthread_spin_unlock(&lock_status);
		}

		int endPrograms(){
			return endProgram;
		}

		bool isEmpty(){
			bool flag = true;
			for(int i = 0; i < objs.size(); i++){
				if(objs[i].size() >0 )
					flag = false;
			}
			return flag;
		}

		int set_reaasignment_count(int sid)
		{
			pthread_spin_lock(&lock_reassign_count);
			reassignment_count++;
			std::cout << "sid=" << sid << " reassignment_count=" << reassignment_count << std::endl;
			pthread_spin_unlock(&lock_reassign_count);
		}
};

#endif
