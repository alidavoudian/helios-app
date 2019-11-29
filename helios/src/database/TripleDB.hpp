#ifndef TRIPLEDB_HPP_
#define TRIPLEDB_HPP_

#include "RedisStore.hpp"
/*
 * TripleStore includes:
 * 	triple info:  
 * 		vpd-v, vd-p.
 * 	v-related meta data:
 * 		v+COUNTER - weight & edgenum  (Redis HashMap v+COUNTER - 0|1 - num)
 *		workload/topology locality degree(HashMap):
 *			v - Exists/TOP/WORK - #
 *	
 *	Edge_log: 
 *		vid_i - vid - weight (HashMap)
 *		MRA bucket "EdgeList - vid_vid_i" (List)
 * 
 * Local TripleStore keeps vertex-related info that are reassigned with vertex.
 * 
 * */

class TripleDB : public RedisStore{
	private:
		const int WEIGHT = 0, EDGENUM = 1;
		const int TOP = 0, WORKLOAD= 1, EXISTS = 2;

	public:
		TripleDB(const char* host, int port)
			: RedisStore(host, port){}

		virtual ~TripleDB(){};

		bool insert_triple(const triple_t &triple){
			return _insert_triple(triple.s, triple.p, triple.o, triple.d);
		}

		bool _insert_triple(sid_t s, sid_t p, sid_t o, dir_t d){
			
			redisAppendCommand(contxt, 
				"helios.insert_triple %ld %ld %ld %d", s, p, o, d);
			pip_command++;
			
			if(pip_command > 30)
				flush_command();
			return true;
		}
		
		int batch_set_vloc(sid_t v, int loc){
			sid_t vl_key = key_vpid_t(v, 0, LOC, (dir_t)0);
			redisAppendCommand(contxt,"SET %ld %d", vl_key, loc);
			pip_command++;
			
			if(pip_command > 30)
				flush_command();
			return true;
		}



		bool reassign_vd(sid_t v, dir_t d, TripleDB* tgtDB){
			bool flag = true;
			sid_t sd_key = key_vpid_t(v, 0, NO_INDEX, d);
			size_t len = 0;
			const char* str2 = dump(sd_key, &len);
			if(str2 != NULL)
				flag = flag && tgtDB->restore(sd_key, str2, len);
			if(!flag)
				printf("Error, reassign vd fail!\n");
			return flag;
		}

		bool reassign_vpd(sid_t v, sid_t p,dir_t d, TripleDB* tgtDB){
			bool flag = true;
			sid_t sp_key = key_vpid_t(v, p, NO_INDEX, d);
			size_t len = 0;
			const char* str = dump(sp_key, &len);
			if(str != NULL)
				flag = flag && tgtDB->restore(sp_key, str, len);
			else 
				printf("error, reassign vpd %ld %ld %d %ld\n", v, p, d, sp_key);
			assert(str != NULL);
			if(!flag)
				printf("Error, reassign vpd fail!\n");
			return flag;
		}

		void get_neighbor(sid_t v, const vector<sid_t> &p_list, dir_t d, vector<sid_t>* neighbors){
			
			for(int i = 0; i < p_list.size(); i++){
				sid_t p = p_list[i];
				sid_t sp_key = key_vpid_t(v, p, NO_INDEX, d);
				redisAppendCommand(contxt,"GET %ld", sp_key);
			}
			
			redisReply* reply;
			for(int i = 0; i< p_list.size(); i++){
				reply = redis_get_reply(contxt);
				if(reply == NULL || reply -> type != REDIS_REPLY_STRING){
					printf("get neighbor sp error %ld %ld!\n",v,p_list[i]);
					continue;
				}
				deserial_vector(reply->str, reply->len, neighbors);
				freeReplyObject(reply);
			}
		}


		bool reassign_vcounter_weight(sid_t v, TripleDB* tgtDB, bool isTS){
			bool flag = true;
			sid_t sc_key = key_vpid_t(v, 0, COUNTER, (dir_t)0);
			
			size_t len = 0;
			const char* str = dump(sc_key, &len);
			if(str != NULL)
				flag = flag && tgtDB -> restore(sc_key, str, len);

			if(!isTS){
				sid_t ew_key = key_vpid_t(v, 0, EDGE_WEIGHT, (dir_t)0);
				const char *str2 = dump(ew_key, &len);
				if(str2 != NULL)
					flag = flag && tgtDB -> restore(ew_key, str2, len);
			}

			if(!flag)
				printf("Error, reassign counter & weight error!\n");
			return flag;
		}

		vector<int> get_server_info(){
			redisReply* reply;
			reply = (redisReply*)redisCommand(contxt, "HMGET %s %d %d %d", 
					SERVER_INFO, WEIGHT, EDGENUM, OBJNUM);
			vector<int> results = get_array<int>(reply);
			if(results.size() == 0){
				printf("REDIS ERROR: HMGET");
			}
			return results;	
		}

		int reassign(sid_t v, bool isTS){
			redisReply *reply;
			if(isTS)
				reply = (redisReply*)redisCommand(contxt, 
					"helios.reassign %ld ", v);
			else
				reply = (redisReply*)redisCommand(contxt, 
					"helios.reassign_counter %ld", v);
			assert(reply != NULL);
			freeReplyObject(reply);
			return 1;
		}

		int receive(sid_t v, bool isTS){
			redisReply *reply;
			if(isTS)
				reply = (redisReply*)redisCommand(contxt, "helios.receive %ld", v);
			else 
				reply = (redisReply*)redisCommand(contxt, "helios.receive_counter %ld", v);
			assert(reply != NULL);
			freeReplyObject(reply);
			return 1;
		}

		int reassign_lock(sid_t v){
			redisReply *reply;
			reply = (redisReply*)redisCommand(contxt, 
				"helios.reassign_lock %ld ", v);
			assert(reply != NULL);
			int suc = 0;
			if(reply->type == REDIS_REPLY_INTEGER){
				suc = reply->integer;
				//printf("reassign lock %d %s\n", suc, reply->str);
			}
			freeReplyObject(reply);
			return suc;
		}

		int set_v_loc(sid_t v, int loc){
			sid_t vl_key = key_vpid_t(v, 0, LOC, (dir_t)0);
			redisReply *reply;
			reply = (redisReply*)redisCommand(contxt, "SET %ld %d", vl_key, loc);
			assert(reply != NULL);
			freeReplyObject(reply);
			return 1;
		}

		int get_v_loc(sid_t v){
			sid_t vl_key = key_vpid_t(v, 0, LOC, (dir_t)0);
			redisReply *reply;
			reply = (redisReply*)redisCommand(contxt, "GET %ld", vl_key);
			assert(reply != NULL);
			int server = -1;
			if(reply->type == REDIS_REPLY_STRING)
				server = stringToNum<int>(reply->str);
			freeReplyObject(reply);
			return server;
		}
		
		int del_v_loc(sid_t v){
			sid_t vl_key = key_vpid_t(v, 0, LOC, (dir_t)0);
			redisReply *reply;
			reply = (redisReply*)redisCommand(contxt, "DEL %ld", vl_key);
			assert(reply != NULL);
			freeReplyObject(reply);
			return 1;
		}
		
		void write_command(const vector<string> &params){
			int argc = params.size();
			const char** command = (const char**)malloc(argc*sizeof(char*));
			for(int i = 0; i < argc; i++){
				command[i] = params[i].c_str();
			}

			redisReply *reply;
			reply = (redisReply*)redisCommandArgv(contxt, argc, command, NULL);
			if(reply->type == REDIS_REPLY_ERROR)
				printf("command ERR %s\n", reply->str);
			if(reply->type == REDIS_REPLY_INTEGER)
				printf("command integer result: %ld\n", reply->integer);
			if(reply->type == REDIS_REPLY_STRING)
				printf("command string result: %s\n", reply->str);
			if(reply->type == REDIS_REPLY_ARRAY)
				printf("command array result: %d\n", reply->elements);
			freeReplyObject(reply);
		}

};
#endif
