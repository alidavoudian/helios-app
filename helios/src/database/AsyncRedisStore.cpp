#include "AsyncRedisStore.hpp"
#include <unistd.h>
#include "QueryNode.hpp"
#include <omp.h>
void AsyncRedisStore::get_p_counter( sid_t p, vector<int> *buf){
	//redisLibevAttach(EV_DEFAULT_ c);
	int executed = 1;
	async_pam *targ = new async_pam(buf, &executed);	

	sid_t pc_key = key_vpid_t(0, p, COUNTER,(dir_t)0);
	redisAsyncCommand(c, rediscommand_callback, targ, 
			"HMGET %ld %d %d", pc_key, OUT, IN);

	while(executed > 0){
		redisAsyncHandleWrite(c); 
      		redisAsyncHandleRead(c); 
      		usleep(1000);
	}
	//ev_loop(EV_DEFAULT_ 0);
}

void AsyncRedisStore::get_reassign_info( sid_t v, vector<int> *buf, bool isTS){
	//redisLibevAttach(EV_DEFAULT_ c);
	int executed = 1;
	async_pam *targ = new async_pam(buf, &executed);
	
	if(isTS)
		redisAsyncCommand(c, rediscommand_callback, targ, 
				"helios.get_reassign_info_ts %ld", v);
	else
		redisAsyncCommand(c, rediscommand_callback, targ, 
				"helios.get_reassign_info_cs %ld", v);
			
	while(executed > 0){
		redisAsyncHandleWrite(c); 
      		redisAsyncHandleRead(c); 
      		usleep(1000);
	}
	//ev_loop(EV_DEFAULT_ 0);
}

////////////////////////////////// counter db///////////////////////////////////////////////////
void AsyncRedisStore::batch_update_edgeweight(sid_t root_v, const vector<triple_t> &triples, int edgelog_len){
	int executed = 0, c_num = 0;
if(root_v == 0){	
	for(int i = 0; i < triples.size(); i++){
		triple_t t = triples[i];
		
		if(t.o != 0){
		//	printf("update edge weight %ld %ld", t.s, t.o);
			redisAsyncCommand(c, rediswrite_callback, &executed, 
				"helios.update_edgeweight %ld %ld %d", t.s, t.o, edgelog_len);
			c_num ++;
		}
		for(int j = 0; j< t.o_vals.size(); j++){
		//	printf("update edge weight %ld %ld", t.s, (t.o_vals)[j]);
			redisAsyncCommand(c, rediswrite_callback, &executed, 
				"helios.update_edgeweight %ld %ld %d", t.s, (t.o_vals)[j], edgelog_len);
			c_num++;
		}
		if(c_num > 50000){
			while(executed < c_num){
				redisAsyncHandleWrite(c); 
      				redisAsyncHandleRead(c); 
      				usleep(1000);
			}
			executed = 0; c_num = 0;
			break;
		}
	}
}else {
	for(int i = 0; i < triples.size(); i++){
		triple_t t = triples[i];

		if(t.o != 0){
			redisAsyncCommand(c, rediswrite_callback, &executed, 
				"helios.update_edgeweight %ld %ld %d", root_v, t.o, edgelog_len);
			c_num ++;
		}
		for(int j = 0; j< t.o_vals.size(); j++){
			redisAsyncCommand(c, rediswrite_callback, &executed, 
				"helios.update_edgeweight %ld %ld %d", root_v, (t.o_vals)[j], edgelog_len);
			c_num++;
		}
		if(c_num > 50000){
			while(executed < c_num){
				redisAsyncHandleWrite(c); 
      				redisAsyncHandleRead(c); 
      				usleep(1000);
			}
			executed = 0; c_num = 0;
			break;
		}
	}
}
	
	while(executed < c_num){
		redisAsyncHandleWrite(c); 
      		redisAsyncHandleRead(c); 
      		usleep(1000);
	}
}

void AsyncRedisStore::batch_update_vertexweight(const vector<sid_t> &vs, int incr_weight, int log_len){
	int executed = 0, c_num = 0;
	
	int server_weight = vs.size() * incr_weight;
	redisAsyncCommand(c, rediswrite_callback, &executed, "HINCRBY %s %d %d", SERVER_INFO, WEIGHT, server_weight);
	c_num ++;

	for(int i = 0; i < vs.size(); i++){
		sid_t s = vs[i];
		redisAsyncCommand(c,rediswrite_callback, &executed, "HINCRBY %ld %d %d", 
			key_vpid_t(s, 0, COUNTER, (dir_t)0), WEIGHT, incr_weight);
		c_num++;
		//if(log_len > 0){
			redisAsyncCommand(c,rediswrite_callback, &executed, "helios.append_activev %ld %d", s, log_len);
			c_num++;
		//}
		if(c_num > 50000){
			while(executed < c_num){
				redisAsyncHandleWrite(c); 
      				redisAsyncHandleRead(c); 
      				usleep(1000);
			}
			executed = 0; c_num = 0;
			break;
		}
	}
	
	while(executed < c_num){
		redisAsyncHandleWrite(c); 
      		redisAsyncHandleRead(c); 
      		usleep(1000);
	}
}
///////////////////////////////////////////////////////////////////

void AsyncRedisStore::write_command(const vector<const char*> &params){
	int argc = params.size();
	const char** command = (const char**)malloc(argc*sizeof(char*));
	for(int i = 0; i < argc; i++)
		command[i] = params[i];
	
	/*for(int i = 0; i < argc;  i++)
		printf("%s %d ", command[i], strlen(command[i]));*/
	redisAsyncCommandArgv(c, NULL, NULL, argc, command, NULL);
	redisAsyncHandleWrite(c);
}

void AsyncRedisStore::write_command(const vector<string> &params){
	int argc = params.size();
	const char** command = (const char**)malloc(argc*sizeof(char*));
	for(int i = 0; i < argc; i++){
		char* array = new char[params[i].length()+1];
		strcpy(array, params[i].c_str());
		command[i] = array;
	}

	redisAsyncCommandArgv(c, NULL, NULL, argc, command, NULL);
	redisAsyncHandleWrite(c);
}

/*void AsyncRedisStore::write_command(const char** command, int argc){
	printf("commands: %d ", argc);
	for(int i = 0; i < argc;  i++)
		printf("%s l", command[i]);
	redisAsyncCommandArgv(c, NULL, NULL, argc, command, NULL);
	redisAsyncHandleWrite(c);
}*/

void AsyncRedisStore::read_zset(string key, vector<sid_t> *results){
	//redisLibevAttach(EV_DEFAULT_ c);
	int executed = 1;
	readset_pam *targ = new readset_pam(results, &executed);	
	//redisAsyncCommand(c,read_zset_callback, targ, "ZRANGE %s %d %d", key, 0, -1);

	printf("Read zset %s", key.c_str());
	redisAsyncCommand(c, read_zset_callback, targ, "SMEMBERS %s", key.c_str());
	while(executed > 0){
		redisAsyncHandleWrite(c); // This sends the command.
      		redisAsyncHandleRead(c); // This calls the callback if the reply has been received.
      		usleep(1000);
	}
	//ev_loop(EV_DEFAULT_ 0);
}

void AsyncRedisStore::read_zset(sid_t key, vector<sid_t> *results){
	//redisLibevAttach(EV_DEFAULT_ c);
	int executed = 1;
	readset_pam *targ = new readset_pam(results, &executed);
	//redisAsyncCommand(c,read_zset_callback, targ, "ZRANGE %s %d %d", key, 0, -1);
	//printf("Read zset %ld \n", key);
	redisAsyncCommand(c, read_zset_callback, targ, "SMEMBERS %ld", key);
	while(executed > 0){
		redisAsyncHandleWrite(c); // This sends the command.
      		redisAsyncHandleRead(c); // This calls the callback if the reply has been received.
      		usleep(1000);
	}
	//ev_loop(EV_DEFAULT_ 0);
}

void AsyncRedisStore::read_set(const char* key, vector<sid_t> *results){
	//redisLibevAttach(EV_DEFAULT_ c);
	
	int executed = 1;
	readset_pam *targ = new readset_pam(results, &executed);
	
	redisAsyncCommand(c,read_strset_callback, targ, "GET %s", key);
	
	//ev_loop(EV_DEFAULT_ 0);
	while(executed > 0){
		redisAsyncHandleWrite(c); 
      		redisAsyncHandleRead(c); 
      		usleep(1000);
	}
}
void AsyncRedisStore::read_set(sid_t key, vector<sid_t> *results){
	//redisLibevAttach(EV_DEFAULT_ c);
	int executed = 1;
	readset_pam *targ = new readset_pam(results, &executed);
	//printf("set key %s\n",key);
	redisAsyncCommand(c,read_strset_callback, targ, "GET %ld", key);
	
	//ev_loop(EV_DEFAULT_ 0);
	while(executed > 0){
		redisAsyncHandleWrite(c); 
      		redisAsyncHandleRead(c); 
      		usleep(1000);
	}
}

void AsyncRedisStore::traverse_vertex(QueryNode* queryRoot, const vector<sid_t> &vs, QueryEdge* edge, 
		vector<triple_t> *results){
	
	int executed = (edge->bind_val).size() * vs.size();
	int c_num = 0;
	uint64_t start= get_usec();
	for(int k = 0; k < (edge->bind_val).size(); k++){
		sid_t p = (edge->bind_val)[k];
		for(int i = 0; i < vs.size(); i++){
			sid_t s = vs[i];
			int s_weight = 0;
			traverse_pam *targ = new traverse_pam(s,p,edge->d,&executed);
			if(edge->towardsJoint == 1)
				targ->node = edge->node;
			else 
				targ->node = NULL;
			targ->root = queryRoot; targ->results = results;
			sid_t key_t = key_vpid_t(s, p, NO_INDEX,edge->d);
			c_num++;
			redisAsyncCommand(c, traverse_callback, targ, "GET %ld", key_t);
		
		}
	}
	//printf("c_num %d %ldusec \n", c_num, get_usec()-start);
	start = get_usec();
	if(c_num > 0){
		
		executed = c_num;	
		while(executed > 0){
			redisAsyncHandleWrite(c); 
			redisAsyncHandleRead(c); 
      			usleep(1000);
		}
	}
	//printf("command time :%ldusec \n", get_usec()-start);
}

void AsyncRedisStore::batch_getloc(const vector<sid_t> &vs, map<int, vector<sid_t>> *v_locs){
	//redisLibevAttach(EV_DEFAULT_ c);
	int executed = vs.size();
	
	for(int i = 0; i < vs.size(); i++){
		sid_t vid = vs[i];
		getloc_pam *targ = new getloc_pam(v_locs, &executed, 
				vid, connectto_sid);
		
		sid_t vloc = key_vpid_t(vid, 0, LOC, (dir_t)0);
		redisAsyncCommand(c,batch_getloc_callback,targ,
					"GET %ld", vloc);
	}
	while(executed > 0){
		redisAsyncHandleWrite(c); 
      		redisAsyncHandleRead(c); 
      		usleep(1000);
	}
	//ev_loop(EV_DEFAULT_ 0);
}


