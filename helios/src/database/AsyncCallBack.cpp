#include "AsyncCallBack.hpp"
#include "QueryNode.hpp"

void connectCallback(const redisAsyncContext *c, int status){
	if(status != REDIS_OK){
		printf("ERR: %s\n", c->errstr);
		return;
	}
	printf("Connected...\n");
}

void disconnectCallback(const redisAsyncContext *c, int status){
	if(status != REDIS_OK){
		printf("ERR: %s\n", c->errstr);
		return;
	}
	printf("Disconnected...\n");
	//aeStop(loop);
}

void rediswrite_callback(redisAsyncContext *c, void *r, void *privdata){
	redisReply *reply = (redisReply*)r;
	int* executed = (int*)privdata;
	if(reply == NULL){
		printf("ERR - rediscommand_callback async reply NULL \n");
	}
	
	(*executed)++;
}

void rediscommand_callback(redisAsyncContext *c, void *r, void *privdata){
	redisReply *reply = (redisReply*)r;
	
	async_pam *targ = (async_pam*)privdata;
	vector<int>* buf = targ->buf;
	int * executed = targ->executed;
	delete targ;

	if(reply == NULL){
		printf("ERR - rediscommand_callback async reply NULL \n");
		goto end;
	}
	if(reply->type == REDIS_REPLY_ARRAY){
		//assert(reply->elements == 2 || reply->elements == 10);
		for(int i = 0; i<reply->elements; i++){
			if(reply->element[i]->type == REDIS_REPLY_STRING){
				//printf("buff str %s ", reply->element[i]->str);
				buf->push_back(stringToNum<int>(reply->element[i]->str));
			}
			else if(reply->element[i]->type == REDIS_REPLY_INTEGER){
				//printf("buff str %s ", reply->element[i]->integer);
				buf->push_back(reply->element[i]->integer);
			}
			else {
				//printf("error ele type %d \n", reply->element[i]->type);
				buf->push_back(0); }
		}	
	}else {
		buf->push_back(-1);
	}
	goto end;
end:
	(*executed)--;	
	//if((*executed) <= 0) ev_break(EV_DEFAULT_ EVBREAK_ONE);
}

void read_zset_callback(redisAsyncContext *c, void *r, void *privdata){
	readset_pam *targ = (readset_pam*)privdata;
	vector<sid_t> *results = targ->results;
	int * executed = targ->executed;
	delete targ;

	redisReply *reply = (redisReply*)r;
	
	if(reply == NULL){
		printf("ERR - async reply NULL in read_zset\n");
		goto end;
	}

	if(reply->type != REDIS_REPLY_ARRAY){
		printf("EMPTY - read_zset - %s\n", reply->str);
		goto end;
	}
	//printf("reply size %d\n", reply->elements);
	for(int i = 0; i<reply->elements; i++){
		redisReply* r_ele = reply->element[i];
		if(r_ele -> type != REDIS_REPLY_STRING){
			printf("ERR- async zset type not string!\n");
			continue;
		}
		//printf("%s ", r_ele->str);
		results->push_back(stringToNum<sid_t>(r_ele->str));
	}
	//printf("\n");
	goto end;
end:
	(*executed)--;
	//if((*executed) <= 0) ev_break(EV_DEFAULT_ EVBREAK_ONE);
}
void read_strset_callback(redisAsyncContext *c, void *r, void *privdata){
	readset_pam *targ = (readset_pam*)privdata;
	vector<sid_t> *results = targ->results;
	int * executed = targ->executed;
	delete targ;

	redisReply *reply = (redisReply*)r;
	
	if(reply == NULL){
		printf("ERR - async reply NULL in read_strset\n");
		goto end;
	}

	if(reply->type != REDIS_REPLY_STRING){
		printf("Empty- read_strset - %s\n", reply->str);
		goto end;
	}
	deserial_vector(reply->str, reply->len, results);
	goto end;
end:
	(*executed)--;
	//if((*executed) <= 0)  ev_break(EV_DEFAULT_ EVBREAK_ONE);
}

void traverse_callback(redisAsyncContext *c, void *r, void *privdata){
	traverse_pam*targ = (traverse_pam*)privdata;
	triple_t rt(targ->s, targ->p, targ->d);
	int * executed = targ->executed;
	QueryNode* node = targ->node;
	QueryNode* root = targ->root;
	vector<triple_t> *results = targ->results; 
	delete targ;

	redisReply *reply = (redisReply*)r;
	
	if(reply == NULL){
		printf("ERR - async reply NULL in vertex traversal\n");
		goto end;
	}

	if(reply->type != REDIS_REPLY_STRING){
		//printf("EMPTY - vertex traversal - %s\n", reply->str);
		root->insert_prune_bind(rt.s);
		goto end;
	}
	
	rt.o_vals = deserial_vector(reply->str, reply->len);
	if(node != NULL){	
		//sort_rem_dup(&(triple->o_vals));
		//node->insert_tmp_bind(rt.o_vals);
	}//else
	//	rt.o_list = reply->str;
	
	results->push_back(rt);
	goto end;
end:
	(*executed)--;
	//if((*executed) <= 0)ev_break(EV_DEFAULT_ EVBREAK_ONE);
}

/*
 * getloc_reqlock_callback
 * batch_getloc_callback
 * */
void batch_getloc_callback(redisAsyncContext *c, void *r, void *privdata){
	getloc_pam *targ = (getloc_pam*)privdata;
	map<int, vector<sid_t>> *v_locs = targ->v_locs;
	int *executed = targ->executed;
	int vid = targ->vid;
	int connectto_sid = targ->connectto_sid;
	
	redisReply *reply = (redisReply*)r;
	if(reply == NULL){
		printf("ERR - async reply NULL in pattern evaluate\n");
		goto end;
	}

	int loc;
	if(reply->type == REDIS_REPLY_STRING)
		loc = stringToNum<int>(reply->str);
	else if(reply->type == REDIS_REPLY_INTEGER){
		loc = reply->integer;
		if(loc == -1)
			loc = connectto_sid;
	}else if (reply->type == REDIS_REPLY_NIL){
		loc = connectto_sid;
	}
	if(v_locs->find(loc) == v_locs->end()){
		v_locs->insert(pair<int,vector<sid_t>>(loc, vector<sid_t>()));
	}
	(*v_locs)[loc].push_back(vid);
	goto end;
end:
	delete targ;
	(*executed)--;	
	//if((*executed) <= 0) ev_break(EV_DEFAULT_ EVBREAK_ONE);
}


