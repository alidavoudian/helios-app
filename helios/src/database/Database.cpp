#include "Database.hpp"
#include <math.h>
#include <omp.h>
bool Database::_add_triple(int initial_node, const triple_t &triple){
	triple_store[initial_node]->insert_triple(triple);
	return true;	
}
map<int, vector<sid_t>> Database::get_initial_server(const vector<sid_t> &v){
	map<int, vector<sid_t>> initial_servers;
	for(int i = 0; i < v.size(); i++){
		int init_s = jumpConsistentHash(v[i], num_servers);
		if(initial_servers.find(init_s) == initial_servers.end()){
			initial_servers.insert(pair<int, vector<sid_t>>(init_s, vector<sid_t>()));
		}
		initial_servers[init_s].push_back(v[i]);
	}
	return initial_servers;
}

/*Load data*/
bool Database::load_data(const vector<itriple_t> &spo, dir_t d){
	uint64_t s = 0;
        while (s < spo.size()) {
            // predicate-based key (subject + predicate)
            uint64_t e = s + 1;
            while ((e < spo.size())
                    && (spo[s].s == spo[e].s)
                    && (spo[s].p == spo[e].p))  { e++; }
            
	    // insert vertex
            sid_t key = key_vpid_t(spo[s].s, spo[s].p, NO_INDEX,d);

	    int sz = e - s;
	    int* val = (int*)malloc(sizeof(int) * sz);
	    //setobj* val = (setobj*)malloc(sizeof(setobj*));

	    int off = 0;
            // insert edges
            for (uint64_t i = s; i < e; i++)
            	val[off++] = spo[i].o;

	   //insert to redis 
            s = e;
        }
}
/*************vertex location******************/
int Database::get_global_vloc(sid_t v){
	int initial_server = jumpConsistentHash(v, num_servers);
	int host_server = triple_store[initial_server]->get_v_loc(v);
	if(host_server == -1)
		return initial_server; // not reassigned, on the initial server
	else return host_server;
}

int Database::set_v_loc(sid_t v, int loc){
	int initial_server = jumpConsistentHash(v, num_servers);
	if(loc == initial_server){
		//printf("Vertex %ld Loc = initial %d\n", v, initial_server);
		triple_store[initial_server]->del_v_loc(v);
	}
	else {
		//printf("Vertex %ld Loc initial = %d, current = %d\n", v, initial_server,loc);
		triple_store[initial_server]->set_v_loc(v, loc);
	}
}

map<int, vector<sid_t>> Database::batch_get_global_vloc(const vector<sid_t> &v){
	map<int, vector<sid_t>> initial_servers = get_initial_server(v);
	
	map<int, vector<sid_t>> real_locs;
	for(int i = 0; i < num_servers; i++){
		if(initial_servers.find(i) == initial_servers.end()){
			continue;
		}
		async_store[i][0]->batch_getloc(initial_servers[i], &real_locs);
	}
	return real_locs;
}
/*****************************predicate info************************************/
int Database::get_p_counter(sid_t p, int *scounter, int *ocounter){
	return mem->get_p_counter(p, scounter, ocounter, this);
}

vector<int> Database::get_p_loc(sid_t p, dir_t d){
	return mem->get_p_loc(p, d);
}

int Database::get_global_pcounter(sid_t p, int *counters, 
		vector<int> *p_out_locs, vector<int> *p_in_locs){

	vector<vector<int>> pcounters; pcounters.resize(num_servers);
	#pragma omp parallel for num_threads(num_servers)
	for(int i = 0; i < num_servers; i++){
		async_store[i][0]->get_p_counter(p, &pcounters[i]);
		assert(pcounters[i].size() == 2);
	}

	for(int i = 0; i < num_servers; i++){
		int scounter = pcounters[i][0], ocounter = pcounters[i][1];
		printf("pcounter %d %d %d \n", p, scounter, ocounter);
		if(scounter > 0){
			counters[0] += scounter;
			p_out_locs->push_back(i);
		}
		if(ocounter > 0){
			counters[1] += ocounter;
			p_in_locs->push_back(i);
		}
	}

	return 1; 
}

/************************** Begin: execute patterns on client-side *****************************************/
QueryNode* Database::parse_patterns(const vector<triple_t> &triples, const vector<vector<sid_t>> &binds){
	int triple_num = triples.size();
	int tag_num = binds.size();
	vector<void*> query_nodes; query_nodes.resize(tag_num);
	QueryNode* queryRoot;

	for(int i = 0; i < triple_num; i++){
		int s_t = triples[i].s;
		QueryNode *snode = (QueryNode*)query_nodes[s_t];
		if(snode == NULL){
			snode = new QueryNode(s_t, binds[s_t]);
			query_nodes[s_t] = snode;
		}

		int p_t = triples[i].p;
		QueryEdge* edge;
		if(query_nodes[p_t] == NULL){
			edge = new QueryEdge(p_t, triples[i].d, 1, binds[p_t]);
			query_nodes[p_t] = edge;
		}else printf("ERR: duplicate predicate tag in query pattern\n");

		int o_t = triples[i].o;
		QueryNode *onode = (QueryNode*)query_nodes[o_t];
		if(onode == NULL){
			onode = new QueryNode(o_t, binds[o_t]);
			query_nodes[o_t] = onode;
		}

		if(triples[i].d == OUT){
			queryRoot = snode;
			(snode->edges).push_back(edge);
			edge->node = onode;
		}else{
			queryRoot = onode;
			(onode->edges).push_back(edge);
			edge->node = snode;
		}
	}
	return queryRoot;
}
	
int Database::evaluate_root_binding_local(QueryNode* queryRoot, vector<sid_t>* src_bind){
	queryRoot->init_interunion_key(num_servers);
	int flag = queryRoot->preprocess(sid, async_store[sid][0]);
	if(flag){
		async_store[sid][0]->read_zset((queryRoot->src_inter_key[sid]), src_bind);
		return 1;
	}
	return 0;
}

int Database::_client_pattern_evaluate(int tgt_s, vector<sid_t> src_bind, QueryNode* queryRoot, 
		vector<triple_t> *results){
	vector<sid_t> inter_sets;
	uint64_t start = get_usec();
		
	int flag = queryRoot->preprocess(tgt_s, async_store[tgt_s][0]);
	if(flag){
		async_store[tgt_s][0]->read_zset((queryRoot->src_inter_key[tgt_s]), &inter_sets);
	}
	if(src_bind.size() != 0)
		if(inter_sets.size() != 0){
			//printf("@%d inter_sets size %d src_bind size %d\n", sid, inter_sets.size(), src_bind.size());
			inter_sets = intersect(inter_sets, src_bind);
			if(inter_sets.size() != src_bind.size()){
				vector<sid_t> bind_prune = difference(src_bind, inter_sets);
				//printf("@%d src_bind %d src_num %d prune bind size %d\n", sid, src_bind.size(), inter_sets.size(), bind_prune.size());
				queryRoot->insert_prune_bind(bind_prune);
			}
		}
		else
			inter_sets = src_bind;
	else if (inter_sets.size() != 0)
		queryRoot->insert_bind(inter_sets);

	if(inter_sets.size() == 0){
		//printf("Node is not bound or preprocess doesnot find ant binding!\n ");
		return 0;
	}

	//printf("src num %ld \n", inter_sets.size());	

	int thread_num;
	int edge_sz = (queryRoot->edges).size();
	if(edge_sz < NUM_CONNECT)   thread_num = edge_sz;
	else thread_num = NUM_CONNECT;
	vector<vector<triple_t>> res; res.resize(edge_sz);
	#pragma omp parallel for num_threads(thread_num)
	for(int j = 0; j < edge_sz; j++){
		QueryEdge* edge = (queryRoot->edges)[j];
		
		int aid = j % thread_num;
		//printf("@%d connect %d thread %d run edge %d\n", sid, tgt_s, aid, j);
		async_store[tgt_s][aid]->traverse_vertex(queryRoot, inter_sets, 
				edge, &(res[j]));
		//printf("@%d thread %d edge %d local_results %d\n",sid, aid, j ,res[j].size());
	}

	for(int j = 0; j < edge_sz; j++){
		if(res[j].size() <= 0)
			continue;
		results->insert(results->end(), res[j].begin(), res[j].end());
		
		if(cfg->enable_reassign && (queryRoot->edges)[j]->towardsJoint)// && res[j].size() <= 500000)
			mem->reassign_queue->push_weight_task(sid, new WeightUpdateTask(queryRoot->root_vertex, res[j]));
			//mem->reassign_queue->push_weight_task(tgt_s, new WeightUpdateTask(queryRoot->root_vertex, res[j]));
	}

	if(cfg->enable_reassign)
		mem->reassign_queue->push_weight_task(tgt_s, 
				new WeightUpdateTask(inter_sets, edge_sz));
	
	//printf("@%d  connect %d get %d pattern evaluate time %ldusec\n", sid, tgt_s,
	//		results->size(), get_usec()-start);
	return 1;
}

int Database::client_pattern_evaluate_global(QueryNode* queryRoot, vector<triple_t> *results){
	printf("Donnot call this function now!\n");
	if(queryRoot == NULL)
		return 0;
	queryRoot->init_interunion_key(num_servers);
	if((queryRoot->bind_val).size() != 0){
		int flag = 1;
		map<int, vector<sid_t>> loc_binds = batch_get_global_vloc(queryRoot->bind_val);
		int map_sz = loc_binds.size();
		vector<vector<triple_t>> res; res.resize(num_servers);
		int sum = 0;
		#pragma omp parallel for num_threads(num_servers) //reduction(+:sum)
		for(int i = 0; i < num_servers; i++){
			if(loc_binds.find(i) == loc_binds.end()){
				continue;
			}
			sum ++;
			//printf("%d send %d to node %d\n", sid, loc_binds[i].size(), i);
			flag = flag & _client_pattern_evaluate(i, loc_binds[i], queryRoot, &res[i]);
		}
		for(int i = 0; i < num_servers; i++)
			results -> insert(results->end(), res[i].begin(), res[i].end());
		if(sum != map_sz){
			printf("WARN: some objects may not be evaluated!\n");
		}
		
		//vector<sid_t> dif = difference(queryRoot->bind_val, queryRoot->bind_to_prune);
		//printf("@%d bind_val %d - bind_to_prune %d size =  %d\n", sid, queryRoot->bind_val.size(), queryRoot->bind_to_prune.size(), dif.size());
		return flag;
	}else{
		return _client_pattern_evaluate(sid, vector<sid_t>(), queryRoot, results);
	}
}

int Database::client_pattern_evaluate_local(QueryNode* queryRoot, vector<triple_t> *results){
	if(queryRoot == NULL)
		return 0;
	queryRoot->init_interunion_key(num_servers);
	return _client_pattern_evaluate(sid, queryRoot->bind_val, queryRoot, results);
}
int Database::client_pattern_evaluate(int sid, const vector<triple_t> &triples, 
		const vector<vector<sid_t>> &binds, vector<triple_t> *results){
	QueryNode* queryRoot = parse_patterns(triples, binds);
	queryRoot->init_interunion_key(num_servers);
	clock_t start = clock();
	int flag = _client_pattern_evaluate(sid, queryRoot->bind_val,  queryRoot, results);
	clock_t s1 = clock();
	//printf("%d client pattern evaluate time: %ld\n", sid, s1-start);
	//delete queryRoot;
	return flag;
}
/************************* End: execute patterns on client side *******************************/

/************************** Begin: Reassignment ********************************/
int Database::reassign_evaluate(int loc_s, sid_t v){
	return get_reassign_info(loc_s, v);	
}

/*This reassign func is not tested!*/
/*int Database::reassign(int loc_s, sid_t v, int tgt_node){
	int flag;
	if(request_global_reassign_lock(loc_s, v) == 1){
		printf("%d reassign %d to %d\n",loc_s, v, tgt_node);
		if(triple_store[loc_s] -> reassign(v, (cfg->host_names)[tgt_node].c_str(), 
				(cfg->ports)[tgt_node])){
			
			if(triple_store[tgt_node] -> receive(v)){
				set_v_loc(v, tgt_node);
				flag = 1; //success
			}
			else flag = -2;// tgt receive fails
		}
		else flag  = -1;
	}
	else flag = 0;// reassign fails
	return flag;
}*/

//////////////////////////////////////////////////////////////////////////////////

bool Database::client_reassign_vd(int loc_s, sid_t v, dir_t d, int tgt_node){
	bool flag = true;
	sid_t sd_key = key_vpid_t(v, 0, NO_INDEX, d);
	vector<sid_t> p_list;
	//printf("sd_key %ld %ld\n", v, sd_key);
	async_store[loc_s][0]->read_zset(sd_key, &p_list);
	for(int i = 0; i < p_list.size(); i++){
		sid_t p = p_list[i];
		flag = flag && triple_store[loc_s]
			->reassign_vpd(v,p,d,triple_store[tgt_node]);
	}
	flag = flag && triple_store[loc_s]->reassign_vd(v,d,triple_store[tgt_node]);
	return flag;
}

int Database::client_reassign(int loc_s, sid_t v, int tgt_node){
	int f;
	if(request_global_reassign_lock(loc_s, v) == 1){
		printf("%d reassign %d to %d\n",loc_s, v, tgt_node);
		bool flag = true;
		flag = flag && client_reassign_vd(loc_s, v, OUT, tgt_node);
		flag = flag && client_reassign_vd(loc_s, v, IN, tgt_node);
		flag = flag && triple_store[loc_s]
			->reassign_vcounter_weight(v, triple_store[tgt_node], true);
		flag = flag && counter_db[loc_s]
			->reassign_vcounter_weight(v, counter_db[tgt_node], false);
		if(flag){
			if(triple_store[tgt_node] -> receive(v,true)
				&& counter_db[tgt_node]->receive(v,false)){
				set_v_loc(v, tgt_node);
				if(triple_store[loc_s] -> reassign(v, true)
					&& counter_db[loc_s] -> reassign(v,false))
					f = 1;
				else f = -3; //success
			}
			else f = -2;
		}else f = -1;
	}else f = 0;
	return f;
}
////////////////////////////////////////////////////////////////////////////////////

int Database::get_reassign_info(int loc_s, sid_t v){
	vector<vector<int>> reassign_info_ts, reassign_info_cs;
	reassign_info_ts.resize(num_servers);
	reassign_info_cs.resize(num_servers);
	int obj_weight = 0, obj_edgenum = 0, obj_prev_reassign_weight = 0; 
	int edgesum = 0, weightsum = 0, objsum = 0; 
	bool flag = true, locality_null = true;

#pragma omp parallel for num_threads(num_servers)
	for(int i = 0; i < num_servers; i++){
		/* 0- server_objnum. 1 - server_edgenum, 
		 * 2 - obj-exists, 3 - obj_top_ll, 
		 * 4 - obj-edgenum
		 * */
		async_store[i][0]->get_reassign_info( v, &reassign_info_ts[i], true);
		if(reassign_info_ts[i].size() != 5)
			printf("error reassign info %d %ld\n", i, v);
		assert(reassign_info_ts[i].size() == 5);
		
		objsum += reassign_info_ts[i][0];
		edgesum += reassign_info_ts[i][1];
		if(reassign_info_ts[i][2] == 1){
			if(i != loc_s)
				flag = false;
			obj_edgenum = reassign_info_ts[i][4];
		}
	}

#pragma omp parallel for num_threads(num_servers)
	for(int i = 0; i < num_servers; i++){
		/*  0 - server_weight, 1- active_obj_num
		 *  2- obj_workload_ll, 
		 *  3 - obj_weight, 4 - obj-prev_reassign_weight
		 * */
		counter_store[i]->get_reassign_info( v, &reassign_info_cs[i],false);
		if(reassign_info_cs[i].size() != 5)
			printf("error reassign counter info %d %ld\n", i, v);
		assert(reassign_info_cs[i].size() == 5);
		
		weightsum += reassign_info_cs[i][0];
		if(reassign_info_cs[i][2] > 0)
			locality_null = false;
		if(i == loc_s){
			obj_weight = reassign_info_cs[i][3];
			obj_prev_reassign_weight = reassign_info_cs[i][4];
		}
	}


	if(obj_edgenum == 0 || !flag || locality_null)
		return -1;
	
	if(obj_prev_reassign_weight == 0)
	{
		if(obj_weight < cfg->trigger_k)
			return -1;
	}else{
		if(cfg->trigger_double && 
				obj_weight - obj_prev_reassign_weight < obj_prev_reassign_weight)
			return -1;
		if(!(cfg->trigger_double) && 
				obj_weight - obj_prev_reassign_weight < cfg->trigger_k)
			return -1;
	}

	double avg_edge = edgesum/num_servers, avg_weight = weightsum/num_servers, avg_obj = objsum/num_servers;
	double max_score = -INFINITY;
	int server_chose = -1;
	for(int i = 0; i < num_servers; i++){
		double score = fennel(reassign_info_cs[loc_s][0], reassign_info_cs[i][0]
				, reassign_info_cs[i][1],avg_weight, 
				obj_weight, reassign_info_cs[i][22]);
		if(score > max_score){
			max_score = score;
			server_chose = i;
		}
	}
	return server_chose;
}

double Database::fennel(int src_weight, int server_weight, int active_obj_num, int avg_weight, 
		int o_weight, int locality){

	if((double)(src_weight - o_weight ) - (avg_weight * (2 - cfg -> gama)) < 0.0
		|| (double)(server_weight + o_weight) - (avg_weight * cfg->gama) > 0.0)
		return -INFINITY;

	if(active_obj_num == 0)
		return (double)locality ;
	else
		return (double)locality - cfg->beta * (server_weight/(num_servers * active_obj_num));
}
/***************************** End: reassignment *************************************/
//double up
int Database::request_global_reassign_lock(int loc_s, sid_t v){
	return triple_store[loc_s]->reassign_lock(v);	
}

int Database::batch_up_edgeweight(int sid, sid_t root_v, const vector<triple_t> &triples){
	//async_store[sid][0]->batch_update_edgeweight(triples, cfg->edgelog_len);
	counter_store[sid]->batch_update_edgeweight(root_v, triples, cfg->edgelog_len);	
	return 1;
}

int Database::batch_up_vertexweight(int sid, const vector<sid_t> &vs, int incr_weight){
	//async_store[sid][0]->batch_update_vertexweight(vs, incr_weight, cfg->verticelog_len);
	counter_store[sid]->batch_update_vertexweight(vs, incr_weight, cfg->verticelog_len);
	return 1;
}
