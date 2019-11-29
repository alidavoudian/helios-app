///////////////////////////////////////////////////////////////////////////////////
#include <string>
#include <iostream>
#include <random>
#include <fstream>
#include <unistd.h>
#include <map>
#include "mpi.h"
#include "DataLoader.hpp"
#include "HeliosConfig.hpp"
#include "type.h"
#include "Repartitioner.hpp"
#include "omp.h"

///////////////////////////////////////////////////////////////////////////////////
using namespace std;



///////////////////////////////////////////////////////////////////////////////////
int main(int argc, char *argv[]){
	MPI_Init(NULL, NULL);
	
	double begin = MPI_Wtime();

	int32_t global_num_servers;
	MPI_Comm_size(MPI_COMM_WORLD, &global_num_servers);

	int32_t world_rank;
	MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);

	HeliosConfig config("helios.cfg", global_num_servers); 
	assert(global_num_servers == config.global_num_servers);
	Memory* mem = new Memory(global_num_servers);//including reassign queue
	Database *db = new Database(world_rank,  &config, mem);

	sid_t reduRange = 131072;
	sid_t lubm10240_start = 131073, lubm10240_end = 336637453;
	sid_t watdiv1k_start = 131073, watdiv1k_end = 10421994;
	string datasets[] = {"lubm10240-metis", "watdiv1k-metis"};

	int index = 1; assert(index < sizeof(datasets));
	sid_t startid = 0, endid = 0, num_edges = 0;
	switch(index){
		case 0:
			startid = lubm10240_start;
			endid = lubm10240_end; 
			break;
		case 1:
			startid = watdiv1k_start;
			endid = watdiv1k_end;
			break;
		default:
			break;
	}

	ofstream ofile((datasets[index]+"_"+ std::to_string(world_rank)).c_str());

	sid_t vertex_num = endid - startid + 1;
	sid_t range = vertex_num / global_num_servers;

	sid_t local_id = world_rank*range + startid, local_start = local_id;
	sid_t local_range = local_id + range;
	if(world_rank == global_num_servers - 1){
		local_range = vertex_num + 1;
	}
	
	for(; local_id < local_range; local_id++){
		vector<sid_t> neighbors;
		db->get_neighbor_global(local_id, &neighbors);

		for(int j = 0; j < neighbors.size(); j++ ){
			if(neighbors[j] <= reduRange)
				continue;
			num_edges ++;
			ofile << neighbors[j] - reduRange <<"\t";
		}
		ofile << endl;
	}

	printf("node %d: vertex num %ld (range %ld - %ld), edge num %ld\n", 
			world_rank, vertex_num, local_start, local_range,  num_edges);
	MPI_Finalize();
}


