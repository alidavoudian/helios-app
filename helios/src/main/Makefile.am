AUTOMAKE_OPTIONS=foreign

PWD = $(shell /bin/pwd)/..

#bin_PROGRAMS=gen_metis_graph
#gen_metis_graph_SOURCES=gen_metis_main.cpp  
#gen_metis_graph_LDADD=  $(PWD)/loader/libloader.a  $(PWD)/server/libserver.a $(PWD)/utils/libutils.a $(PWD)/database/libdatabase.a   $(PWD)/utils/libutils.a 
#AM_CPPFLAGS=-I$(PWD)/utils -I$(PWD)/database  -I$(PWD)/loader -I/$(PWD)/server -I$(MPI_CXXFLAGS)  
#gen_metis_graph_LDFLAGS=$(MPI_CXXLDFLAGS) -lmpi -lconfig++ -lhiredis -lpthread -ltbb -lboost_serialization -lboost_thread -lrt -lzmq 

bin_PROGRAMS=helios_server
helios_server_SOURCES=main.cpp  
helios_server_LDADD=  $(PWD)/loader/libloader.a  $(PWD)/server/libserver.a $(PWD)/utils/libutils.a $(PWD)/database/libdatabase.a   $(PWD)/utils/libutils.a 
AM_CPPFLAGS=-I$(PWD)/utils -I$(PWD)/database  -I$(PWD)/loader -I/$(PWD)/server -I$(MPI_CXXFLAGS)  
helios_server_LDFLAGS=$(MPI_CXXLDFLAGS) -lmpi -lconfig++ -lhiredis -lpthread -ltbb -lboost_serialization -lboost_thread -lboost_date_time -lrt -lzmq 

