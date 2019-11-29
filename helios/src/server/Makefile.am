AUTOMAKE_OPTIONS=foreign

PWD = $(shell /bin/pwd)/..

noinst_LIBRARIES=libserver.a
libserver_a_SOURCES=Repartitioner.cpp 

LIBS = -lhiredis
AM_CPPFLAGS = -I$(PWD)/utils -I$(PWD)/database 
