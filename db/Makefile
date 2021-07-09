# LEVELDB_INC=-I/home/xp/flying/leveldb-cuda/include 
# ROCKSDB_INC=-I/home/xp/flying/rocksdb-6.4.6/include -I/home/xp/flying/rocksdb-6.4.6/
# TITAN_INC=-I/home/xp/flying/titan/include 
ROCKSDB_INC=-I/home/dc/zcj/TRAID/include -I/home/dc/zcj/TRAID/
# ROCKSDB_INC=-I/home/dc/zcj/rocksdb4.13/include -I/home/dc/zcj/rocksdb4.13/
LOCAL_INC=-I../

LIB=-L../static
INCLUDE=$(LEVELDB_INC) $(ROCKSDB_INC) $(TITAN_INC) $(LOCAL_INC)

CC=g++
CFLAGS=-std=c++11 -c -g -Wall -fgnu-tm
SOURCES=$(wildcard *.cc)
OBJECTS=$(SOURCES:.cc=.o)

all: $(SOURCES) $(OBJECTS)

.cc.o:
	$(CC) $(CFLAGS) $(INCLUDES) $< -o $@ $(INCLUDE) $(LIB)

clean:
	$(RM) $(OBJECTS)

