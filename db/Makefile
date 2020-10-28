# LEVELDB_INC=-I/home/zhangxin/leveldb/include 
ROCKSDB_INC=-I/home/zhangxin/rocksdb-zx/include -I/home/zhangxin/rocksdb-zx/
LOCAL_INC=-I../

INCLUDE=$(ROCKSDB_INC) $(LOCAL_INC)

LIB=-L/home/zhangxin/rocksdb-zx

CC=g++
CFLAGS=-std=c++11 -c -g -Wall -fgnu-tm
SOURCES=$(wildcard *.cc)
OBJECTS=$(SOURCES:.cc=.o)

all: $(SOURCES) $(OBJECTS)

.cc.o:
	$(CC) $(CFLAGS) $(INCLUDES) $< -o $@ $(INCLUDE) $(LIB)

clean:
	$(RM) $(OBJECTS)