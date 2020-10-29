# LEVELDB_INC=-I/home/zhangxin/leveldb/include 
ROCKSDB_INC=-I/home/zhangxin/rocksdb-zx/include -I/home/zhangxin/rocksdb-zx/
LOCAL_INC=-I./

INCLUDE=$(ROCKSDB_INC) $(LOCAL_INC)

LIB=-L/home/zhangxin/rocksdb-zx

CC=g++
CFLAGS=-std=c++11 -g -Wall -pthread -I./
LDFLAGS=-lpthread -lrt -lsnappy -lgflags -lz -lbz2 -llz4 -lzstd -ldl -lrocksdb #-lleveldb
SUBDIRS=core db
SUBSRCS=$(wildcard core/*.cc) $(wildcard db/*.cc)
OBJECTS=$(SUBSRCS:.cc=.o)
EXEC=ycsbc

all: $(SUBDIRS) $(EXEC)

$(SUBDIRS):
	$(MAKE) -C $@

$(EXEC): $(wildcard *.cc) $(OBJECTS)
	$(CC) $(CFLAGS) $^ $(LDFLAGS) -o $@ $(INCLUDE) $(LIB)

clean:
	for dir in $(SUBDIRS); do \
		$(MAKE) -C $$dir $@; \
	done
	$(RM) $(EXEC)

.PHONY: $(SUBDIRS) $(EXEC)