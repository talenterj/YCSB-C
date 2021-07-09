# LEVELDB_INC=-I/home/xp/flying/leveldb-cuda/include
ROCKSDB_INC=-I/home/dc/zcj/TRAID/include -I/home/dc/zcj/TRAID/
# ROCKSDB_INC=-I/home/dc/zcj/rocksdb4.13/include -I/home/dc/zcj/rocksdb4.13/
# TITAN_INC=-I/home/xp/flying/titan/include
LOCAL_INC=-I./

INCLUDE=$(ROCKSDB_INCLUDE) $(LOCAL_INC)

LIB=-L./static

CC=g++
CFLAGS=-std=c++11 #-pthread
LDFLAGS= -lpthread -lrocksdb  -lhdr_histogram -lrt -lsnappy -lgflags -lz -lbz2 -llz4 -lzstd -ld -ljemallocl
SUBDIRS= core db 
SUBSRCS=$(wildcard core/*.cc) $(wildcard db/*.cc)
OBJECTS=$(SUBSRCS:.cc=.o)
EXEC=ycsbc

all: $(SUBDIRS) $(EXEC)

$(SUBDIRS):
	$(MAKE) -C $@
	#$(MAKE) -C $@ ROCKSDB_INCLUDE=${ROCKSDB_INCLUDE}

$(EXEC): $(wildcard *.cc) $(OBJECTS)
	$(CC) $(CFLAGS) $^ $(LDFLAGS) -o $@ $(INCLUDE) $(LIB)
clean:
	for dir in $(SUBDIRS); do \
		$(MAKE) -C $$dir $@; \
	done
	$(RM) $(EXEC)

.PHONY: $(SUBDIRS) $(EXEC)

