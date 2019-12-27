//
// 
//

#ifndef YCSB_C_ROCKSDB_DB_H
#define YCSB_C_ROCKSDB_DB_H

#include "core/db.h"
#include <iostream>
#include <string>
#include "core/properties.h"
#include <rocksdb/db.h>
#include "unistd.h"
#include <hdr/hdr_histogram.h>
#include <fstream>
#include <sys/time.h>

using std::cout;
using std::endl;

namespace ycsbc {
    class RocksDB : public DB{
    public :
        //hdr histogram
        struct hdr_histogram* hdr_ = NULL;
        struct hdr_histogram* hdr_last_1s_ = NULL;
        struct hdr_histogram* hdr_get_= NULL;
        struct hdr_histogram* hdr_put_= NULL;
        struct hdr_histogram* hdr_update_ = NULL;
        struct hdr_histogram* hdr_scan_ = NULL;
        struct hdr_histogram* hdr_delete_ = NULL;
        //int wrapper_hdr_init(struct hdr_histogram** h);
        void latency_hiccup(uint64_t iops);
        // hdr output files in ./hdr
        std::FILE* f_hdr_output_; //xp: hdr histogram of this run
        std::FILE* f_hdr_hiccup_output_; //xp: 95th lat. of all ops in every 1 second

        RocksDB(const char *dbfilename, utils::Properties &props);
        int Read(const std::string &table, const std::string &key,
                 const std::vector<std::string> *fields,
                 std::vector<KVPair> &result);

        int Scan(const std::string &table, const std::string &key,
                 int len, const std::vector<std::string> *fields,
                 std::vector<std::vector<KVPair>> &result);

        int Insert(const std::string &table, const std::string &key,
                   std::vector<KVPair> &values);

        int Update(const std::string &table, const std::string &key,
                   std::vector<KVPair> &values);


        int Delete(const std::string &table, const std::string &key);

        void PrintStats();

        bool HaveBalancedDistribution();

        uint64_t get_now_micros(){
            struct timeval tv;
            gettimeofday(&tv, NULL);
            return (tv.tv_sec) * 1000000 + tv.tv_usec;
        }


        ~RocksDB();

    private:
        rocksdb::DB *db_;
        unsigned noResult;

        void SetOptions(rocksdb::Options *options, utils::Properties &props);
        void SerializeValues(std::vector<KVPair> &kvs, std::string &value);
        void DeSerializeValues(std::string &value, std::vector<KVPair> &kvs);

    };
}


#endif //YCSB_C_ROCKSDB_DB_H
