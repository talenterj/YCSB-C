//
// Created by wujy on 1/23/19.
//
#include <iostream>


#include "rocksdb_db.h"
#include "rocksdb/status.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/table.h"
#include "lib/coding.h"
#include <hdr/hdr_histogram.h>
#include <cstdio>
#include <cstdlib>

using namespace std;

namespace ycsbc {

  //xp
  // output mean, 95th, 99th, 99.99th lat. of all operations every 1 second
  void RocksDB::latency_hiccup(uint64_t iops) {
    //fprintf(f_hdr_hiccup_output_, "mean     95th     99th     99.99th   IOPS");
    fflush(f_hdr_hiccup_output_);
  }

  RocksDB::RocksDB(const char *dbfilename, utils::Properties &props) :noResult(0){
    //cout << "hdr_ init success, &hdr_=" << &hdr_ << endl;

    //time_t curr_t = time(0);
    //char tmp[64];
    //std::strftime(tmp,
    //              sizeof(tmp),
    //              "%Y%m%d%H%M%S",
    //              std::localtime(&curr_t)); //xp: get the time
	//std::string t_str = tmp;
    //std::string f_name_lat_perc = "./hdr/rocksdb-lat-perc-" + t_str + ".output";
    //std::string f_name_lat_hiccup = "./hdr/rocksdb-lat-hiccup-" + t_str + ".output";
    //
	//strcpy(tmp, f_name_lat_perc.c_str());
    f_hdr_output_= std::fopen("./hdr/rocksdb-lat.hgrm", "w+");
    if(!f_hdr_output_) {
      std::perror("hdr output file opening failed");
      exit(0);
    }   
	//strcpy(tmp, f_name_lat_hiccup.c_str());
    f_hdr_hiccup_output_ = std::fopen("./hdr/rocksdb-lat.hiccup", "w+");
    if(!f_hdr_hiccup_output_) {
      std::perror("hdr hiccup output file opening failed");
      exit(0);
    }   
    fprintf(f_hdr_hiccup_output_, "#mean       95th    99th    99.99th    IOPS\n");

    //set option
    rocksdb::Options options;
    SetOptions(&options, props);
     
    rocksdb::Status s = rocksdb::DB::Open(options,dbfilename,&db_);
    if(!s.ok()){
        cout <<"Can't open rocksdb "<<dbfilename<<" "<<s.ToString()<<endl;
        exit(0);
    }
  }

    void RocksDB::SetOptions(rocksdb::Options *options, utils::Properties &props) {

        //// 默认的Rocksdb配置
        options->create_if_missing = true;
        options->compression = rocksdb::kNoCompression;
        options->enable_pipelined_write = true;
		rocksdb::BlockBasedTableOptions table_options;
        table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10));

        // a column family's max memtable size
        //  default 64MB
        options->write_buffer_size = 4 * 1024 * 1024;
        // sst file size
        options->target_file_size_base = 4 * 1024 * 1024 + 100*1024;
        // tune a large number, or loading 0.1 billion KVs
        //  will failed with IOError.
        options->max_open_files = 4096;
        // max # of cocurrent jobs (compact + flushes)
        //  default is 2.
        //  our Intel E5-1620 v4 has 8 logic cores
        options->max_background_jobs = 8;
        // The maximum number of write buffers that are built up in memory.
        // The default and the minimum number is 2, so that when 1 write buffer
        // is being flushed to storage, new writes can continue to the other
        // write buffer.
        // If max_write_buffer_number > 3, writing will be slowed down to
        // options.delayed_write_rate if we are writing to the last write buffer
        // allowed.
        //
        // Default: 2
        //options->max_write_buffer_number = 6;
        // Control maximum total data size for a level.
        // max_bytes_for_level_base is the max total for level-1.
        // Maximum number of bytes for level L can be calculated as
        // (max_bytes_for_level_base) * (max_bytes_for_level_multiplier ^ (L-1))
        // For example, if max_bytes_for_level_base is 200MB, and if
        // max_bytes_for_level_multiplier is 10, total data size for level-1
        // will be 200MB, total file size for level-2 will be 2GB,
        // and total file size for level-3 will be 20GB.
        //
        // Default: 256MB.
        //options->max_bytes_for_level_base = 256*1024*1024; //xp: leveldb 'MaxBytesForLevel()' 
        // Number of files to trigger level-0 compaction. A value <0 means that
        // level-0 compaction will not be triggered by number of files at all.
        //
        // Default: 4
		// level0_file_num_compaction_trigger -- Once level 0 reaches this 
		// number of files, L0->L1 compaction is triggered. 
		// We can therefore estimate level 0 size in stable state as 
		// write_buffer_size * min_write_buffer_number_to_merge *
		// level0_file_num_compaction_trigger
        //options->level0_file_num_compaction_trigger = 4;
        //options->level0_file_num_compaction_trigger = 64; // 1024MB
		//
        // Soft limit on number of level-0 files. We start slowing down writes at this
        // point. A value <0 means that no writing slow down will be triggered by
        // number of files in level-0.
        //
        // Default: 20
        //options->level0_slowdown_writes_trigger = 60;
        // Maximum number of level-0 files.  We stop writes at this point.
        //
        // Default: 36
        //options->level0_stop_writes_trigger = 64;


		
		// save with LevelDB
		//options->level0_file_num_compaction_trigger = 4;
		//options->level0_slowdown_writes_trigger = 8;     
		//options->level0_stop_writes_trigger = 12;

        ////

        int dboption = stoi(props["dboption"]);

        if ( dboption == 1) {  //RocksDB-L0-NVM: L0 have file path 
#ifdef ROCKSDB_L0_NVM 
            printf("set Rocksdb_L0_NVM options!\n");
            options->level0_file_num_compaction_trigger = 4;
            options->level0_slowdown_writes_trigger = 112;     
            options->level0_stop_writes_trigger = 128;
            options->level0_file_path = "/pmem/nvm";
#endif
        }
        else if ( dboption == 2 ) { //Matrixkv: 
#ifdef MATRIXKV 
            printf("set Matrixkv options!\n");
            rocksdb::NvmSetup* nvm_setup = new rocksdb::NvmSetup();
            nvm_setup->use_nvm_module = true;
            nvm_setup->pmem_path = "/pmem/nvm";
            options->nvm_setup.reset(nvm_setup);
            options->max_background_jobs = 3;
            options->max_bytes_for_level_base = 8ul * 1024 * 1024 * 1024;
#endif 
        }
        
    }


    int RocksDB::Read(const std::string &table, const std::string &key, const std::vector<std::string> *fields,
                      std::vector<KVPair> &result) {
        string value;
        uint64_t tx_begin_time = get_now_micros(); //xp: us
        rocksdb::Status s = db_->Get(rocksdb::ReadOptions(),key,&value);
        uint64_t tx_xtime = get_now_micros() - tx_begin_time;
        if(tx_xtime > 3600000000) {
          cout << "too large tx_xtime" << endl;
        }

        if(s.ok()) {
            //printf("value:%lu\n",value.size());
            DeSerializeValues(value, result);
            /* printf("get:key:%lu-%s\n",key.size(),key.data());
            for( auto kv : result) {
                printf("get field:key:%lu-%s value:%lu-%s\n",kv.first.size(),kv.first.data(),kv.second.size(),kv.second.data());
            } */
            return DB::kOK;
        }
        if(s.IsNotFound()){
            noResult++;
            //cerr<<"read not found:"<<noResult<<endl;
            return DB::kOK;
        }
		else{
            cout <<"RocksDB GET() ERROR! error string: "<< s.ToString() << endl;
			//cerr<<"RocksDB.stats: "<< s.code() << endl;
            //string stats;
            //db_->GetProperty("rocksdb.stats",&stats);
            //cout<<stats<<endl;
            exit(0);
        }
    }


    int RocksDB::Scan(const std::string &table, const std::string &key, int len, const std::vector<std::string> *fields,
                      std::vector<std::vector<KVPair>> &result) {
         auto it=db_->NewIterator(rocksdb::ReadOptions());
        it->Seek(key);
        std::string val;
        std::string k;
        //printf("len:%d\n",len);
        for(int i=0;i < len && it->Valid(); i++){
            k = it->key().ToString();
            val = it->value().ToString();
            //printf("i:%d key:%lu value:%lu\n",i,k.size(),val.size());
            it->Next();
        } 
        delete it;
        return DB::kOK;
    }


    int RocksDB::Insert(const std::string &table, const std::string &key,
                        std::vector<KVPair> &values){
        rocksdb::Status s;
        string value;
        SerializeValues(values,value);
        /* printf("put:key:%lu-%s\n",key.size(),key.data());
        for( auto kv : values) {
            printf("put field:key:%lu-%s value:%lu-%s\n",kv.first.size(),kv.first.data(),kv.second.size(),kv.second.data());
        } */
        uint64_t tx_begin_time = get_now_micros(); //xp: us
        s = db_->Put(rocksdb::WriteOptions(), key, value);
        uint64_t tx_xtime = get_now_micros() - tx_begin_time;
        if(tx_xtime > 3600000000) {
          cout << "too large tx_xtime" << endl;
        }

        if(!s.ok()){
            cout <<"RocksDB PUT() ERROR! error string: "<< s.ToString() << endl;
            exit(0);
        }
       
        return DB::kOK;
    }


    int RocksDB::Update(const std::string &table, const std::string &key, std::vector<KVPair> &values) {
        int s;
        uint64_t tx_begin_time = get_now_micros(); //xp: us
        s =  Insert(table,key,values);
        uint64_t tx_xtime = get_now_micros() - tx_begin_time;
        if(tx_xtime > 3600000000) {
          cout << "too large tx_xtime" << endl;
        }

		if(0 != s) { //xp: not OK
		  cout << "UPD() ERROR! error code: " << s << endl;
		}
        return s;
    }


    int RocksDB::Delete(const std::string &table, const std::string &key) {
        rocksdb::Status s;
        s = db_->Delete(rocksdb::WriteOptions(),key);
        if(!s.ok()){
            cout <<"RocksDB DEL() ERROR! error string: "<< s.ToString() << endl;
            exit(0);
        }
        return DB::kOK;
    }


    void RocksDB::PrintStats() {
      cout<<"read not found:"<<noResult<<endl;
      string stats;
      bool rt = db_->GetProperty("rocksdb.stats", &stats);
	  if(rt == false) {
	    cout << "RocksDB GetProperty() failed with return: " << rt << endl;
	  }
	  else {
	    //cout << "RocksDB GetProperty() success with return: " << rt << endl;
		cout << stats << endl;
	  }

      cout << "-------------------------------" << endl;
      cout << "SUMMARY latency (us) of this run with HDR measurement" << endl;
      cout << "         ALL        GET        PUT        UPD" << endl;

      cout << "-------------------------------" << endl;
    }

    bool RocksDB::HaveBalancedDistribution() {
		return true;
        //return db_->HaveBalancedDistribution();
    }

    RocksDB::~RocksDB() {
      printf("wait for closing and deleting RocksDB\n");
      rocksdb::Status s;
	  s = db_->Close();
	  if(!s.ok()) {
	    printf("RocksDB Close() failed!\n");
	  }
      delete db_;
      printf("~RocksDB() success, terminated.\n");
    }

    void RocksDB::SerializeValues(std::vector<KVPair> &kvs, std::string &value) {
        value.clear();
        PutFixed64(&value, kvs.size());
        for(unsigned int i=0; i < kvs.size(); i++){
            PutFixed64(&value, kvs[i].first.size());
            value.append(kvs[i].first);
            PutFixed64(&value, kvs[i].second.size());
            value.append(kvs[i].second);
        }
    }

    void RocksDB::DeSerializeValues(std::string &value, std::vector<KVPair> &kvs){
        uint64_t offset = 0;
        uint64_t kv_num = 0;
        uint64_t key_size = 0;
        uint64_t value_size = 0;

        kv_num = DecodeFixed64(value.c_str());
        offset += 8;
        for( unsigned int i = 0; i < kv_num; i++){
            ycsbc::DB::KVPair pair;
            key_size = DecodeFixed64(value.c_str() + offset);
            offset += 8;

            pair.first.assign(value.c_str() + offset, key_size);
            offset += key_size;

            value_size = DecodeFixed64(value.c_str() + offset);
            offset += 8;

            pair.second.assign(value.c_str() + offset, value_size);
            offset += value_size;
            kvs.push_back(pair);
        }
    }
}
