//
// Created by wujy on 18-1-21.
//


#include <iostream>

#include "leveldb_db.h"
#include "leveldb/filter_policy.h"
#include "lib/coding.h"
#include <cstdio>
#include <cstdlib>
//#include <time.h>
#include <string>

using namespace std;

namespace ycsbc {

  //xp
  // output mean, 95th, 99th, 99.99th lat. and IOPS of all operations every 1 second
  void LevelDB::latency_hiccup(uint64_t iops) {
    //fprintf(f_hdr_hiccup_output_, "mean     95th     99th     99.99th   IOPS");
    fprintf(f_hdr_hiccup_output_, "%-11.2lf %-8ld %-8ld %-8ld %-8ld\n",
              hdr_mean(hdr_last_1s_),
              hdr_value_at_percentile(hdr_last_1s_, 95),
              hdr_value_at_percentile(hdr_last_1s_, 99),
              hdr_value_at_percentile(hdr_last_1s_, 99.99),
			  iops);
    hdr_reset(hdr_last_1s_);
    fflush(f_hdr_hiccup_output_);
  }

    LevelDB::LevelDB(const char *dbfilename, utils::Properties &props) :noResult(0){

      //init hdr
      //cerr << "DEBUG- init histogram &=" << &hdr_ << endl;
      int r = hdr_init(
        1,  // Minimum value
        INT64_C(3600000000),  // Maximum value
        3,  // Number of significant figures
        &hdr_);  // Pointer to initialise
      r |= hdr_init(1, INT64_C(3600000000), 3, &hdr_last_1s_);
      r |= hdr_init(1, INT64_C(3600000000), 3, &hdr_get_);
      r |= hdr_init(1, INT64_C(3600000000), 3, &hdr_put_);
      r |= hdr_init(1, INT64_C(3600000000), 3, &hdr_update_);
     
      if((0 != r) ||
         (NULL == hdr_) ||
         (NULL == hdr_last_1s_) ||
         (NULL == hdr_get_) ||
         (NULL == hdr_put_) ||
         (NULL == hdr_update_) ||
         (23552 < hdr_->counts_len)) {
        cerr << "DEBUG- init hdrhistogram failed." << endl;
        cerr << "DEBUG- r=" << r << endl;
        cerr << "DEBUG- histogram=" << &hdr_ << endl;
        cerr << "DEBUG- counts_len=" << hdr_->counts_len << endl;
        cerr << "DEBUG- counts:" << hdr_->counts << ", total_c:" << hdr_->total_count << endl;
        cerr << "DEBUG- lowest:" << hdr_->lowest_trackable_value << ", max:" <<hdr_->highest_trackable_value << endl;
        free(hdr_);
        exit(0);
      }
      //cout << "hdr_ init success, &hdr_=" << &hdr_ << endl;

	  //time_t curr_t = time(0);
	  //char tmp[32];
	  //strftime(tmp,
	  //              sizeof(tmp),
	  //  			"%Y%m%d%H%M%S",
	  //  			localtime(&curr_t)); //xp: get the time
	  //std::string t_str = tmp;
	  //std::string f_name_lat_perc = "./hdr/cuda-lat-perc-" + t_str + ".output";
	  //std::string f_name_lat_hiccup = "./hdr/cuda-lat-hiccup-" + t_str + ".output";
	  //
	  //strcpy(tmp, f_name_lat_perc.c_str());
      f_hdr_output_= std::fopen("./hdr/cuda-lat.hgrm", "w+");
      if(!f_hdr_output_) {
        std::perror("hdr output file opening failed");
        exit(0);
      }
	  //strcpy(tmp, f_name_lat_hiccup.c_str());
      f_hdr_hiccup_output_ = std::fopen("./hdr/cuda-lat.hiccup", "w+");
      if(!f_hdr_hiccup_output_) {
        std::perror("hdr hiccup output file opening failed");
        exit(0);
      }
      fprintf(f_hdr_hiccup_output_, "#mean       95th    99th    99.99th   IOPS\n");

        
        //set option
        leveldb::Options options;
        SetOptions(&options, props);

#ifndef NOVELSM
        leveldb::Status s = leveldb::DB::Open(options, dbfilename, &db_);

#else   //novelsm
        std::string db_mem = "/pmem/nvm";
        leveldb::Status s = leveldb::DB::Open(options,dbfilename, db_mem, &db_);  //novelsm
#endif
        if(!s.ok()){
            cerr<<"Can't open leveldb "<<dbfilename<<" "<<s.ToString()<<endl;
            exit(0);
        }
    }


    void LevelDB::SetOptions(leveldb::Options *options, utils::Properties &props) {

        //// 默认的Leveldb配置
        options->create_if_missing = true;
        options->compression = leveldb::kNoCompression;
		//xp: keep write_buffer_size == max_file_size
		options->write_buffer_size = 4 * 1024 * 1024;
		//options->max_file_size = 4 * 1024 * 1024 + 100* 1024; //xp: leveldb-1.22
		options->max_file_size = 4 * 1024 * 1024; //xp: cuda
		options->filter_policy = leveldb::NewBloomFilterPolicy(10);

        ////

        int dboption = stoi(props["dboption"]);

        if ( dboption == 1) {  //Novelsm options
#ifdef NOVELSM 
            printf("set Novelsm options!\n");
            //options->db_mem = "/pmem/nvm";
            options->write_buffer_size = 64ul * 1024 * 1024;
            options->nvm_buffer_size = 4096ul * 1024 * 1024;
            options->num_levels = 2;
            // /options->cnsdl = 0;
#endif
        }
        
    }

    int LevelDB::Read(const std::string &table, const std::string &key, const std::vector<std::string> *fields,
                      std::vector<KVPair> &result) {
        string value;
      uint64_t tx_begin_time = get_now_micros(); //xp: us
        leveldb::Status s = db_->Get(leveldb::ReadOptions(),key,&value);
      uint64_t tx_xtime = get_now_micros() - tx_begin_time;
        if(tx_xtime > 3600000000) {
          cerr << "too large tx_xtime" << endl;
        } else {
          //cout << "xtime: " << tx_xtime << endl;
	  //cout << "DEBUG- &hdr_: " << &hdr_<< endl;
          hdr_record_value(hdr_, tx_xtime);
          hdr_record_value(hdr_last_1s_, tx_xtime);
          hdr_record_value(hdr_get_, tx_xtime);
          //cout << "ops: " << hdrhistogram->total_count << endl;
        }
        //printf("read:key:%lu-%s [%lu]\n",key.size(),key.data(),value.size());
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
        }else{
            cerr<<"read error"<<endl;
            exit(0);
        }
    }


    int LevelDB::Scan(const std::string &table, const std::string &key, int len, const std::vector<std::string> *fields,
                      std::vector<std::vector<KVPair>> &result) {
        auto it=db_->NewIterator(leveldb::ReadOptions());
        it->Seek(key);
        std::string val;
        std::string k;
        for(int i=0;i<len && it->Valid();i++){
            k = it->key().ToString();
            val = it->value().ToString();
            it->Next();
        }
        delete it;
        return DB::kOK;
    }

    int LevelDB::Insert(const std::string &table, const std::string &key,
               std::vector<KVPair> &values){
        leveldb::Status s;
        string value;
        SerializeValues(values,value);
        //printf("put:key:%lu-%s [%lu]\n",key.size(),key.data(),value.size());
        /* for( auto kv : values) {
            printf("put field:key:%lu-%s value:%lu-%s\n",kv.first.size(),kv.first.data(),kv.second.size(),kv.second.data());
        } */ 
        
      uint64_t tx_begin_time = get_now_micros(); //xp: us
        s = db_->Put(leveldb::WriteOptions(), key, value);
      uint64_t tx_xtime = get_now_micros() - tx_begin_time;
        if(tx_xtime > 3600000000) {
          cerr << "too large tx_xtime" << endl;
        } else {
          hdr_record_value(hdr_, tx_xtime);
          hdr_record_value(hdr_last_1s_, tx_xtime);
          hdr_record_value(hdr_put_, tx_xtime);
        }
        if(!s.ok()){
            //cerr<<"insert ERROR! error code: " << s.code() << endl;
            cerr<<"CUDA PUT() ERROR!" << endl;
            exit(0);
        }
        
        return DB::kOK;
    }

    int LevelDB::Update(const std::string &table, const std::string &key, std::vector<KVPair> &values) {
  int s;
        uint64_t tx_begin_time = get_now_micros(); //xp: us
        s =  Insert(table,key,values);
        uint64_t tx_xtime = get_now_micros() - tx_begin_time;
        if(tx_xtime > 3600000000) {
          cerr << "too large tx_xtime" << endl;
        } else {
          hdr_record_value(hdr_, tx_xtime);
          hdr_record_value(hdr_last_1s_, tx_xtime);
          hdr_record_value(hdr_update_, tx_xtime);
        }
        return s;
    }

    int LevelDB::Delete(const std::string &table, const std::string &key) {
        leveldb::Status s;
        s = db_->Delete(leveldb::WriteOptions(),key);
        if(!s.ok()){
            cerr<<"Delete error\n"<<endl;
            exit(0);
        }
        return DB::kOK;
    }

    void LevelDB::PrintStats() {
      cout<<"read not found:"<<noResult<<endl;
      string stats;
      int rt = db_->GetProperty("leveldb.stats",&stats);
	  if (false == rt) {
	    cout << "LevelDB GetProperty() failed." << endl;
	  }
	  else {
	    //cout << "LevelDB GetProperty() success." << endl;
	    cout << stats << endl;
	  }

      cout << "-------------------------------" << endl;
      cout << "SUMMARY latency (us) of this run with HDR measurement" << endl;
	  cout << "         ALL        GET        PUT        UPD" << endl;
      fprintf(stdout, "mean     %-10lf %-10lf %-10lf %-10lf\n",
                hdr_mean(hdr_),
                hdr_mean(hdr_get_),
                hdr_mean(hdr_put_),
                hdr_mean(hdr_update_));
      fprintf(stdout, "95th     %-10ld %-10ld %-10ld %-10ld\n",
                hdr_value_at_percentile(hdr_, 95),
                hdr_value_at_percentile(hdr_get_, 95),
                hdr_value_at_percentile(hdr_put_, 95),
                hdr_value_at_percentile(hdr_update_, 95));
      fprintf(stdout, "99th     %-10ld %-10ld %-10ld %-10ld\n",
                hdr_value_at_percentile(hdr_, 99),
                hdr_value_at_percentile(hdr_get_, 99),
                hdr_value_at_percentile(hdr_put_, 99),
                hdr_value_at_percentile(hdr_update_, 99));
      fprintf(stdout, "99.99th  %-10ld %-10ld %-10ld %-10ld\n",
                hdr_value_at_percentile(hdr_, 99.99),
                hdr_value_at_percentile(hdr_get_, 99.99),
                hdr_value_at_percentile(hdr_put_, 99.99),
                hdr_value_at_percentile(hdr_update_, 99.99));

      int ret = hdr_percentiles_print(
              hdr_,
              f_hdr_output_,  // File to write to
              5,  // Granularity of printed values
              1.0,  // Multiplier for results
              CLASSIC);  // Format CLASSIC/CSV supported.
      if(0 != ret) {
        cerr << "hdr output print file error!" <<endl;
      }
      cout << "-------------------------------" << endl;
    }

    bool LevelDB::HaveBalancedDistribution() {
        //return db_->HaveBalancedDistribution();
		return true;
    }

    LevelDB::~LevelDB() {
      free(hdr_);
      free(hdr_last_1s_);
      free(hdr_get_);
      free(hdr_put_);
      free(hdr_update_);
        delete db_;
    }

    void LevelDB::SerializeValues(std::vector<KVPair> &kvs, std::string &value) {
        value.clear();
        PutFixed64(&value, kvs.size());
        for(unsigned int i=0; i < kvs.size(); i++){
            PutFixed64(&value, kvs[i].first.size());
            value.append(kvs[i].first);
            PutFixed64(&value, kvs[i].second.size());
            value.append(kvs[i].second);
        }
    }

    void LevelDB::DeSerializeValues(std::string &value, std::vector<KVPair> &kvs){
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
