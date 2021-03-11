#include "leveldb/db.h"
#include <iostream>
#include <vector>
#include <sstream>
#include <thread>
#include <unordered_map>
#include <random>
#include <algorithm>

#include "bifurcated_leveldb_helper.h"

namespace leveldb {
    class BifurcatedLevelDB{
        private:
            std::vector<leveldb::DB*> DBs_;
            std::vector<leveldb::Options> DBOptions_;
            std::unordered_map<int, std::vector<int>> bindings_;
            int num_lsmts_;

            bool kv_separation_;
            FILE* vlog;

            int get_best_lsm(const std::vector<int>& lsms){
                for(int lsm: lsms){
                    if(this -> DBs_[lsm] -> GetNumL0Files() >= 4){
                        std::cout << "too many L0 files in " << lsm << ", checking next" << std::endl;
                    }
                    
                    // this next block is supposed to work but doesn't. commenting it out for now.

                    // else if(this -> DBs_[lsm] -> WaitingForBackgroundWork()){
                    //     std::cout << "Waiting for background activity in " << lsm << ", checking next" << std::endl;
                    //     // not sure this works as its supposed to
                    // }
                    else{
                        return lsm;
                    }
                }

                // if everything is busy, return it to its original lsm
                cout << "all lsms busy, sending to its original" << std::endl;
                return lsms.front();
            }

            int get_lsm_num(std::string key){
                // placeholder for now
                size_t hash = std::hash<std::string>{}(key);
                return get_best_lsm(bindings_[hash % 10]);
            }

            void generate_bindings(int total_lsms, int fallbacks){
            
                for (int i = 0; i < 10; i++){
                    bindings_[i] = std::vector<int>{i % total_lsms};

                    for(int j = 1; j <= fallbacks; j++){
                        bindings_[i].push_back((i + j) % total_lsms);
                    }
                }
            // no reason for the fallbacks to be contiguous, but this
            // seemed easiest to implement for now.
            }

            void print_bindings(){
                for(int i = 0; i < 10; i++){
                    std::cout << i << " : "; 
                    for(int x: bindings_[i]){
                        std::cout << x << " ";
                    }
                    std::cout << std::endl;
                }
            }

        public:
            BifurcatedLevelDB( int num_lsmts=5, int fallbacks = 2, bool kv_separation = true, std::string database_location = "./database"): num_lsmts_(num_lsmts){
                this -> DBs_ = std::vector<leveldb::DB*>(num_lsmts);
                this -> DBOptions_ = std::vector<leveldb::Options>(num_lsmts);

                for(int i = 0; i < num_lsmts; i++){
                    this -> DBOptions_[i].create_if_missing = true;
                    leveldb::Status status = leveldb::DB::Open(this -> DBOptions_[i], 
                                                            database_location + "_" + std::to_string(i),
                                                            &(this -> DBs_[i]));
                }

                this -> kv_separation_ = kv_separation;
                if(kv_separation_) this -> vlog = fopen("logfile", "wb+");

                std::cout << "[Bifurcated LevelDB] Created " << this -> num_lsmts_ << " LSMTs \n";
                
                generate_bindings(num_lsmts, fallbacks);
                print_bindings();
            } 

            void Put(std::string key, std::string value){
                int LSM_num = this -> get_lsm_num(key);

                if(kv_separation_){
                    // K-V separation

                    long offset = ftell(this->vlog);
                    long size = sizeof(value);
                    std::string vlog_offset = std::to_string(offset);
                    std::string vlog_size = std::to_string(size);
                    std::stringstream vlog_value;
                    vlog_value << vlog_offset << "&&" << vlog_size;
                    std::string s = vlog_value.str();
                    fwrite (&value, sizeof(value), 1, this->vlog);

                    // LevelDB Implementation
                    
                    // this -> _DBs[LSM_num] -> Put(leveldb::WriteOptions(), key, s);
                    leveldb_set(this -> DBs_[LSM_num], key, s);
                }

                else{
                    this -> DBs_[LSM_num] -> Put(leveldb::WriteOptions(), key, value);
                }
                
            }

            std::string Get(std::string key){
                // messed up updates if multiple lsmts
                int LSM_num = this -> get_lsm_num(key);

                if(kv_separation_){
                    string offsetinfo;
                    const bool found = leveldb_get(this -> DBs_[LSM_num], key, offsetinfo);
                    if (found) {

                    } else {
                        return "Record not found";
                    }

                    string value_offset;
                    string value_length;
                    string s = offsetinfo;
                    string delimiter = "&&";
                    size_t pos = 0;
                    string token;

                    while((pos = s.find(delimiter)) != string::npos) {
                        token = s.substr(0, pos);
                        value_offset = token;
                        s.erase(0, pos + delimiter.length());
                    }

                    value_length = s;

                    string::size_type sz;
                    long offset = stol(value_offset, &sz);
                    long length = stol(value_length, &sz);

                    string value_record;
                    fseek(this -> vlog, offset, SEEK_SET);
                    fread(&value_record, length, 1, this -> vlog);

                    return value_record;
                }

                else{
                    std::string result;
                    this -> DBs_[LSM_num] -> Get(leveldb::ReadOptions(), key, &result);
                    // if(!this -> DBs_[LSM_num]->WaitingForBackgroundWork()) std::cout <<"not waiting\n";
                    return result;
                } 
            }
        };
}



int main(){
    auto b = BifurcatedLevelDB(5);
    for(int i = 0; i < 1000000; i++){
        b.Put("ahelp", "me");
        // std::cout << i << std::endl;

    }
    std::cout << b.Get("ahelp");
    return 0;
}