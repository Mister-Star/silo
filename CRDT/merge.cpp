//
// Created by Star on 2025/3/24.
//

#include "merge.h"
#include "epoch_manager.h"

/// [shard][epoch]
std::vector<std::shared_ptr<std::vector<std::shared_ptr<
        concurrent_crdt_unordered_map<std::string, std::string, std::string>>>>>
        Merge::epoch_merge_map_vec,
        Merge::epoch_abort_map_vec;

///[epoch]
std::vector<std::shared_ptr<
        concurrent_crdt_unordered_map<std::string, std::string, std::string>>>
        Merge::epoch_global_abort_map_vec;

/// [shard][epoch]
std::vector<std::shared_ptr<std::vector<std::shared_ptr<
        concurrent_unordered_map<std::string, std::shared_ptr<CRDTTransaction>>>>>>
Merge::epoch_txn_map_vec;

///[shard]
std::vector<std::shared_ptr<concurrent_unordered_map<std::string, std::string>>>
        Merge::read_version_map_data_vec,
        Merge::read_version_map_csn_vec,
        Merge::insert_set_vec;

/// [shard][epoch]
std::vector<std::shared_ptr<std::vector<std::shared_ptr<
        BlockingConcurrentQueue<std::shared_ptr<CRDTTransaction>>>>>>
    Merge::epoch_read_validate_queue_vec,
    Merge::epoch_merge_queue_vec,
    Merge::epoch_commit_queue_vec,
    Merge::epoch_redo_log_queue_vec,
    Merge::epoch_result_return_queue_vec;

std::vector<std::shared_ptr<
        BlockingConcurrentQueue<std::shared_ptr<CRDTTransaction>>>>
    Merge::epoch_result_returned_queue_vec;

std::vector<std::shared_ptr<std::atomic<uint64_t>>> Merge::shard_init_flag;

void Merge::Init() { /// [shard][epoch]
    auto max_length = CRDTContext::kCacheMaxLength;
    auto shard =  CRDTContext::kShardNum;

    epoch_merge_map_vec.resize(shard);
    epoch_abort_map_vec.resize(shard);

    epoch_global_abort_map_vec.resize(max_length);

    epoch_txn_map_vec.resize(shard);

    read_version_map_data_vec.resize(shard);
    read_version_map_csn_vec.resize(shard);
    insert_set_vec.resize(shard);

    epoch_read_validate_queue_vec.resize(shard);
    epoch_merge_queue_vec.resize(shard);
    epoch_commit_queue_vec.resize(shard);
    epoch_redo_log_queue_vec.resize(shard);
    epoch_result_return_queue_vec.resize(shard);

    epoch_result_returned_queue_vec.resize(shard);

    shard_init_flag.resize(shard);

    for(int i = 0; i < static_cast<int>(max_length); i ++) {
        epoch_global_abort_map_vec[i] = std::make_shared<concurrent_crdt_unordered_map<std::string, std::string, std::string>>();
    }

    for(int i = 0; i < static_cast<int>(shard); i ++) {
        shard_init_flag[i] = std::make_shared<std::atomic<uint64_t>>(0);
    }
}

void Merge::ShardInit(const uint64_t &shard) { /// [shard][epoch]
    if(shard_init_flag[shard]->fetch_add(1) != 0)
        return ;
    auto max_length = CRDTContext::kCacheMaxLength;

    std::cerr << "Merge::ShardInit  shard_id " << shard << std::endl;

    epoch_merge_map_vec[shard] = std::make_shared<std::vector<std::shared_ptr<
            concurrent_crdt_unordered_map<std::string, std::string, std::string>>>>();
    epoch_abort_map_vec[shard] = std::make_shared<std::vector<std::shared_ptr<
            concurrent_crdt_unordered_map<std::string, std::string, std::string>>>>();

    epoch_txn_map_vec[shard] = std::make_shared<std::vector<std::shared_ptr<
            concurrent_unordered_map<std::string, std::shared_ptr<CRDTTransaction>>>>>();

    read_version_map_data_vec[shard] = std::make_shared<concurrent_unordered_map<std::string, std::string>>();
    read_version_map_csn_vec[shard] = std::make_shared<concurrent_unordered_map<std::string, std::string>>();
    insert_set_vec[shard] = std::make_shared<concurrent_unordered_map<std::string, std::string>>();

    epoch_read_validate_queue_vec[shard] = std::make_shared<std::vector<std::shared_ptr<
            BlockingConcurrentQueue<std::shared_ptr<CRDTTransaction>>>>>();
    epoch_merge_queue_vec[shard] = std::make_shared<std::vector<std::shared_ptr<
            BlockingConcurrentQueue<std::shared_ptr<CRDTTransaction>>>>>();
    epoch_commit_queue_vec[shard] = std::make_shared<std::vector<std::shared_ptr<
            BlockingConcurrentQueue<std::shared_ptr<CRDTTransaction>>>>>();
    epoch_redo_log_queue_vec[shard] = std::make_shared<std::vector<std::shared_ptr<
            BlockingConcurrentQueue<std::shared_ptr<CRDTTransaction>>>>>();
    epoch_result_return_queue_vec[shard] = std::make_shared<std::vector<std::shared_ptr<
            BlockingConcurrentQueue<std::shared_ptr<CRDTTransaction>>>>>();

    std::cerr << "Merge::ShardInit  resize shard_id " << shard << std::endl;

    epoch_merge_map_vec[shard]->resize(max_length);
    epoch_abort_map_vec[shard]->resize(max_length);

    epoch_txn_map_vec[shard]->resize(max_length);

    epoch_read_validate_queue_vec[shard]->resize(max_length);
    epoch_merge_queue_vec[shard]->resize(max_length);
    epoch_commit_queue_vec[shard]->resize(max_length);
    epoch_redo_log_queue_vec[shard]->resize(max_length);
    epoch_result_return_queue_vec[shard]->resize(max_length);


    for(int i = 0; i < static_cast<int>(max_length); i ++) {
        (*epoch_merge_map_vec[shard])[i] = std::make_unique<concurrent_crdt_unordered_map<std::string, std::string, std::string>>();
        (*epoch_abort_map_vec[shard])[i] = std::make_unique<concurrent_crdt_unordered_map<std::string, std::string, std::string>>();

        (*epoch_txn_map_vec[shard])[i] = std::make_unique<concurrent_unordered_map<std::string, std::shared_ptr<CRDTTransaction>>>();

        (*epoch_read_validate_queue_vec[shard])[i] = std::make_unique<BlockingConcurrentQueue<std::shared_ptr<CRDTTransaction>>>();
        (*epoch_merge_queue_vec[shard])[i] = std::make_unique<BlockingConcurrentQueue<std::shared_ptr<CRDTTransaction>>>();
        (*epoch_commit_queue_vec[shard])[i] = std::make_unique<BlockingConcurrentQueue<std::shared_ptr<CRDTTransaction>>>();
        (*epoch_redo_log_queue_vec[shard])[i] = std::make_unique<BlockingConcurrentQueue<std::shared_ptr<CRDTTransaction>>>();
        (*epoch_result_return_queue_vec[shard])[i] = std::make_unique<BlockingConcurrentQueue<std::shared_ptr<CRDTTransaction>>>();
    }

    epoch_result_returned_queue_vec[shard] = std::make_shared<
            BlockingConcurrentQueue<std::shared_ptr<CRDTTransaction>>>();

    std::cerr << "Merge::ShardInit  end shard_id " << shard << std::endl;
}

void Merge::EpochClear(uint64_t &epoch) {
    auto epoch_mod_temp = epoch % CRDTContext::kCacheMaxLength;

    for(int i = 0; i < static_cast<int>(CRDTContext::kShardNum); i ++) {
        (*epoch_merge_map_vec[i])[epoch_mod_temp]->clear();
        (*epoch_abort_map_vec[i])[epoch_mod_temp]->clear();

        (*epoch_txn_map_vec[i])[epoch_mod_temp]->clear();

        epoch_global_abort_map_vec[epoch_mod_temp]->clear();
    }

    ///must be empty
//    epoch_read_validate_queue[i]
//    epoch_merge_queue[i]
//    epoch_commit_queue[i]
//    epoch_redo_log_queue[i]
//    epoch_result_return_queue[i]
}


void Merge::MergeThreadLocalInit(const uint64_t &shard) {

    std::cerr <<"Merge::MergeThreadLocalInit shard_id " << shard << std::endl;
    shard_id = shard;
    max_length = CRDTContext::kCacheMaxLength;

    epoch_should_read_validate_txn_num_shard = CRDTCounters::epoch_should_read_validate_txn_num_vec[shard];
    epoch_read_validated_txn_num_shard = CRDTCounters::epoch_read_validated_txn_num_vec[shard];
    epoch_should_merge_txn_num_shard = CRDTCounters::epoch_should_merge_txn_num_vec[shard];
    epoch_merged_txn_num_shard = CRDTCounters::epoch_merged_txn_num_vec[shard];
    epoch_should_commit_txn_num_shard = CRDTCounters::epoch_should_commit_txn_num_vec[shard];
    epoch_committed_txn_num_shard = CRDTCounters::epoch_committed_txn_num_vec[shard];
    epoch_record_commit_txn_num_shard = CRDTCounters::epoch_record_commit_txn_num_vec[shard];
    epoch_record_committed_txn_num_shard = CRDTCounters::epoch_record_committed_txn_num_vec[shard];
    epoch_result_return_txn_num_shard = CRDTCounters::epoch_result_return_txn_num_vec[shard];
    epoch_result_returned_txn_num_shard = CRDTCounters::epoch_result_returned_txn_num_vec[shard];

    total_merge_txn_num_shard = CRDTCounters::total_merge_txn_num_vec[shard];
    total_merge_latency_shard = CRDTCounters::total_merge_latency_vec[shard];
    total_commit_txn_num_shard = CRDTCounters::total_commit_txn_num_vec[shard];
    total_commit_latency_shard = CRDTCounters::total_commit_latency_vec[shard];
    success_commit_txn_num_shard = CRDTCounters::success_commit_txn_num_vec[shard];
    success_commit_latency_shard = CRDTCounters::success_commit_latency_vec[shard];
    total_read_version_check_failed_txn_num_shard = CRDTCounters::total_read_version_check_failed_txn_num_vec[shard];
    total_failed_txn_num_shard = CRDTCounters::total_failed_txn_num_vec[shard];

    epoch_merge_map_shard = epoch_merge_map_vec[shard];
    epoch_abort_map_shard = epoch_abort_map_vec[shard];

    epoch_txn_map_shard = epoch_txn_map_vec[shard];
    read_version_map_data_shard = read_version_map_data_vec[shard];
    read_version_map_csn_shard = read_version_map_csn_vec[shard];
    insert_set_shard = insert_set_vec[shard];
    epoch_read_validate_queue_shard = epoch_read_validate_queue_vec[shard];
    epoch_merge_queue_shard = epoch_merge_queue_vec[shard];
    epoch_commit_queue_shard = epoch_commit_queue_vec[shard];
    epoch_redo_log_queue_shard = epoch_redo_log_queue_vec[shard];
    epoch_result_return_queue_shard = epoch_result_return_queue_vec[shard];

    epoch_result_returned_queue_shard = epoch_result_returned_queue_vec[shard];
}


void Merge::ReadValidateQueueEnqueue(const uint64_t &shard_, const uint64_t &epoch_,
                                     const uint64_t &max_length_, const std::shared_ptr<CRDTTransaction> &txn_ptr_){
    auto epoch_mod_temp = epoch_ % max_length_;
    std::cerr << "ReadValidateQueueEnqueue shard_id " << shard_ << " epoch " << epoch_ << " epoch_mod " << epoch_mod_temp << " enqueue a txn" << std::endl;
    CRDTCounters::epoch_should_read_validate_txn_num_vec[shard_]->IncCount(epoch_mod_temp, 1);
    (*epoch_read_validate_queue_vec[shard_])[epoch_mod_temp]->enqueue(txn_ptr_);
    (*epoch_read_validate_queue_vec[shard_])[epoch_mod_temp]->enqueue(nullptr);
}
void Merge::MergeQueueEnqueue(const uint64_t &shard_, const uint64_t &epoch_,
                              const uint64_t &max_length_, const std::shared_ptr<CRDTTransaction> &txn_ptr_){
    auto epoch_mod_temp = epoch_ % max_length_;
    std::cerr << "MergeQueueEnqueue shard_id " << shard_ << " epoch " << epoch_ << " epoch_mod " << epoch_mod_temp << " enqueue a txn" << std::endl;
    CRDTCounters::epoch_should_merge_txn_num_vec[shard_]->IncCount(epoch_mod_temp, 1);
    (*epoch_merge_queue_vec[shard_])[epoch_mod_temp]->enqueue(txn_ptr_);
    (*epoch_merge_queue_vec[shard_])[epoch_mod_temp]->enqueue(nullptr);
}
void Merge::CommitQueueEnqueue(const uint64_t &shard_, const uint64_t &epoch_,
                               const uint64_t &max_length_, const std::shared_ptr<CRDTTransaction> &txn_ptr_){
    auto epoch_mod_temp = epoch_ % max_length_;
    CRDTCounters::epoch_should_commit_txn_num_vec[shard_]->IncCount(epoch_mod_temp, 1);
    (*epoch_commit_queue_vec[shard_])[epoch_mod_temp]->enqueue(txn_ptr_);
    (*epoch_commit_queue_vec[shard_])[epoch_mod_temp]->enqueue(nullptr);
}
void Merge::ResultReturnQueueEnqueue(const uint64_t &shard_, const uint64_t &epoch_,
                                     const uint64_t &max_length_, const std::shared_ptr<CRDTTransaction> &txn_ptr_){
    auto epoch_mod_temp = epoch_ % max_length_;
    CRDTCounters::epoch_result_return_txn_num_vec[shard_]->IncCount(epoch_mod_temp, 1);
    (*epoch_result_return_queue_vec[shard_])[epoch_mod_temp]->enqueue(txn_ptr_);
    (*epoch_result_return_queue_vec[shard_])[epoch_mod_temp]->enqueue(nullptr);
}


bool Merge::ValidateReadSet() {
    ///RR & SI
    ///RC do not check read data
//    epoch_mod = txn_ptr->cen % max_length;
//    auto time1 = now_to_us();
    result = true;
    for(auto &i : txn_ptr->read_set) {
        auto &key = i.first;
        csn_temp = std::to_string(i.second.csn);
        if (!read_version_map_csn_shard->getValue(key, csn_version)) {
            /// should be abort, but Taas do not connect load data,
            /// so read the init snap will get empty in read_version_map
            continue;
        }
        if (csn_version != csn_temp) {
//                continue; ///only for debug
            (*epoch_abort_map_shard)[epoch_mod]->insert(csn_temp, csn_temp);
            epoch_global_abort_map_vec[epoch_mod]->insert(csn_temp, csn_temp);
            result =  false;
            break;
        }
    }
    if (!result) {
        total_read_version_check_failed_txn_num_shard->IncCount(epoch_mod, 1);
    }
    epoch_read_validated_txn_num_shard->IncCount(epoch_mod, 1);
    return result;
}

bool Merge::CRDTMerge() {
    auto time1 = now_to_us();
    csn_temp = std::to_string(txn_ptr->csn);
    csn_result = "";
    result = true;
    for(auto &i : txn_ptr->write_set) {
        auto &key = i.first;
        auto &row = i.second;
        if (!(*epoch_merge_map_shard)[epoch_mod]->insert(key, csn_temp, csn_result)) {
            (*epoch_abort_map_shard)[epoch_mod]->insert(csn_temp, csn_temp);
            epoch_global_abort_map_vec[epoch_mod]->insert(csn_temp, csn_temp);
            result = false;
        }
    }
    total_merge_txn_num_shard->IncCount(epoch_mod, 1);
    total_merge_latency_shard->IncCount(epoch_mod, now_to_us() - time1);
    epoch_merged_txn_num_shard->IncCount(epoch_mod, 1);
    return result;
}


bool Merge::Commit() {
    csn_temp = std::to_string(txn_ptr->csn);
    csn_result = "";
    result = true;
    if((*epoch_abort_map_shard)[epoch_mod]->contain(csn_temp, csn_temp)) {
        result = false;
    }
    if (result) {
        for(auto &i : txn_ptr->write_set) {
            auto &key = i.first;
            auto &row = i.second;
            if(row.op_type == OpType::Insert) {
                insert_set_shard->insert(key, csn_temp);
            }
            else if(row.op_type == OpType::Delete) {
                insert_set_shard->remove(key, csn_temp);
            }
            else {
                //nothing to do
            }
            read_version_map_csn_shard->insert(key, csn_temp);

            ///todo:: update to data stores.
        }
    }
    epoch_committed_txn_num_shard->IncCount(epoch_mod, 1);
    epoch_record_committed_txn_num_shard->IncCount(epoch_mod, 1);
    return result;
}

void Merge::EpochMerge() {
    epoch = EpochManager::GetLogicalEpoch();
    while (!EpochManager::IsTimerStop()) {
        sleep_flag = true;
        epoch = EpochManager::GetLogicalEpoch();
        epoch_mod = epoch % max_length;

        while((*epoch_read_validate_queue_shard)[epoch_mod]->try_dequeue(txn_ptr)) { /// only local txn do this procedure
            if (txn_ptr != nullptr) {
                std::cerr << "merge dequeue read_validate" << shard_id << " epoch " << epoch << std::endl;
                ValidateReadSet();
                txn_ptr.reset();
                sleep_flag = false;
            }
        }

        if(!EpochManager::IsEpochMergeComplete(epoch)) {
//            std::cerr << "merge try_dequeue epoch_merge_queue_shard" << shard_id << " epoch " << epoch << std::endl;
            while ((*epoch_merge_queue_shard)[epoch_mod]->try_dequeue(txn_ptr)) {
                std::cerr << "merge dequeue CRDTMerge" << shard_id << " epoch " << epoch << std::endl;
                if (txn_ptr != nullptr) {
                    std::cerr << "merge dequeue sharded_txn CRDTMerge"<< std::endl;
                    CRDTMerge();
                    txn_ptr.reset();
                    sleep_flag = false;
                }
            }
        }

        if(EpochManager::IsAbortSetMergeComplete(epoch) && !EpochManager::IsCommitComplete(epoch)) {
//            std::cerr << "merge try_dequeue epoch_commit_queue_shard" << shard_id << " epoch " << epoch << std::endl;
            while (!EpochManager::IsCommitComplete(epoch) &&
                    (*epoch_commit_queue_shard)[epoch_mod]->try_dequeue(txn_ptr)) {
                if (txn_ptr != nullptr) {
                    std::cerr << "merge dequeue sharded_txn Commit"<< std::endl;
                    Commit();
                    txn_ptr.reset();
                    sleep_flag = false;
                }
            }
        }

        /// dequeue from epoch_result_return_queue_vec, set the result,
        /// then enqueue to  epoch_result_returned_queue_vec[shard_id]
        if(EpochManager::IsRecordCommitted(epoch)) {
//            std::cerr << "merge try_dequeue epoch_result_return_queue_shard " << shard_id  << " epoch " << epoch << std::endl;
            while (!EpochManager::IsResultReturned(epoch) &&
                   (*epoch_result_return_queue_shard)[epoch_mod]->try_dequeue(txn_ptr)) {
                if (txn_ptr != nullptr) {
                    std::cerr << "merge dequeue txn ResultReturn"<< std::endl;
                    epoch_mod = txn_ptr->cen % max_length;
                    csn_temp = std::to_string(txn_ptr->csn);
                    csn_result = "";
                    result = true;
                    if((*epoch_abort_map_shard)[epoch_mod]->contain(csn_temp, csn_temp)) {
                        result = false;
                    }
                    txn_ptr->result = result;
                    epoch_result_returned_queue_shard->enqueue(txn_ptr);
                    epoch_result_returned_txn_num_shard->IncCount(epoch_mod, 1);
                    txn_ptr.reset();
                    sleep_flag = false;
                }
            }
        }

        if(sleep_flag)
            usleep(50);
//            std::this_thread::yield();
    }
}