//
// Created by Star on 2025/3/24.
//

#include "crdt_counters.h"
#include "epoch_manager.h"


std::atomic<uint64_t> CRDTCounters::inc_id(0);
uint64_t CRDTCounters::max_length = 1, CRDTCounters::shard_num = 1;

std::vector<std::shared_ptr<AtomicCounters>> ///[shard][epoch]
        CRDTCounters::epoch_should_read_validate_txn_num_vec,
        CRDTCounters::epoch_read_validated_txn_num_vec,
        CRDTCounters::epoch_should_merge_txn_num_vec,
        CRDTCounters::epoch_merged_txn_num_vec,
        CRDTCounters::epoch_should_commit_txn_num_vec,
        CRDTCounters::epoch_committed_txn_num_vec,
        CRDTCounters::epoch_record_commit_txn_num_vec,
        CRDTCounters::epoch_record_committed_txn_num_vec,
        CRDTCounters::epoch_result_return_txn_num_vec,
        CRDTCounters::epoch_result_returned_txn_num_vec;

std::vector<std::shared_ptr<std::atomic<uint64_t>>>
        CRDTCounters::total_exec_txn_num_vec,
        CRDTCounters::total_exec_latency_vec,
        CRDTCounters::total_read_validate_txn_num_vec,
        CRDTCounters::total_read_validate_latency_vec,
        CRDTCounters::total_merge_txn_num_vec,
        CRDTCounters::total_merge_latency_vec,
        CRDTCounters::total_commit_txn_num_vec,
        CRDTCounters::total_commit_latency_vec,
        CRDTCounters::total_result_return_txn_num_vec,
        CRDTCounters::total_result_return_latency_vec,
        CRDTCounters::success_commit_txn_num_vec,
        CRDTCounters::success_commit_latency_vec,
        CRDTCounters::total_read_version_check_failed_txn_num_vec,
        CRDTCounters::total_failed_txn_num_vec;

AtomicCounters///[[epoch]
    CRDTCounters::epoch_should_exec_txn_num(500),
    CRDTCounters::epoch_exec_txn_num(500);

std::vector<std::shared_ptr<std::atomic<bool>>> ///[epoch]
        CRDTCounters::epoch_read_validate_complete,
        CRDTCounters::epoch_merge_complete,
        CRDTCounters::epoch_commit_complete,
        CRDTCounters::epoch_record_committed,
        CRDTCounters::epoch_result_returned;

std::vector<std::shared_ptr<std::atomic<uint64_t>>> CRDTCounters::shard_init_flag;

void CRDTCounters::StaticInit() {

    max_length = CRDTContext::kCacheMaxLength;
    shard_num = CRDTContext::kShardNum;

    std::cerr << "CRDTCounters::StaticInit" << " max_length " << max_length <<
    " shard_num " << shard_num << std::endl;

    epoch_should_read_validate_txn_num_vec.resize(shard_num); ///[shard][epoch]
    epoch_read_validated_txn_num_vec.resize(shard_num);
    epoch_should_merge_txn_num_vec.resize(shard_num);
    epoch_merged_txn_num_vec.resize(shard_num);
    epoch_should_commit_txn_num_vec.resize(shard_num);
    epoch_committed_txn_num_vec.resize(shard_num);
    epoch_record_commit_txn_num_vec.resize(shard_num);
    epoch_record_committed_txn_num_vec.resize(shard_num);
    epoch_result_return_txn_num_vec.resize(shard_num);
    epoch_result_returned_txn_num_vec.resize(shard_num);

    total_exec_txn_num_vec.resize(shard_num); ///[shard][epoch]
    total_exec_latency_vec.resize(shard_num);
    total_read_validate_txn_num_vec.resize(shard_num);
    total_read_validate_latency_vec.resize(shard_num);
    total_merge_txn_num_vec.resize(shard_num);
    total_merge_latency_vec.resize(shard_num);
    total_commit_txn_num_vec.resize(shard_num);
    total_commit_latency_vec.resize(shard_num);
    total_result_return_txn_num_vec.resize(shard_num);
    total_result_return_latency_vec.resize(shard_num);
    success_commit_txn_num_vec.resize(shard_num);
    success_commit_latency_vec.resize(shard_num);
    total_read_version_check_failed_txn_num_vec.resize(shard_num);
    total_failed_txn_num_vec.resize(shard_num);

    ///epoch merge state
    epoch_should_exec_txn_num.Resize(max_length); ///[epoch]
    epoch_exec_txn_num.Resize(max_length); ///[epoch]

    epoch_read_validate_complete.resize(max_length); ///[epoch]
    epoch_merge_complete.resize(max_length);
    epoch_commit_complete.resize(max_length);
    epoch_record_committed.resize(max_length);
    epoch_result_returned.resize(max_length);
    for(int i = 0; i < static_cast<int>(max_length); i ++) {
        epoch_read_validate_complete[i] = std::make_unique<std::atomic<bool>>(false);
        epoch_merge_complete[i] = std::make_unique<std::atomic<bool>>(false);
        epoch_commit_complete[i] = std::make_unique<std::atomic<bool>>(false);
        epoch_record_committed[i] = std::make_unique<std::atomic<bool>>(false);
        epoch_result_returned[i] = std::make_unique<std::atomic<bool>>(false);
    }

    shard_init_flag.resize(shard_num);
    for(int i = 0; i < static_cast<int>(shard_num); i ++) {
        shard_init_flag[i] = std::make_shared<std::atomic<uint64_t>>(0);
    }
    return;
}

void CRDTCounters::StaticInitShard(uint64_t& shard) {
    if(shard_init_flag[shard]->fetch_add(1) != 0)
        return;

    std::cerr << "CRDTCounters::StaticInitShard  shard_id " << shard << std::endl;
    ///[shard]
    epoch_should_read_validate_txn_num_vec[shard] = std::make_shared<AtomicCounters>(max_length);
    epoch_read_validated_txn_num_vec[shard] = std::make_shared<AtomicCounters>(max_length);
    epoch_should_merge_txn_num_vec[shard] = std::make_shared<AtomicCounters>(max_length);
    epoch_merged_txn_num_vec[shard] = std::make_shared<AtomicCounters>(max_length);
    epoch_should_commit_txn_num_vec[shard] = std::make_shared<AtomicCounters>(max_length);
    epoch_committed_txn_num_vec[shard] = std::make_shared<AtomicCounters>(max_length);
    epoch_record_commit_txn_num_vec[shard] = std::make_shared<AtomicCounters>(max_length);
    epoch_record_committed_txn_num_vec[shard] = std::make_shared<AtomicCounters>(max_length);
    epoch_result_return_txn_num_vec[shard] = std::make_shared<AtomicCounters>(max_length);
    epoch_result_returned_txn_num_vec[shard] = std::make_shared<AtomicCounters>(max_length);


    total_exec_txn_num_vec[shard] = std::make_shared<std::atomic<uint64_t>>(0); ///[shard][epoch]
    total_exec_latency_vec[shard] = std::make_shared<std::atomic<uint64_t>>(0);
    total_read_validate_txn_num_vec[shard] = std::make_shared<std::atomic<uint64_t>>(0);
    total_read_validate_latency_vec[shard] = std::make_shared<std::atomic<uint64_t>>(0);
    total_merge_txn_num_vec[shard] = std::make_shared<std::atomic<uint64_t>>(0);
    total_merge_latency_vec[shard] = std::make_shared<std::atomic<uint64_t>>(0);
    total_commit_txn_num_vec[shard] = std::make_shared<std::atomic<uint64_t>>(0);
    total_commit_latency_vec[shard] = std::make_shared<std::atomic<uint64_t>>(0);
    total_result_return_txn_num_vec[shard] = std::make_shared<std::atomic<uint64_t>>(0);
    total_result_return_latency_vec[shard] = std::make_shared<std::atomic<uint64_t>>(0);
    success_commit_txn_num_vec[shard] = std::make_shared<std::atomic<uint64_t>>(0);
    success_commit_latency_vec[shard] = std::make_shared<std::atomic<uint64_t>>(0);
    total_read_version_check_failed_txn_num_vec[shard] = std::make_shared<std::atomic<uint64_t>>(0);
    total_failed_txn_num_vec[shard] = std::make_shared<std::atomic<uint64_t>>(0);

    std::cerr << "CRDTCounters::StaticInitShard  end shard_id " << shard << std::endl;
    return;
}

void CRDTCounters::StaticClear(uint64_t& epoch) {
    auto epoch_mod_temp = epoch % max_length;
    auto cache_clear_epoch_num_mod = epoch % max_length;


    ///Merge
    epoch_should_exec_txn_num.Clear(epoch); ///[epoch]
    epoch_exec_txn_num.Clear(epoch); ///[epoch]

    epoch_read_validate_complete[epoch_mod_temp]->store(false);
    epoch_merge_complete[epoch_mod_temp]->store(false);
    epoch_commit_complete[epoch_mod_temp]->store(false);
    epoch_record_committed[epoch_mod_temp]->store(false);
    epoch_result_returned[epoch_mod_temp]->store(false);


    ClearAllThreadLocalCountNum(epoch, epoch_should_read_validate_txn_num_vec);
    ClearAllThreadLocalCountNum(epoch, epoch_read_validated_txn_num_vec);
    ClearAllThreadLocalCountNum(epoch, epoch_should_merge_txn_num_vec);
    ClearAllThreadLocalCountNum(epoch, epoch_merged_txn_num_vec);
    ClearAllThreadLocalCountNum(epoch, epoch_should_commit_txn_num_vec);
    ClearAllThreadLocalCountNum(epoch, epoch_committed_txn_num_vec);
    ClearAllThreadLocalCountNum(epoch, epoch_record_commit_txn_num_vec);
    ClearAllThreadLocalCountNum(epoch, epoch_record_committed_txn_num_vec);
    ClearAllThreadLocalCountNum(epoch, epoch_result_return_txn_num_vec);
    ClearAllThreadLocalCountNum(epoch, epoch_result_returned_txn_num_vec);

    return;
}


bool CRDTCounters::CheckEpochReadValidateComplete(const uint64_t& epoch) {
    if(epoch_read_validate_complete[epoch % max_length]->load(std::memory_order_acquire)) {
        return true;
    }
    if (epoch < EpochManager::GetPhysicalEpoch() && IsReadValidateComplete(epoch)) {
        epoch_read_validate_complete[epoch % max_length]->store(true, std::memory_order_release);
        return true;
    }
    return false;
}
bool CRDTCounters::CheckEpochMergeComplete(const uint64_t& epoch) {
    if(epoch_merge_complete[epoch % max_length]->load(std::memory_order_acquire)) {
        return true;
    }
    if (epoch < EpochManager::GetPhysicalEpoch() && IsMergeComplete(epoch)) {
        epoch_merge_complete[epoch % max_length]->store(true, std::memory_order_release);
        return true;
    }
    return false;
}
bool CRDTCounters::CheckEpochCommitComplete(const uint64_t& epoch) {
    if (epoch_commit_complete[epoch % max_length]->load(std::memory_order_acquire)) return true;
    if (epoch < EpochManager::GetPhysicalEpoch() && IsCommitComplete(epoch)) {
        epoch_commit_complete[epoch % max_length]->store(true, std::memory_order_release);
        return true;
    }
    return false;
}
bool CRDTCounters::CheckEpochRecordCommitted(const uint64_t& epoch) {
    if (epoch_record_committed[epoch % max_length]->load(std::memory_order_acquire)) return true;
    if (epoch < EpochManager::GetPhysicalEpoch() && IsCommitComplete(epoch) && IsRecordCommitted(epoch)) {
        epoch_record_committed[epoch % max_length]->store(true, std::memory_order_release);
        return true;
    }
    return false;
}

bool CRDTCounters::CheckEpochResultReturned(const uint64_t& epoch) {
    if (epoch_result_returned[epoch % max_length]->load(std::memory_order_acquire)) return true;
    if (epoch < EpochManager::GetPhysicalEpoch() && IsRecordCommitted(epoch) && IsResultReturned(epoch)) {
        epoch_result_returned[epoch % max_length]->store(true, std::memory_order_release);
        return true;
    }
    return false;
}





bool CRDTCounters::IsReadValidateComplete(const uint64_t& epoch) {
    if(GetAllThreadLocalCountNum(epoch, epoch_should_read_validate_txn_num_vec) >
       GetAllThreadLocalCountNum(epoch, epoch_read_validated_txn_num_vec))
        return false;
    return true;
}
bool CRDTCounters::IsMergeComplete(const uint64_t& epoch) {
    if(GetAllThreadLocalCountNum(epoch,epoch_should_read_validate_txn_num_vec) >
       GetAllThreadLocalCountNum(epoch,epoch_read_validated_txn_num_vec))
        return false;
    if(GetAllThreadLocalCountNum(epoch, epoch_should_merge_txn_num_vec) >
       GetAllThreadLocalCountNum(epoch, epoch_merged_txn_num_vec))
        return false;
    return true;
}
bool CRDTCounters::IsCommitComplete(const uint64_t & epoch) {
    if(GetAllThreadLocalCountNum(epoch, epoch_should_commit_txn_num_vec) >
       GetAllThreadLocalCountNum(epoch, epoch_committed_txn_num_vec))
        return false;
    return true;
}
bool CRDTCounters::IsRecordCommitted(const uint64_t & epoch) {
    if(GetAllThreadLocalCountNum(epoch, epoch_record_commit_txn_num_vec) >
       GetAllThreadLocalCountNum(epoch, epoch_record_committed_txn_num_vec))
        return false;
    return true;
}

bool CRDTCounters::IsResultReturned(const uint64_t & epoch) {
    if(GetAllThreadLocalCountNum(epoch, epoch_result_return_txn_num_vec) >
       GetAllThreadLocalCountNum(epoch, epoch_result_returned_txn_num_vec))
        return false;
    return true;
}

void CRDTCounters::ClearAllThreadLocalCountNum(const uint64_t &epoch, const std::vector<std::shared_ptr<AtomicCounters>> &vec) {
    for(const auto& i : vec) {
        if(i != nullptr)
            i->Clear(epoch);
    }
}

uint64_t CRDTCounters::GetAllThreadLocalCountNum(const uint64_t &epoch, const std::vector<std::shared_ptr<AtomicCounters>> &vec) {
    uint64_t ans = 0;
    for(const auto& i : vec) {
        if(i != nullptr) {
            ans += i->GetCount(epoch);
        }
    }
    return ans;
}
uint64_t CRDTCounters::GetAllThreadLocalCountNum(const uint64_t &epoch, const uint64_t &shard_id, const std::vector<std::shared_ptr<AtomicCounters>> &vec) {
    return vec[shard_id]->GetCount(epoch);
}