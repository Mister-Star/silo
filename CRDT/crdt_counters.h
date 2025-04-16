//
// Created by Star on 2025/3/24.
//

#ifndef SILO_1_CRDTCOUNTERS_H
#define SILO_1_CRDTCOUNTERS_H

#pragma once

#include "atomic_counters.h"
#include "atomic_counters_cache.h"

class CRDTCounters {
public:
    uint64_t thread_id = 0, shard_id = 0;

    static uint64_t max_length, shard_num;
    static std::atomic<uint64_t> inc_id;

    static void StaticInit();
    static void StaticInitShard(uint64_t& shard);
    static void StaticClear(uint64_t& epoch);

public:
    ///epoch manager access all shard counters
    static std::vector<std::shared_ptr<AtomicCounters>>///[shard][epoch]
            epoch_should_read_validate_txn_num_vec,
            epoch_read_validated_txn_num_vec,
            epoch_should_merge_txn_num_vec,
            epoch_merged_txn_num_vec,
            epoch_should_commit_txn_num_vec,
            epoch_committed_txn_num_vec,
            epoch_record_commit_txn_num_vec,
            epoch_record_committed_txn_num_vec,
            epoch_result_return_txn_num_vec,
            epoch_result_returned_txn_num_vec;

    static std::vector<std::shared_ptr<std::atomic<uint64_t>>> ///shard
            total_exec_txn_num_vec,
            total_exec_latency_vec,
            total_read_validate_txn_num_vec,
            total_read_validate_latency_vec,
            total_merge_txn_num_vec,
            total_merge_latency_vec,
            total_commit_txn_num_vec,
            total_commit_latency_vec,
            total_result_return_txn_num_vec,
            total_result_return_latency_vec,
            success_commit_txn_num_vec,
            success_commit_latency_vec,
            total_read_version_check_failed_txn_num_vec,
            total_failed_txn_num_vec;

    static AtomicCounters///[[epoch]
            epoch_should_exec_txn_num,
            epoch_exec_txn_num;
//            epoch_should_merge_txn_num,
//            epoch_merged_txn_num,
//            epoch_should_commit_txn_num,
//            epoch_committed_txn_num,
//            epoch_record_commit_txn_num,
//            epoch_record_committed_txn_num,
//            epoch_result_return_txn_num,
//            epoch_result_returned_txn_num;

    static std::vector<std::shared_ptr<std::atomic<bool>>> ///[epoch]
        epoch_read_validate_complete,
        epoch_merge_complete,
        epoch_commit_complete,
        epoch_record_committed,
        epoch_result_returned;

    static std::vector<std::shared_ptr<std::atomic<uint64_t>>> shard_init_flag;


    static bool CheckEpochReadValidateComplete(const uint64_t& epoch);
    static bool CheckEpochMergeComplete(const uint64_t& epoch) ;
    static bool CheckEpochCommitComplete(const uint64_t& epoch) ;
    static bool CheckEpochRecordCommitted(const uint64_t& epoch) ;
    static bool CheckEpochResultReturned(const uint64_t& epoch) ;

    static uint64_t IncEpochShouldExecTxnNum(const uint64_t& epoch, uint64_t value) {
        return epoch_should_exec_txn_num.IncCount(epoch, value);
    }
    static uint64_t DecEpochShouldExecTxnNum(const uint64_t& epoch, uint64_t value) {
        return epoch_should_exec_txn_num.DecCount(epoch, value);
    }
    static uint64_t GetEpochShouldExecTxnNum(const uint64_t& epoch) {
        return epoch_should_exec_txn_num.GetCount(epoch);
    }

    static uint64_t IncEpochExecTxnNum(const uint64_t& epoch, uint64_t value) {
        return epoch_exec_txn_num.IncCount(epoch, value);
    }
    static uint64_t DecEpochExecTxnNum(const uint64_t& epoch, uint64_t value) {
        return epoch_exec_txn_num.DecCount(epoch, value);
    }
    static uint64_t GetEpochExecTxnNum(const uint64_t& epoch) {
        return epoch_exec_txn_num.GetCount(epoch);
    }

    static bool CheckEpochExecComplete(const uint64_t& epoch) {
        return epoch_exec_txn_num.GetCount(epoch) >= epoch_should_exec_txn_num.GetCount(epoch);
    }

    static bool IsReadValidateComplete(const uint64_t& epoch) ;
    static bool IsMergeComplete(const uint64_t& epoch) ;
    static bool IsCommitComplete(const uint64_t & epoch) ;
    static bool IsRecordCommitted(const uint64_t & epoch) ;
    static bool IsResultReturned(const uint64_t & epoch) ;

    static void ClearAllThreadLocalCountNum(const uint64_t &epoch, const std::vector<std::shared_ptr<AtomicCounters>> &vec) ;
    static uint64_t GetAllThreadLocalCountNum(const uint64_t &epoch, const std::vector<std::shared_ptr<AtomicCounters>> &vec) ;
    static uint64_t GetAllThreadLocalCountNum(const uint64_t &epoch, const uint64_t &shard_id, const std::vector<std::shared_ptr<AtomicCounters>> &vec);

};


#endif //SILO_1_CRDTCOUNTERS_H
