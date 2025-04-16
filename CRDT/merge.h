//
// Created by Star on 2025/3/24.
//

#ifndef SILO_1_MERGE_H
#define SILO_1_MERGE_H

#pragma once
#include <queue>
#include "crdt_utils.h"
#include "crdt_transaction.h"
#include "crdt_context.h"
#include "crdt_counters.h"

class Merge {

public:
    /// [shard][epoch]
    static std::vector<std::shared_ptr<std::vector<std::shared_ptr<
            concurrent_crdt_unordered_map<std::string, std::string, std::string>>>>>
        epoch_merge_map_vec, ///epoch merge   row_header
        epoch_abort_map_vec; /// for epoch final check

    ///[epoch]
    static std::vector<std::shared_ptr<
            concurrent_crdt_unordered_map<std::string, std::string, std::string>>>
            epoch_global_abort_map_vec;

    /// [shard][epoch]
    static std::vector<std::shared_ptr<std::vector<std::shared_ptr<
        concurrent_unordered_map<std::string, std::shared_ptr<CRDTTransaction>>>>>>
        epoch_txn_map_vec;

    ///[shard]
    static std::vector<std::shared_ptr<concurrent_unordered_map<std::string, std::string>>>
        read_version_map_data_vec, ///read validate for higher isolation
        read_version_map_csn_vec, ///read validate for higher isolation
        insert_set_vec;   ///插入集合，用于判断插入是否可以执行成功 check key exits?

    ///[shard][epoch]
    static std::vector<std::shared_ptr<std::vector<std::shared_ptr<
        BlockingConcurrentQueue<std::shared_ptr<CRDTTransaction>>>>>>
        epoch_read_validate_queue_vec,
        epoch_merge_queue_vec,///存放要进行merge的事务，分片
        epoch_commit_queue_vec, ///存放每个epoch要进行写日志的事务，分片写日志
        epoch_redo_log_queue_vec,
        epoch_result_return_queue_vec; ///存放每个epoch要处理结果的full txn

    ///[shard]
    static std::vector<std::shared_ptr<
            BlockingConcurrentQueue<std::shared_ptr<CRDTTransaction>>>>
        epoch_result_returned_queue_vec; ///存放要处理结果的full txn

    static std::vector<std::shared_ptr<std::atomic<uint64_t>>> shard_init_flag;

    static void Init();
    static void ShardInit(const uint64_t &shard);

    static void EpochClear(uint64_t& epoch);

    static void ReadValidateQueueEnqueue(const uint64_t &shard_, const uint64_t &epoch_,
                                         const uint64_t &max_length_, const std::shared_ptr<CRDTTransaction> &txn_ptr_);
    static void MergeQueueEnqueue(const uint64_t &shard_, const uint64_t &epoch_,
                                  const uint64_t &max_length_, const std::shared_ptr<CRDTTransaction>& txn_ptr_);
    static void CommitQueueEnqueue(const uint64_t &shard_, const uint64_t &epoch_,
                                   const uint64_t &max_length_, const std::shared_ptr<CRDTTransaction>& txn_ptr_);
    static void ResultReturnQueueEnqueue(const uint64_t &shard_, const uint64_t &epoch_,
                                         const uint64_t &max_length_, const std::shared_ptr<CRDTTransaction>& txn_ptr_);


public:

    uint64_t shard_id, max_length, epoch, epoch_mod;
    std::string csn_temp, csn_result, csn_version;
    std::shared_ptr<CRDTTransaction> txn_ptr;
    bool result, sleep_flag;

    std::shared_ptr<std::vector<std::shared_ptr<CRDTTransaction>>> epoch_sharded_txn_ptr;

            ///each merge thread access local counter
    std::shared_ptr<AtomicCounters>///[epoch]
        epoch_should_read_validate_txn_num_shard,
        epoch_read_validated_txn_num_shard,
        epoch_should_merge_txn_num_shard,
        epoch_merged_txn_num_shard,
        epoch_should_commit_txn_num_shard,
        epoch_committed_txn_num_shard,
        epoch_record_commit_txn_num_shard,
        epoch_record_committed_txn_num_shard,
        epoch_result_return_txn_num_shard,
        epoch_result_returned_txn_num_shard;

    std::shared_ptr<std::atomic<uint64_t>>///[epoch]
        total_exec_txn_num_shard,
        total_exec_latency_shard,
        total_read_validate_txn_num_shard,
        total_read_validate_latency_shard,
        total_merge_txn_num_shard,
        total_merge_latency_shard,
        total_commit_txn_num_shard,
        total_commit_latency_shard,
        total_result_return_txn_num_shard,
        total_result_return_latency_shard,
        success_commit_txn_num_shard,
        success_commit_latency_shard,
        total_read_version_check_failed_txn_num_shard,
        total_failed_txn_num_shard;

    /// [epoch]
    std::shared_ptr<std::vector<std::shared_ptr<
        concurrent_crdt_unordered_map<std::string, std::string, std::string>>>>
    epoch_merge_map_shard, ///epoch merge   row_header
    epoch_abort_map_shard; /// for epoch final check

    ///[epoch]
    std::shared_ptr<std::vector<std::shared_ptr<
        concurrent_unordered_map<std::string, std::shared_ptr<CRDTTransaction>>>>>
    epoch_txn_map_shard;

    std::shared_ptr<concurrent_unordered_map<std::string, std::string>>
            read_version_map_data_shard, ///read validate for higher isolation
        read_version_map_csn_shard, ///read validate for higher isolation
        insert_set_shard;   ///插入集合，用于判断插入是否可以执行成功 check key exits?

    std::shared_ptr<std::vector<std::shared_ptr<
        BlockingConcurrentQueue<std::shared_ptr<CRDTTransaction>>>>>
        epoch_read_validate_queue_shard,
        epoch_merge_queue_shard,///存放要进行merge的事务，分片
        epoch_commit_queue_shard,
        epoch_redo_log_queue_shard,
        epoch_result_return_queue_shard;///存放每个epoch要进行写日志的事务，分片写日志

    std::shared_ptr<
        BlockingConcurrentQueue<std::shared_ptr<CRDTTransaction>>>
            epoch_result_returned_queue_shard;

    void MergeThreadLocalInit(const uint64_t &shard);

    void EpochMerge();

    bool ValidateReadSet();
    bool CRDTMerge();
    bool Commit();

};


#endif //SILO_1_MERGE_H
