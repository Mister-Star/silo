#ifndef _NDB_TXN_IMPL_CRDT_H_
#define _NDB_TXN_IMPL_CRDT_H_

#include "txn.h"
#include "lockguard.h"
#include "CRDT/epoch_manager.h"
#include "util.h"

// addby

template<template<typename> class Protocol, typename Traits>
transaction<Protocol, Traits>::transaction(uint64_t flags, string_allocator_type &sa, void *buf,
                                           CoreIdArena::Node *node, int workerId)
        : transaction_base(flags), sa(&sa), txn_buf(buf), node(node), workerId(workerId) {
    coreId = coreid::core_id();
    INVARIANT(rcu::s_instance.in_rcu_region());
#ifdef BTREE_LOCK_OWNERSHIP_CHECKING
    concurrent_btree::NodeLockRegionBegin();
#endif

    while (!EpochManager::IsShardInitOK()) usleep(200);
    sen = EpochManager::GetPhysicalEpoch();
//    DISKAdaptor::AddNum(sen, 1);
//    DISKAdaptor::nnew_txn.fetch_add(1);
}

template<template<typename> class Protocol, typename Traits>
bool
transaction<Protocol, Traits>::crdt_commit(void* txn, int shard_id, void* crdt_txn) {
    /// called by bench_worker
//    std::cerr << "crdt_commit commit a txn to merge"<< std::endl;
    coreid::set_core_id_without_check(coreId);
    state = TXN_COMMITED;

    auto* temp_crdt_txn_ptr = static_cast<std::shared_ptr<CRDTTransaction>*>(crdt_txn);
    auto crdt_txn_ptr = *temp_crdt_txn_ptr;
    crdt_txn_ptr->crdt_txn = crdt_txn_ptr;
    auto epoch_txn_id = 0;

    ///sharding
    std::vector<std::shared_ptr<CRDTTransaction>> sharded_txn;
    sharded_txn.reserve(CRDTContext::kShardNum);

    {
        cen = EpochManager::GetPhysicalEpoch();
        epoch_txn_id = CRDTCounters::IncEpochShouldExecTxnNum(cen, 1);
        crdt_txn_ptr->cen = cen;
        crdt_txn_ptr->csn = now_to_us();
    }

    for(uint64_t i = 0; i < CRDTContext::kShardNum; i ++) {
        sharded_txn[i] = (*Merge::epoch_sharded_txn_vec[i])[epoch_txn_id];
        sharded_txn[i]->sen = crdt_txn_ptr->sen;
        sharded_txn[i]->tid = crdt_txn_ptr->tid;
        sharded_txn[i]->txn = crdt_txn_ptr->txn;
        sharded_txn[i]->cen = crdt_txn_ptr->cen;
        sharded_txn[i]->csn = crdt_txn_ptr->csn;
        sharded_txn[i]->read_set.clear();
        sharded_txn[i]->write_set.clear();
    }

    const uint64_t MAX_KEY = CRDTContext::kNKeys;
    uint64_t range_per_queue = MAX_KEY / CRDTContext::kShardNum;

    for(auto &it : read_set) {
        ///sharded_read_txn
        auto tuple = it.get_tuple();
        auto shard = (tuple->keyNum / range_per_queue) % CRDTContext::kShardNum;
        sharded_txn[shard]->read_set.emplace_back(
                std::make_pair(tuple->index_key,
                               std::move(CRDTRow(tuple->stable_csn, OpType::Read))));
    }

    for(auto it : write_set) {
        ///sharded_write_txn
        auto tuple = it.get_tuple();
        auto shard = (tuple->keyNum / range_per_queue) % CRDTContext::kShardNum;
        //todo: op_type
        // if(tuple->op_type == update)
        sharded_txn[shard]->read_set.emplace_back(
                std::make_pair(tuple->index_key,
                               std::move(CRDTRow(tuple->stable_csn, OpType::Update,
                                                 tuple->index_key,
                                                 std::string(reinterpret_cast<const char*>(tuple->value_start)
                                                             , tuple->size)))));
    }

    {
        //Enqueue
    //    std::cerr << "crdt_commit enqueue sharded_txn cen" << cen << std::endl;
        for(uint64_t i = 0; i < CRDTContext::kShardNum; i ++) {
            Merge::ReadValidateQueueEnqueue(i, cen, CRDTContext::kCacheMaxLength, sharded_txn[i]);
            Merge::MergeQueueEnqueue(i, cen, CRDTContext::kCacheMaxLength, sharded_txn[i]);
            Merge::CommitQueueEnqueue(i, cen, CRDTContext::kCacheMaxLength, sharded_txn[i]);
        }
        Merge::ResultReturnQueueEnqueue(shard_id, cen, CRDTContext::kCacheMaxLength, crdt_txn_ptr);
    }

    CRDTCounters::IncEpochExecTxnNum(cen, 1);
    crdt_txn_ptr->shrad_time = crdt_txn_ptr->t.lap();
//    delete temp_crdt_txn_ptr; /// no personal malloc initialized; all use std::shared_ptr

    return true;
}

template<template<typename> class Protocol, typename Traits>
bool
transaction<Protocol, Traits>::crdt_record_commit(void* txn, int shard_id, void* crdt_txn) {
    ///writing record
    /// called by merge thread or bench_worker.
    ///todo: update the record

    //    util::timer t;
//    uint64_t duration;
//    std::pair<bool, tid_t> commit_tid{false, 0};
//    duration = wait_commit_t.lap();
//    DISKAdaptor::wait_commit += duration;
////    std::cout << "从事务创建到准备commit " << duration << " 微秒" << "(epoch" << cen
////              << " )" << std::endl;
//
//    if (!crdt_read_set.empty()) {
//        typename crdt_read_set_map::iterator it = crdt_read_set.begin();
//        typename crdt_read_set_map::iterator it_end = crdt_read_set.end();
//        for (; it != it_end; ++it) {
//            if (it->get_tuple()->stable_csn == it->get_csn()) {
//                continue;
//            }
////            std::cout << "事务不是读最新tuple，abort" << std::endl;
//            goto do_crdt_abort;
//        }
//    }
//    duration = t.lap();
////    std::cout << "等待读取验证需要等待" << duration << " 微秒" << "(epoch" << cen
////              << " )" << std::endl;
//
//    if (DISKAdaptor::abort_transcation_csn_set.contain(csn)) {
////        std::cout << "事务merge时冲突，abort" << std::endl;
//        goto do_crdt_abort;
//    }
//
//    if (!write_dbtuples.empty()) {
//        typename dbtuple_write_info_vec::iterator it = write_dbtuples.begin();
//        typename dbtuple_write_info_vec::iterator it_end = write_dbtuples.end();
//        dbtuple_write_info *last_px = nullptr;
//        bool inserted_last_run = false;
//        for (; it != it_end; last_px = &(*it), ++it) {
//            if (likely(last_px && last_px->tuple != it->tuple)) {
//                // on boundary
//                if (unlikely(!handle_last_tuple_in_group(*last_px, inserted_last_run))) {
//                    abort_trap((reason = ABORT_REASON_WRITE_NODE_INTERFERENCE));
//                    goto do_crdt_abort;
//                }
//                inserted_last_run = false;
//            }
//            if (it->is_insert()) {
//                it->entry->set_do_write(); // all inserts are marked do-write
//                inserted_last_run = true;
//            }
//        }
//        if (likely(last_px) &&
//            unlikely(!handle_last_tuple_in_group(*last_px, inserted_last_run))) {
//            abort_trap((reason = ABORT_REASON_WRITE_NODE_INTERFERENCE));
//            goto do_crdt_abort;
//        }
//        duration = t.lap();
//        DISKAdaptor::lock_done += duration;
////        std::cout << "完成lock需要 " << duration << "微秒" << "(epoch" << cen << " )" << std::endl;
//
//        commit_tid.second = cast()->gen_commit_tid(write_dbtuples);
//        duration = t.lap();
//        DISKAdaptor::gen_tid += duration;
////        std::cout << "生成commit_tid需要 " << duration << "微秒" << "(epoch" << cen << " )" << std::endl;
//    }
//
//    if (!write_set.empty()) {
//        typename write_set_map::iterator it = write_set.begin();
//        typename write_set_map::iterator it_end = write_set.end();
//        for (; it != it_end; ++it) {
//            if (unlikely(!it->do_write()))
//                continue;
//            dbtuple *const tuple = it->get_tuple();
//            tuple->sen = sen;
//            tuple->cen = cen;
//            tuple->csn = csn;
//            if (it->is_insert()) {
//                tuple->version = commit_tid.second;
//                tuple->stable_csn = csn;
//            } else {
//                const dbtuple::write_record_ret ret =
//                        tuple->write_record_at(
//                                cast(), commit_tid.second,
//                                it->get_value(), it->get_writer());
//                bool unlock_head = false;
//                if (unlikely(ret.head_ != tuple)) {
//                    // tuple was replaced by ret.head_
//                    INVARIANT(ret.rest_ == tuple);
//                    // XXX: write_record_at() should acquire this lock
//                    ret.head_->lock(true);
//                    ret.head_->keyNum = tuple->keyNum;
//                    ret.head_->sen = tuple->sen;
//                    ret.head_->cen = tuple->cen;
//                    ret.head_->csn = tuple->csn;
//                    ret.head_->stable_csn = tuple->csn;
//                    unlock_head = true;
//                    // need to unlink tuple from underlying btree, replacing
//                    // with ret.rest_ (atomically)
//                    typename concurrent_btree::value_type old_v = 0;
//                    if (it->get_btree()->insert(
//                            varkey(it->get_key()), (typename concurrent_btree::value_type) ret.head_, &old_v, NULL))
//                        // should already exist in tree
//                        INVARIANT(false);
//                    INVARIANT(old_v == (typename concurrent_btree::value_type) tuple);
//                    // we don't RCU free this, because it is now part of the chain
//                    // (the cleaners will take care of this)
//                    ++evt_dbtuple_latest_replacement;
//                }
//                if (unlikely(ret.rest_)) {
//                    // spill happened: schedule GC task
//                    cast()->on_dbtuple_spill(ret.head_, ret.rest_);
//                }
//                if (!it->get_value())
//                    // logical delete happened: schedule GC task
//                    cast()->on_logical_delete(ret.head_, it->get_key(), it->get_btree());
//                if (unlikely(unlock_head))
//                    ret.head_->unlock();
//            }
//        }
//
//        for (typename dbtuple_write_info_vec::iterator it = write_dbtuples.begin();
//             it != write_dbtuples.end(); ++it) {
//            if (it->is_locked())
//                it->tuple->unlock();
//            else
//                INVARIANT(!it->is_insert());
//        }
//        duration = t.lap();
//        DISKAdaptor::write_back_done += duration;
////        std::cout << "写回需要 " << duration << "微秒" << "(epoch" << cen << " )" << std::endl;
//    }
//
//    duration = commit_done_t.lap();
////    std::cout << "事务创建直到commit（epoch" << cen << " )完成需要 " << duration << " 微秒" << std::endl;
//    state = TXN_COMMITED;
//    clear();
//    DISKAdaptor::IncLocalApplyCounters(cen);
//    duration = total_t.lap();
////        std::cout << "从事务创建到被回收内存需要 " << duration << " 微秒" << "(epoch" << cen
////                  << " )" << std::endl;
//    DISKAdaptor::latencys += duration;
//    return true;
//
//    do_crdt_abort:
//    for (typename dbtuple_write_info_vec::iterator it = write_dbtuples.begin();
//         it != write_dbtuples.end(); ++it) {
//        if (it->is_locked()) {
//            if (it->is_insert()) {
//                this->cleanup_inserted_tuple_marker(
//                        it->tuple.get(), it->entry->get_key(), it->entry->get_btree());
//            }
//            it->get_tuple()->unlock();
//        }
//    }
//
//    duration = commit_done_t.lap();
////    std::cout << "事务创建直到abort（epoch" << cen << " )完成需要 " << duration << " 微秒" << std::endl;
//    state = TXN_ABRT;
//    clear();
//    DISKAdaptor::IncLocalApplyCounters(cen);
    return true;
}

#endif /* _NDB_TXN_IMPL_CRDT_H_ */
