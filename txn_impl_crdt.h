#ifndef _NDB_TXN_IMPL_CRDT_H_
#define _NDB_TXN_IMPL_CRDT_H_

#include "txn.h"
#include "lockguard.h"
#include "CRDT/epoch_manager.h"

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

//template<template<typename> class Protocol, typename Traits>
//transaction<Protocol, Traits>::~transaction() {
//    // transaction shouldn't fall out of scope w/o resolution
//    // resolution means TXN_EMBRYO, TXN_COMMITED, and TXN_ABRT
//    util::timer t;
//    uint64_t duration;
//    INVARIANT(state != TXN_ACTIVE);
//    INVARIANT(rcu::s_instance.in_rcu_region());
//    const unsigned cur_depth = rcu_guard_->sync()->depth();
//    rcu_guard_.destroy();
////    if (cur_depth == 1) {
////        INVARIANT(!rcu::s_instance.in_rcu_region());
////        cast()->on_post_rcu_region_completion();
////    }
//    if (node) {
//        CoreIdArena::recollect(node);
//        ::free(txn_buf);
//    }
//    duration = t.lap();
////    DISKAdaptor::recollect_time.fetch_add(duration);
//
//#ifdef BTREE_LOCK_OWNERSHIP_CHECKING
//    concurrent_btree::AssertAllNodeLocksReleased();
//#endif
//}

template<template<typename> class Protocol, typename Traits>
void
transaction<Protocol, Traits>::crdt_shard(void *txn) {
//    if (DISKAdaptor::initial_info){
//        if (DISKAdaptor::water_level.load() == -1) {
//            DISKAdaptor::water_level =
//                    ((sen - 1) % CRDTContext::kCacheMaxLength + CRDTContext::kCacheMaxLength) % CRDTContext::kCacheMaxLength;
//        }
//        DISKAdaptor::vec_info[sen % CRDTContext::kCacheMaxLength].txn_num.fetch_add(1);
//    }
//
//    cen = sen;
//    util::timer t;
//    split_write_dbtuples.resize(DISKAdaptor::GetNumShard());
//    const uint64_t MAX_KEY = DISKAdaptor::Get_nkeys();
//    uint64_t range_per_queue = MAX_KEY / DISKAdaptor::GetNumShard();
//
//    if (!write_set.empty()) {
//        typename write_set_map::iterator it = write_set.begin();
//        typename write_set_map::iterator it_end = write_set.end();
//        for (size_t pos = 0; it != it_end; ++it, ++pos) {
//            write_dbtuples.emplace_back(it->get_tuple(), &(*it), it->is_insert(), pos);
//        }
//    }
//
//    if (!write_dbtuples.empty()) {
//        int queue_idx;
//        {
//            write_dbtuples.sort(); // in-place
//        }
//        typename dbtuple_write_info_vec::iterator it = write_dbtuples.begin();
//        typename dbtuple_write_info_vec::iterator it_end = write_dbtuples.end();
//        dbtuple_write_info *last_px = nullptr;
//        for (; it != it_end; last_px = &(*it), ++it) {
//            if (likely(last_px && last_px->tuple != it->tuple)) {
//                dbtuple *const tuple = it->get_tuple();
//                queue_idx = tuple->keyNum / range_per_queue;
//                queue_idx = std::min(queue_idx, DISKAdaptor::GetNumShard() - 1);
//                split_write_dbtuples[queue_idx].emplace_back(tuple);
//            }
//        }
//        dbtuple *const tuple = last_px->get_tuple();
//        queue_idx = tuple->keyNum / range_per_queue;
//        queue_idx = std::min(queue_idx, DISKAdaptor::GetNumShard() - 1);
//        split_write_dbtuples[queue_idx].emplace_back(tuple);
//
//        for (int i = 0; i < DISKAdaptor::GetNumShard(); i++) {
//            if (!split_write_dbtuples[i].empty()) {
//                count.fetch_add(1);
//            }
//        }
//        for (int i = 0; i < DISKAdaptor::GetNumShard(); i++) {
//            if (!split_write_dbtuples[i].empty()) {
//                DISKAdaptor::txn_shard_merge_queues[cen % CRDTContext::kCacheMaxLength][i].enqueue(txn);
//            }
//        }
//    } else {
//        DISKAdaptor::IncLocalMergeCounters(cen);
//        DISKAdaptor::txn_commit_queues[cen % CRDTContext::kCacheMaxLength].enqueue(txn);
//    }
//    DISKAdaptor::shard_done += t.lap();
    return;
}

template<template<typename> class Protocol, typename Traits>
void
transaction<Protocol, Traits>::crdt_merge(void *txn, int id) {
//    csn = (util::timer::cur_usec() << 16) |
//                           (DISKAdaptor::counter.fetch_add(1, std::memory_order_relaxed) & 0xFFFF);
//    util::timer t;
//    if (DISKAdaptor::clear_map) {
//        while (DISKAdaptor::clear_map) {
//            sched_yield();
//        }
//        DISKAdaptor::wait_clear_map.fetch_add(t.lap());
//    }
//    DISKAdaptor::critical_section_cnt.fetch_add(1);
//    for (typename dbtuple_write_info_vec::iterator it = split_write_dbtuples[id].begin();
//         it != split_write_dbtuples[id].end(); ++it) {
//        dbtuple *tuple = it->get_tuple();
//        std::pair<uint64_t, uint64_t> value;
//        DISKAdaptor::tuple_map.getValue(tuple, value);
//        if (cen > value.first) {
//            DISKAdaptor::tuple_map.insert(tuple, std::make_pair(cen, csn));
//        } else if (cen == value.first) {
//            if (value.second < csn) {
//                DISKAdaptor::abort_transcation_csn_set.insert(csn, csn);
//            } else if (value.second > csn) {
//                DISKAdaptor::abort_transcation_csn_set.insert(value.second, value.second);
//                DISKAdaptor::tuple_map.insert(tuple, std::make_pair(cen, csn));
//            }
//        } else DISKAdaptor::abort_transcation_csn_set.insert(value.second, value.second);
//    }
//    DISKAdaptor::critical_section_cnt.fetch_sub(1);
//    if (count.fetch_sub(1) == 1) {
//        DISKAdaptor::IncLocalMergeCounters(cen);
//        DISKAdaptor::txn_commit_queues[cen % CRDTContext::kCacheMaxLength].enqueue(txn);
//    }
}

template<template<typename> class Protocol, typename Traits>
bool
transaction<Protocol, Traits>::crdt_commit() {
    coreid::set_core_id_without_check(coreId);
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
    return false;
}



#endif /* _NDB_TXN_IMPL_CRDT_H_ */
