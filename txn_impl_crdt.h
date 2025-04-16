#ifndef _NDB_TXN_IMPL_CRDT_H_
#define _NDB_TXN_IMPL_CRDT_H_

#include "txn.h"
#include "lockguard.h"
#include "CRDT/epoch_manager.h"
#include "CRDT/merge.h"
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
}

template<template<typename> class Protocol, typename Traits>
bool
transaction<Protocol, Traits>::crdt_commit(void* txn, int shard_id, void* crdt_txn) {

    return true;
}

template<template<typename> class Protocol, typename Traits>
bool
transaction<Protocol, Traits>::crdt_record_commit(void* txn, int shard_id, void* crdt_txn) {

    return true;
}

#endif /* _NDB_TXN_IMPL_CRDT_H_ */
