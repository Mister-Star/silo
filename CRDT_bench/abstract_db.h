#ifndef _ABSTRACT_DB_H_
#define _ABSTRACT_DB_H_

#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>

#include <map>
#include <string>

#include "abstract_ordered_index.h"
#include "../str_arena.h"

/**
 * Abstract interface for a DB. This is to facilitate writing
 * benchmarks for different systems, making each system present
 * a unified interface
 */
class abstract_db {
public:

  /**
   * both get() and put() can throw abstract_abort_exception. If thrown,
   * abort_txn() must be called (calling commit_txn() will result in undefined
   * behavior).  Also if thrown, subsequently calling get()/put() will also
   * result in undefined behavior)
   */
  class abstract_abort_exception {};

  // ctor should open db
  abstract_db() {}

  // dtor should close db
  virtual ~abstract_db() {}

  /**
   * an approximate max batch size for updates in a transaction.
   *
   * A return value of -1 indicates no maximum
   */
  virtual ssize_t txn_max_batch_size() const { return -1; }

  virtual bool index_has_stable_put_memory() const { return false; }

  // XXX(stephentu): laziness
  virtual size_t
  sizeof_txn_object(uint64_t txn_flags) const { NDB_UNIMPLEMENTED("sizeof_txn_object"); };

  /**
   * XXX(stephentu): hack
   */
  virtual void do_txn_epoch_sync() const {}

  /**
   * XXX(stephentu): hack
   */
  virtual void do_txn_finish() const {}

  /** loader should be used as a performance hint, not for correctness */
  virtual void thread_init(bool loader) {}

  virtual void thread_end() {}

  // [ntxns_persisted, ntxns_committed, avg latency]
  virtual std::tuple<uint64_t, uint64_t, double>
    get_ntxn_persisted() const { return std::make_tuple(0, 0, 0.0); }

  virtual void reset_ntxn_persisted() { }

  enum TxnProfileHint {
    HINT_DEFAULT,

    // ycsb profiles
    HINT_KV_GET_PUT, // KV workloads over a single key
    HINT_KV_TXN, //addby KV workload do multi_ple rows
    HINT_KV_RMW, // get/put over a single key
    HINT_KV_SCAN, // KV scan workloads (~100 keys)

    // tpcc profiles
    HINT_TPCC_NEW_ORDER,
    HINT_TPCC_PAYMENT,
    HINT_TPCC_DELIVERY,
    HINT_TPCC_ORDER_STATUS,
    HINT_TPCC_ORDER_STATUS_READ_ONLY,
    HINT_TPCC_STOCK_LEVEL,
    HINT_TPCC_STOCK_LEVEL_READ_ONLY,
  };

  /**
   * Initializes a new txn object the space pointed to by buf
   *
   * Flags is only for the ndb protocol for now
   *
   * [buf, buf + sizeof_txn_object(txn_flags)) is a valid ptr
   */
  virtual void *new_txn(
      uint64_t txn_flags,
      str_arena &arena,
      void *buf,
      TxnProfileHint hint = HINT_DEFAULT) = 0;

  typedef std::map<std::string, uint64_t> counter_map;
  typedef std::map<std::string, counter_map> txn_counter_map;

  /**
   * Reports things like read/write set sizes
   */
  virtual counter_map
  get_txn_counters(void *txn) const
  {
    return counter_map();
  }

  /**
   * Returns true on successful commit.
   *
   * On failure, can either throw abstract_abort_exception, or
   * return false- caller should be prepared to deal with both cases
   */
  virtual bool commit_txn(void *txn) = 0;

  ///addby
    virtual int get_workerId(void *txn , int id) = 0;
    virtual int get_coreId(void *txn , int id) = 0;
    virtual bool crdt_commit(void *txn, int id, void *crdt_txn) = 0;
    virtual bool crdt_record_commit(void *txn, int id, void *crdt_txn) = 0;
    virtual void destroy_txn(void *txn, int id) = 0;
    virtual void register_g_threadctxs() = 0;

  /**
   * XXX
   */
  virtual void abort_txn(void *txn) = 0;

  virtual void print_txn_debug(void *txn) const {}

  virtual abstract_ordered_index *
  open_index(const std::string &name,
             size_t value_size_hint,
             bool mostly_append = false) = 0;

  virtual void
  close_index(abstract_ordered_index *idx) = 0;
};

#endif /* _ABSTRACT_DB_H_ */
