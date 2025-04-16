//
// Created by user on 25-3-24.
//

#ifndef SILO_CRDT_TRANSACTION_H
#define SILO_CRDT_TRANSACTION_H

#include <vector>
#include <cstdint>
#include <string>
#include <memory>
#include <numa.h>
#include <iostream>
#include <utility>

#include "crdt_utils.h"

enum OpType{
    Read,
    Update,
    Insert,
    Delete
};

struct CRDTColumn {
    uint64_t col_id;
    std::string value;
};

//template <typename Alloc = std::allocator<CRDTColumn>>
struct CRDTRow {
    uint64_t csn;
    OpType op_type;
    std::string key, value;
//    std::vector<CRDTColumn, Alloc> columns;

//    using allocator_type = Alloc;

    explicit CRDTRow(uint64_t c = 0, OpType o = OpType::Read, std::string k = "",
                     std::string v = ""):
        csn(c), op_type(o), key(std::move(k)), value(std::move(v)){}

//    explicit CRDTRow(uint64_t c = 0, OpType o = OpType::Read, std::string k = "", std::string v = "",
//                     const allocator_type& alloc = allocator_type())
//            : csn(c), op_type(o), key(std::move(k)), value(std::move(v)), columns(alloc) {}
};



class CRDTTransaction {
public:

    explicit CRDTTransaction(uint64_t id = 0, uint64_t se  = 0, uint64_t ce  = 0, uint64_t cs  = 0, uint64_t tti = 0,
                             void * t = nullptr, std::shared_ptr<CRDTTransaction> crdt_t = nullptr,
                             bool r = false, uint64_t rz = 10, uint64_t numa_id = 0):
        tid(id), sen(se), cen(ce), csn(cs),
        crdt_worker_id(tti), row_size(rz),

        txn(t),
        crdt_txn(std::move(crdt_t)),
        result(r) {

        read_set.resize(row_size);
        write_set.resize(row_size);
    }

    uint64_t tid{}, sen{}, cen{}, csn{},
        crdt_worker_id{}, crdt_txn_id{}, row_size{},
        exec_time{}, shrad_time{}, read_validate_time{}, merge_time{}, commit_time{};

//    RowVector read_set, write_set;
    std::vector<std::pair<std::string, struct CRDTRow>> read_set, write_set;

    Timer t{};

    void * txn; /// useless
    std::shared_ptr<CRDTTransaction> crdt_txn;
    bool result;
};

#endif //SILO_CRDT_TRANSACTION_H
