//
// Created by user on 25-3-24.
//

#ifndef SILO_CRDT_TRANSACTION_H
#define SILO_CRDT_TRANSACTION_H

#include <vector>
#include <cstdint>
#include <string>
#include <unordered_map>

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

struct CRDTRow {
    uint64_t csn;
    OpType op_type;
    std::string key, value;
    std::vector<CRDTColumn> columns;
    explicit CRDTRow(uint64_t c = 0, OpType o = OpType::Read, std::string k = "", std::string v = ""):
        csn(c), op_type(o), key(k), value(v){}
};



class CRDTTransaction {
public:
    explicit CRDTTransaction(uint64_t id = 0, uint64_t se  = 0, uint64_t ce  = 0, uint64_t cs  = 0, void * t = nullptr, bool r = false):
        tid(id), sen(se), cen(ce), csn(cs), txn(t), result(false) {}

    uint64_t tid{}, sen{}, cen{}, csn{};
    std::unordered_map<std::string, CRDTRow> read_set, write_set;

    Timer t;

    void * txn;
    bool result;
};


#endif //SILO_CRDT_TRANSACTION_H
