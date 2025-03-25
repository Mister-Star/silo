//
// Created by user on 25-3-24.
//

#ifndef SILO_CRDT_TRANSACTION_H
#define SILO_CRDT_TRANSACTION_H

#include <vector>
#include <cstdint>
#include <string>
#include <unordered_map>

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
};

class CRDTTransaction {
public:

    explicit CRDTTransaction(uint64_t id = 0, uint64_t se  = 0, uint64_t ce  = 0, uint64_t cs  = 0):
        tid(id),sen(se),cen(ce), csn(cs){}

    uint64_t tid, sen, cen, csn;
    std::unordered_map<std::string, CRDTRow> read_set, write_set;

};


#endif //SILO_CRDT_TRANSACTION_H
