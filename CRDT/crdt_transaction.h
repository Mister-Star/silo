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

template <typename T>
class NumaAllocator_Vector {
public:
    using value_type = T;

    explicit NumaAllocator_Vector(uint64_t node = 0) noexcept : node_(node) {}

    template <typename U>
    explicit NumaAllocator_Vector(const NumaAllocator_Vector<U>& other) noexcept : node_(other.node()) {}

    T* allocate(std::size_t n) {
        void* p = numa_alloc_onnode(n * sizeof(T), node_);
        if (!p) throw std::bad_alloc();
        return static_cast<T*>(p);
    }

    void deallocate(T* p, std::size_t n) noexcept {
        numa_free(p, n * sizeof(T));
    }

    int node() const noexcept { return node_; }

    template <typename U>
    bool operator==(const NumaAllocator_Vector<U>& other) const noexcept {
        return node_ == other.node();
    }

    template <typename U>
    bool operator!=(const NumaAllocator_Vector<U>& other) const noexcept {
        return *this != other;
    }

private:
    uint64_t node_;
};

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
    using Row = std::pair<std::string, CRDTRow>;
    using RowVector = std::vector<Row, NumaAllocator_Vector<Row>>;

    explicit CRDTTransaction(uint64_t id = 0, uint64_t se  = 0, uint64_t ce  = 0, uint64_t cs  = 0, uint64_t tti = 0,
                             void * t = nullptr, std::shared_ptr<CRDTTransaction> crdt_t = nullptr,
                             bool r = false, uint64_t rz = 10, uint64_t numa_id = 0):
        tid(id), sen(se), cen(ce), csn(cs),
        crdt_worker_id(tti), row_size(rz),

        read_set(NumaAllocator_Vector<Row>(numa_id)),
        write_set(NumaAllocator_Vector<Row>(numa_id)),

        txn(t),
        crdt_txn(std::move(crdt_t)),
        result(r) {

        read_set.resize(row_size);
        write_set.resize(row_size);
    }

    uint64_t tid{}, sen{}, cen{}, csn{},
        crdt_worker_id{}, crdt_txn_id{}, row_size{},
        exec_time{}, shrad_time{}, read_validate_time{}, merge_time{}, commit_time{};

    RowVector read_set, write_set;

    Timer t{};

    void * txn; /// useless
    std::shared_ptr<CRDTTransaction> crdt_txn;
    bool result;
};

class CRDTRingBuffer {
public:
    void Init(int row_size = 10, size_t shard_num = 1, size_t capacity = 24) {
        row_size_ = row_size;
        shard_num_ = shard_num;
        capacity_ = capacity;
        buffer_.reserve(capacity_);
        used_.reserve(capacity_);
        for (size_t i = 0; i < capacity_; ++i) {
            auto vec = std::make_shared<std::vector<std::shared_ptr<CRDTTransaction>>>();
            buffer_.push_back(vec);
            used_.push_back(false);

            for(size_t j = 0; j < shard_num_; j ++) {
                void* raw = numa_alloc_onnode(sizeof(CRDTTransaction), static_cast<int>(j));
                if (!raw) throw std::bad_alloc();

                try {
                    auto* obj = new (raw) CRDTTransaction(0, 0, 0, 0, 0, nullptr, nullptr, false, row_size_, j);
                    auto txn = std::shared_ptr<CRDTTransaction>(
                            obj,
                            [](CRDTTransaction* p) {
                                p->~CRDTTransaction();
                                numa_free(p, sizeof(CRDTTransaction));
                            }
                    );
                    vec->push_back(txn);
                } catch (...) {
                    numa_free(raw, sizeof(CRDTTransaction));
                    throw;
                }
            }
        }
    }

    size_t get_next_recycle() {
        st:
        for (size_t i = 0; i < used_.size(); ++i) {
            if (!used_[i]) {
                return i;
            }
        }
        expand(capacity_);
        for (size_t i = 0; i < used_.size(); ++i) {
            if (!used_[i]) {
                return i;
            }
        }
        goto st;
    }

    std::shared_ptr<std::vector<std::shared_ptr<CRDTTransaction>>> GetVec(size_t index) {
        return buffer_[index];
    }

    void release(size_t index) {
        used_[index % capacity_] = false;
    }

private:
    void allocate_buffer(size_t count) {
        for (size_t i = 0; i < count; ++i) {
            auto vec = std::make_shared<std::vector<std::shared_ptr<CRDTTransaction>>>();
            buffer_.push_back(vec);
            used_.push_back(false);
            for(size_t j = 0; j < shard_num_; j ++) {
                void* raw = numa_alloc_onnode(sizeof(CRDTTransaction), static_cast<int>(j));
                if (!raw) throw std::bad_alloc();

                try {
                    auto* obj = new (raw) CRDTTransaction(0, 0, 0, 0, 0, nullptr, nullptr, false, row_size_, j);
                    auto txn = std::shared_ptr<CRDTTransaction>(
                            obj,
                            [](CRDTTransaction* p) {
                                p->~CRDTTransaction();
                                numa_free(p, sizeof(CRDTTransaction));
                            }
                    );
                    vec->push_back(txn);
                } catch (...) {
                    numa_free(raw, sizeof(CRDTTransaction));
                    throw;
                }
            }
        }
    }

    void expand(size_t amount) {
        allocate_buffer(amount);
        capacity_ += amount;
    }

    int row_size_{};
    size_t shard_num_{}, capacity_{};
    std::vector<std::shared_ptr<std::vector<std::shared_ptr<CRDTTransaction>>>> buffer_;
    std::vector<bool> used_;
};


#endif //SILO_CRDT_TRANSACTION_H
