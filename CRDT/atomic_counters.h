//
// Created by 周慰星 on 11/8/22.
//

#ifndef TAAS_ATOMIC_COUNTERS_H
#define TAAS_ATOMIC_COUNTERS_H

#pragma once

#include <vector>
#include <atomic>
#include <memory>

/**
 * @brief 原子计数器，对std::atomic<uint64_t>的简单封装
 *
 */
class AtomicCounters {
private:
    std::vector<std::unique_ptr<std::atomic<uint64_t> > > vec;
    uint64_t _size{};
public:

    AtomicCounters() = default;

    explicit AtomicCounters(uint64_t size = 8);

    void Init(uint64_t size = 8);

    uint64_t IncCount(const uint64_t &index, const uint64_t &value) {
        return vec[index % _size]->fetch_add(value, std::memory_order_release);
    }

    uint64_t DecCount(const uint64_t &index, const uint64_t &value) {
        return vec[index % _size]->fetch_sub(value, std::memory_order_release);
    }

    void SetCount(const uint64_t &value) {
        for (auto &i: vec) {
            i->store(value, std::memory_order_release);
        }
    }

    void SetCount(const uint64_t &index, const uint64_t &value) {
        vec[index % _size]->store(value, std::memory_order_release);
    }

    uint64_t GetCount() {
        uint64_t ans = 0;
        for (auto &i: vec) {
            ans += i->load(std::memory_order_acquire);
        }
        return ans;
    }

    uint64_t GetCount(const uint64_t &index) {
        return vec[index % _size]->load(std::memory_order_acquire);
    }

    void ClearAll(const uint64_t value = 0) {
        (void) value;
        for (auto &i: vec) {
            i->store(0, std::memory_order_release);
        }
    }

    void Clear(const uint64_t &index) {
        vec[index % _size]->store(0, std::memory_order_release);
    }

    void Resize(const uint64_t &size) {
        if (size <= _size) return;
        vec.resize(size);
        for (uint64_t i = _size; i < size; i++) {
            vec.emplace_back(std::make_unique<std::atomic<uint64_t>>(0));
        }
    }
};

#endif //TAAS_ATOMIC_COUNTERS_H
