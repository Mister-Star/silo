//
// Created by user on 25-3-24.
//

#ifndef SILO_CRDTCONTEXT_H
#define SILO_CRDTCONTEXT_H

#include <vector>
#include <string>
#include "blocking_concurrent_queue.hpp"

template<typename T>
using  BlockingConcurrentQueue = moodycamel::BlockingConcurrentQueue<T>;

enum IsolationLevel {
    RC = 1,
    RR = 2,
    SI = 3,
    SER = 4,
};

class CRDTContext {
public:
    static uint64_t kEpochSize_us, kCacheMaxLength, print_mode_size,
            kShardNum, kNKeys, kWorkerThreadNum, kMergeThreadNum;
    static IsolationLevel kCRDTIsolation;
    static uint64_t YCSB_OPs, YCSB_Read, YCSB_Write;
    static uint64_t kInitTxnNum, kTxnRowSize;
    static void GetCRDTConfig();
};


#endif //SILO_CRDTCONTEXT_H
