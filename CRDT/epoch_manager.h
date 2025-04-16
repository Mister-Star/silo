//
// Created by user on 25-3-25.
//

#ifndef SILO_EPOCHMANAGER_H
#define SILO_EPOCHMANAGER_H

#pragma once

#include <unistd.h>
#include <atomic>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>
#include <vector>
#include <cassert>
#include <condition_variable>
#include <iostream>
#include <string>
#include <algorithm>

#include "atomic_counters.h"
#include "atomic_counters_cache.h"
#include "crdt_context.h"
#include "crdt_utils.h"
#include "crdt_counters.h"
#include "merge.h"

extern std::atomic<uint64_t> merge_epoch , abort_set_epoch ,
        commit_epoch , redo_log_epoch , clear_epoch ;
extern std::atomic<int> init_ok_num;

extern bool CheckRedoLogPushDownState();

extern void EpochLogicalTimerManagerThreadMain();
extern void EpochPhysicalTimerManagerThreadMain();
extern void MergeThreadMain(uint64_t thread_id);

std::string PrintfToString(const char* format, ...);
void OUTPUTLOG(const std::string& s, uint64_t& epoch);

class EpochManager {
public:
    static bool timerStop;
    static volatile std::atomic<uint64_t> logical_epoch, physical_epoch, push_down_epoch;
    static uint64_t max_length;

public:
    static std::vector<std::unique_ptr<std::atomic<bool>>>
        merge_complete, abort_set_merge_complete,
        commit_complete, record_committed, result_returned,
        is_current_epoch_abort;



    static void SetTimerStop(bool value) {timerStop = value;}
    static bool IsTimerStop() {return timerStop;}

    static bool IsEpochMergeComplete(uint64_t epoch) {return merge_complete[epoch % max_length]->load(std::memory_order_acquire);}
    static void SetEpochMergeComplete(uint64_t epoch, bool value) {merge_complete[epoch % max_length]->store(value, std::memory_order_release);}

    static bool IsAbortSetMergeComplete(uint64_t epoch) {return abort_set_merge_complete[epoch % max_length]->load(std::memory_order_acquire);}
    static void SetAbortSetMergeComplete(uint64_t epoch, bool value) {abort_set_merge_complete[epoch % max_length]->store(value, std::memory_order_release);}

    static bool IsCommitComplete(uint64_t epoch) {return commit_complete[epoch % max_length]->load(std::memory_order_acquire);}
    static void SetCommitComplete(uint64_t epoch, bool value) {commit_complete[epoch % max_length]->store(value, std::memory_order_release);}

    static bool IsRecordCommitted(uint64_t epoch){ return record_committed[epoch % max_length]->load(std::memory_order_acquire);}
    static void SetRecordCommitted(uint64_t epoch, bool value){ record_committed[epoch % max_length]->store(value, std::memory_order_release);}

    static bool IsResultReturned(uint64_t epoch){ return result_returned[epoch % max_length]->load(std::memory_order_acquire);}
    static void SetResultReturned(uint64_t epoch, bool value){ result_returned[epoch % max_length]->store(value, std::memory_order_release);}

    static bool IsCurrentEpochAbort(uint64_t epoch){ return is_current_epoch_abort[epoch % max_length]->load(std::memory_order_acquire);}
    static void SetCurrentEpochAbort(uint64_t epoch, bool value){ is_current_epoch_abort[epoch % max_length]->store(value, std::memory_order_release);}

    static void SetPhysicalEpoch(uint64_t value){ physical_epoch.store(value, std::memory_order_release);}
    static uint64_t AddPhysicalEpoch(){
        return physical_epoch.fetch_add(1, std::memory_order_release);
    }
    static uint64_t GetPhysicalEpoch(){ return physical_epoch.load(std::memory_order_acquire);}

    static void SetLogicalEpoch(uint64_t value){ logical_epoch.store(value, std::memory_order_release);}
    static uint64_t AddLogicalEpoch(){
        return logical_epoch.fetch_add(1, std::memory_order_release);
    }
    static uint64_t GetLogicalEpoch(){ return logical_epoch.load(std::memory_order_acquire);}

    static void SetPushDownEpoch(uint64_t value){ push_down_epoch.store(value, std::memory_order_release);}
    static uint64_t AddPushDownEpoch(){
        return push_down_epoch.fetch_add(1, std::memory_order_release);
    }
    static uint64_t GetPushDownEpoch(){ return push_down_epoch.load(std::memory_order_acquire);}


    static void ClearMergeEpochState(uint64_t& epoch) {
        auto epoch_mod = epoch %  max_length;
        merge_complete[epoch_mod]->store(false, std::memory_order_release);
        abort_set_merge_complete[epoch_mod]->store(false, std::memory_order_release);
        commit_complete[epoch_mod]->store(false, std::memory_order_release);
        record_committed[epoch_mod]->store(false, std::memory_order_release);
        result_returned[epoch_mod]->store(false, std::memory_order_release);
        is_current_epoch_abort[epoch_mod]->store(false, std::memory_order_release);
    }

    static void EpochCacheSafeCheck() {
        if(((GetLogicalEpoch() % max_length) ==  ((GetPhysicalEpoch() + 5) % max_length))) {
            uint64_t i = 0;
            OUTPUTLOG("Assert", reinterpret_cast<uint64_t &>(i));
            printf("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n");
            printf("+++++++++++++++Fata : Cache Size exceeded!!! +++++++++++++++++++++\n");
            printf("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n");
            SetTimerStop(true);
            assert(false);
        }
    }

    static bool IsShardInitOK() {
        return static_cast<uint64_t>(init_ok_num.load()) >= CRDTContext::kMergeThreadNum + 1;
    }

};


#endif //SILO_EPOCHMANAGER_H
