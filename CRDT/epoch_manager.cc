//
// Created by user on 25-3-25.
//

#include <sys/time.h>
#include <cstdarg>

#include "epoch_manager.h"

using namespace std;

bool EpochManager::timerStop = false;
volatile std::atomic<uint64_t> EpochManager::logical_epoch(1), EpochManager::physical_epoch(0), EpochManager::push_down_epoch(1);
uint64_t EpochManager::max_length = 10000;
//epoch merge state
std::vector<std::unique_ptr<std::atomic<bool>>> EpochManager::merge_complete, EpochManager::abort_set_merge_complete,
EpochManager::commit_complete, EpochManager::record_committed, EpochManager::result_returned, EpochManager::is_current_epoch_abort;


// EpochPhysicalTimerManagerThreadMain中得到的当前微秒级别的时间戳
uint64_t start_time_ll, start_physical_epoch = 1;
struct timeval start_time;

// EpochManager是否初始化完成
std::atomic<int> init_ok_num(0);
std::atomic<bool> is_epoch_advance_started(false), test_start(false);


uint64_t last_total_commit_txn_num = 0, total_commit_txn_num = 0;
const uint64_t sleep_time = 10, logical_sleep_timme = 10, storage_sleep_time = 10, merge_sleep_time = 10, message_sleep_time = 10;
std::atomic<uint64_t> merge_epoch(1), abort_set_epoch(1),
        commit_epoch(1), redo_log_epoch(1), clear_epoch(1);

void InitEpochTimerManager(){
    std::cerr << "EpochPhysicalThread start StaticEpochInit  " << std::endl;
    Merge::Init();
    CRDTCounters::StaticInit();

    EpochManager::max_length = CRDTContext::kCacheMaxLength;
    //==========Logical Epoch Merge State=============
    EpochManager::merge_complete.resize(EpochManager::max_length);
    EpochManager::abort_set_merge_complete.resize(EpochManager::max_length);
    EpochManager::commit_complete.resize(EpochManager::max_length);
    EpochManager::record_committed.resize(EpochManager::max_length);
    EpochManager::result_returned.resize(EpochManager::max_length);
    EpochManager::is_current_epoch_abort.resize(EpochManager::max_length);

    for(int i = 0; i < static_cast<int>(EpochManager::max_length); i ++) {
        EpochManager::merge_complete[i] = std::make_unique<std::atomic<bool>>(false);
        EpochManager::abort_set_merge_complete[i] = std::make_unique<std::atomic<bool>>(false);
        EpochManager::commit_complete[i] = std::make_unique<std::atomic<bool>>(false);
        EpochManager::record_committed[i] = std::make_unique<std::atomic<bool>>(false);
        EpochManager::result_returned[i] = std::make_unique<std::atomic<bool>>(false);
        EpochManager::is_current_epoch_abort[i] = std::make_unique<std::atomic<bool>>(false);

    }

    std::cerr << "EpochPhysicalThread finish StaticEpochInit  " << std::endl;
    init_ok_num.fetch_add(1);
}


uint64_t GetSleeptime(){
    uint64_t sleep_time_temp;
    // current_time由两部分组成，tv_sec + tv_usec，代表秒和毫秒数，合起来就是总的时间戳
    struct timeval current_time{};
    uint64_t current_time_ll;
    gettimeofday(&current_time, nullptr);
    // 得到目前的微秒级时间戳
    current_time_ll = current_time.tv_sec * 1000000 + current_time.tv_usec;
    sleep_time_temp = current_time_ll - (start_time_ll + (long)(EpochManager::GetPhysicalEpoch() - start_physical_epoch) * CRDTContext::kEpochSize_us);
    if(sleep_time_temp >= CRDTContext::kEpochSize_us){
        return 0;
    }
    else{
        return CRDTContext::kEpochSize_us - sleep_time_temp;
    }
}

std::string PrintfToString(const char* format, ...) {
    char buffer[5120];
    va_list args;
    va_start(args, format);
    std::vsnprintf(buffer, sizeof(buffer), format, args);
    va_end(args);

    return std::string(buffer);
}

void OUTPUTLOG(const std::string& s, uint64_t& epoch_){
    auto epoch_mod = epoch_ % EpochManager::max_length;
    auto phy_e = EpochManager::GetPhysicalEpoch();
    auto log_e = epoch_;
    cerr << PrintfToString("%60s \n\
        physical                     %6lu, logical                  %6lu,   \
        dist                %6lu  \n\
\
        ShouldExecTxnNum             %6lu, ExecedTxnNum             %6lu,   \
        ShouldReadValidateTxnNum     %6lu, ReadValidatedTxnNum      %6lu, \n\
        ShouldMergeTxnNum            %6lu, MergedTxnNum             %6lu,   \
        ShouldCommitTxnNum           %6lu, CommittedTxnNum          %6lu, \n\
        ShouldRecordCommitTxnNum     %6lu, RecordCommittedTxnNum    %6lu,   \
        ResultreturnTxnNum           %6lu, ResultreturnedTxnNum     %6lu, \n\
        merge_num                    %6lu, time          %lu \n",
       s.c_str(),
       phy_e, epoch_, phy_e - epoch_,

       CRDTCounters::GetEpochShouldExecTxnNum(epoch_mod), CRDTCounters::GetEpochExecTxnNum(epoch_mod),
       CRDTCounters::GetAllThreadLocalCountNum(epoch_mod, CRDTCounters::epoch_should_read_validate_txn_num_vec),
       CRDTCounters::GetAllThreadLocalCountNum(epoch_mod, CRDTCounters::epoch_read_validated_txn_num_vec),
       CRDTCounters::GetAllThreadLocalCountNum(epoch_mod, CRDTCounters::epoch_should_merge_txn_num_vec),
       CRDTCounters::GetAllThreadLocalCountNum(epoch_mod, CRDTCounters::epoch_merged_txn_num_vec),
       CRDTCounters::GetAllThreadLocalCountNum(epoch_mod, CRDTCounters::epoch_should_commit_txn_num_vec),
       CRDTCounters::GetAllThreadLocalCountNum(epoch_mod, CRDTCounters::epoch_committed_txn_num_vec),
       CRDTCounters::GetAllThreadLocalCountNum(epoch_mod, CRDTCounters::epoch_record_commit_txn_num_vec),
       CRDTCounters::GetAllThreadLocalCountNum(epoch_mod, CRDTCounters::epoch_record_committed_txn_num_vec),
       CRDTCounters::GetAllThreadLocalCountNum(epoch_mod, CRDTCounters::epoch_result_return_txn_num_vec),
       CRDTCounters::GetAllThreadLocalCountNum(epoch_mod, CRDTCounters::epoch_result_returned_txn_num_vec),
       (uint64_t)0, now_to_us()
    ) << endl;
}

void EpochPhysicalTimerManagerThreadMain() {
    std::string name = "EpochPhysical";
    pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
    std::cerr << "EpochPhysicalTimerManagerThreadMain  " << std::endl;
    InitEpochTimerManager();
    while(!EpochManager::IsShardInitOK()) std::this_thread::yield();

    gettimeofday(&start_time, nullptr);
    start_time_ll = start_time.tv_sec * 1000000 + start_time.tv_usec;

    EpochManager::SetPhysicalEpoch(1);
    EpochManager::SetLogicalEpoch(1);
    auto epoch_ = EpochManager::GetPhysicalEpoch();
    auto logical = EpochManager::GetLogicalEpoch();
    test_start.store(true);
    is_epoch_advance_started.store(true);

    printf("=============  EpochManager Init 完成，数据库开始正常运行 ============= \n");

    while(!EpochManager::IsTimerStop()){
//        auto sleep_time_ = GetSleeptime();
//        std::cerr << "physical sleep_time " << sleep_time_ << std::endl;
//        usleep(sleep_time_);
        usleep(GetSleeptime());
        EpochManager::AddPhysicalEpoch();
        epoch_ ++;
        logical = EpochManager::GetLogicalEpoch();
        if(epoch_ % CRDTContext::print_mode_size == 0) {
            OUTPUTLOG("============= Epoch INFO ============= ", logical);
        }
        EpochManager::EpochCacheSafeCheck();
    }
        OUTPUTLOG("============= Epoch INFO ============= ", logical);
    printf("EpochTimerManager End!!!\n");
}


void MergeThreadMain(uint64_t thread_id) {
    std::string name = "EpochMerge" + std::to_string(thread_id);
    pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
//    std::cerr << "MergeThreadMain  " << thread_id << std::endl;
    while(init_ok_num.load() < 1) std::this_thread::yield();
    auto shard_id = thread_id % CRDTContext::kShardNum;
//    std::cerr << "thread start shardStatic init  " << thread_id  << " shard_id " << shard_id<< std::endl;
    Merge::ShardInit(shard_id);
    CRDTCounters::StaticInitShard(shard_id);
//    std::cerr << "thread shardStatic init  finished" << thread_id << " shard_id " << shard_id<< std::endl;
    init_ok_num.fetch_add(1);
    while(!EpochManager::IsShardInitOK()) std::this_thread::yield();
//    std::cerr << "thread MergeThreadLocalInit  " << thread_id << std::endl;
    Merge merge;
    merge.MergeThreadLocalInit(shard_id);
    std::cerr << "EpochMerge  " << thread_id << std::endl;
    merge.EpochMerge();
}

void EpochLogicalTimerManagerThreadMain() {
    std::string name = "EpochLogical";
    pthread_setname_np(pthread_self(), name.substr(0, 15).c_str());
    std::cerr << "EpochLogicalTimerManagerThreadMain  " << std::endl;
    while(!EpochManager::IsShardInitOK()) std::this_thread::yield();
    uint64_t epoch = 1;
    OUTPUTLOG("===== Logical Start Epoch的合并 ===== ", epoch);
    while(!EpochManager::IsShardInitOK()) usleep(logical_sleep_timme);
    while(!EpochManager::IsTimerStop()){
//        auto time1 = now_to_us();
//        std::cerr << "epoch >= EpochManager::GetPhysicalEpoch() " << epoch << std::endl;
        while(epoch >= EpochManager::GetPhysicalEpoch()) std::this_thread::yield();
//                LOG(INFO) << "**** Start Epoch Merge Epoch : " << epoch << "****\n";
//        std::cerr << "CheckEpochExecComplete " << epoch << std::endl;
        while(!CRDTCounters::CheckEpochExecComplete(epoch)) {
//            std::cerr << "EpochManager epoch" << epoch << " CheckEpochExecComplete exec tnx count: " <<
//                      CRDTCounters::epoch_should_exec_txn_num.GetCount(epoch)
//                      << "execed txn counters "<<  CRDTCounters::epoch_exec_txn_num.GetCount(epoch)
//                      << std::endl;
            std::this_thread::yield();
        }
//        std::cerr << "CheckEpochReadValidateComplete " << epoch << std::endl;
        while(!CRDTCounters::CheckEpochReadValidateComplete(epoch)) {
//            std::cerr << "EpochManager epoch" << epoch << " CheckEpochReadValidateComplete should read validate count: " <<
//            CRDTCounters::GetAllThreadLocalCountNum(epoch, 0, CRDTCounters::epoch_should_read_validate_txn_num_vec)
//            << "Read validated txn counters "<<  CRDTCounters::GetAllThreadLocalCountNum(epoch, 0, CRDTCounters::epoch_read_validated_txn_num_vec)
//            << std::endl;
            std::this_thread::yield();
        }
//        std::cerr << "EpochReadValidateComplete " << epoch << std::endl;

        while(!CRDTCounters::CheckEpochMergeComplete(epoch)) std::this_thread::yield();
//        std::cerr << "SetEpochMergeComplete " << epoch << std::endl;
        EpochManager::SetEpochMergeComplete(epoch, true);
        merge_epoch.fetch_add(1);
        EpochManager::SetAbortSetMergeComplete(epoch, true);
//        auto time5 = now_to_us();
//        std::cerr << "**** Finished Epoch Merge Epoch : " << epoch << ",time cost : " << time5 - time1 << "****" << std::endl;
//        LOG(INFO) << "**** Finished Epoch Merge Epoch : " << epoch << ",time cost : " << time5 - time1 << "****\n";
        abort_set_epoch.fetch_add(1);
//        auto time6 = now_to_us();
//        LOG(INFO) << "******* Finished Abort Set Merge Epoch : " << epoch << ",time cost : " << time6 - time5 << "********\n";
        while(!CRDTCounters::CheckEpochCommitComplete(epoch)) std::this_thread::yield();
//        std::cerr << "SetCommitComplete " << epoch << std::endl;
        EpochManager::SetCommitComplete(epoch, true);
        commit_epoch.fetch_add(1);

        while(!CRDTCounters::CheckEpochRecordCommitted(epoch)) std::this_thread::yield();
//        std::cerr << "SetRecordCommitted " << epoch << std::endl;
        EpochManager::SetRecordCommitted(epoch, true);

        while(!CRDTCounters::CheckEpochResultReturned(epoch)) std::this_thread::yield();
//        std::cerr << "SetResultReturned " << epoch << std::endl;
        EpochManager::SetResultReturned(epoch, true);

        EpochManager::AddLogicalEpoch();
//        auto time7 = now_to_us();
        auto epoch_commit_success_txn_num =
              CRDTCounters::GetAllThreadLocalCountNum(epoch, CRDTCounters::epoch_record_committed_txn_num_vec);
        total_commit_txn_num += epoch_commit_success_txn_num;///success

        if(epoch % CRDTContext::print_mode_size == 0) {
            OUTPUTLOG("===== Logical Start Epoch的合并 ===== ", epoch);
        }
        Merge::EpochClear(epoch);
        CRDTCounters::StaticClear(epoch);
        EpochManager::ClearMergeEpochState(epoch);
//        auto phy_e = EpochManager::GetPhysicalEpoch();
//        auto dist = phy_e - epoch;
//        if(dist > 1) std:: cerr << "logical epoch " << epoch << " dist " << dist << std::endl;
        epoch ++;
    }

    OUTPUTLOG("===== Logical End Epoch的合并 ===== ", epoch);
}


