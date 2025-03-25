//
// Created by Star on 2025/3/24.
//

#include "crdt_utils.h"


uint64_t now_to_us(){
    // 获取系统当前时间
    auto today = std::chrono::system_clock::now();
    // 获得1970年1月1日到现在的时间间隔
    auto time_duration = today.time_since_epoch();
    // 时间间隔以微妙的形式展现
    std::chrono::microseconds ms_duration = std::chrono::duration_cast<std::chrono::microseconds>(time_duration);
    uint64_t timestamp = ms_duration.count();
    return timestamp;
}