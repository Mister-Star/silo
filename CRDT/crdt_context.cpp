//
// Created by user on 25-3-24.
//

#include <filesystem>
#include <iostream>
#include "crdt_context.h"
#include "tinyxml2.h"

uint64_t CRDTContext::kEpochSize_us = 1000000 /** us */, CRDTContext::kCacheMaxLength = 200000,
        CRDTContext::print_mode_size = 1000,
        CRDTContext::kShardNum = 1, CRDTContext::kNKeys = 0, CRDTContext::kWorkerThreadNum = 1,
        CRDTContext::kMergeThreadNum = 1;
IsolationLevel CRDTContext::kCRDTIsolation = IsolationLevel::SI;
uint64_t CRDTContext::YCSB_OPs = 10, CRDTContext::YCSB_Read = 80, CRDTContext::YCSB_Write = 20;

namespace fs = std::filesystem;

std::string getSourceDir() {
// FILE 是当前源代码文件的绝对路径（由编译器提供）
    std::string full_path = __FILE__;
    return fs::path(full_path).parent_path().string(); // 提取目录部分
}

void CRDTContext::GetCRDTConfig(){
    std::cerr << "GetCRDTConfig" << std::endl;
    std::string config_file_path = getSourceDir() + "/Config.xml";
    std::cerr << config_file_path << std::endl;

    tinyxml2::XMLDocument doc;
    doc.LoadFile(config_file_path.c_str());
    auto* root=doc.RootElement();

    tinyxml2::XMLElement* epoch_size_us = root->FirstChildElement("epoch_size_us");
    kEpochSize_us= std::stoull(epoch_size_us->GetText());
    tinyxml2::XMLElement* cachemaxlength = root->FirstChildElement("cache_max_length");
    kCacheMaxLength = std::stoull(cachemaxlength->GetText());

    tinyxml2::XMLElement* mode_size_t = root->FirstChildElement("print_mode_size");
    print_mode_size = std::stoull(mode_size_t->GetText());

    tinyxml2::XMLElement* shard_num = root->FirstChildElement("shard_num");
    kShardNum= std::stoull(shard_num->GetText());
    tinyxml2::XMLElement* merge_thread_num = root->FirstChildElement("merge_thread_num");
    kMergeThreadNum = std::stoull(merge_thread_num->GetText());

    tinyxml2::XMLElement* isolation = root->FirstChildElement("isolation");
    switch (std::stoull(isolation->GetText())) {
        case 1:
            kCRDTIsolation = IsolationLevel::RC;
            break;
        case 2:
            kCRDTIsolation = IsolationLevel::RR;
            break;
        case 3:
            kCRDTIsolation = IsolationLevel::SI;
            break;
        case 4:
            kCRDTIsolation = IsolationLevel::SER;
            break;
        default:
            kCRDTIsolation = IsolationLevel::SI;
            break;
    }
    tinyxml2::XMLElement* ycsb_ops = root->FirstChildElement("ycsb_ops");
    YCSB_OPs = std::stoull(ycsb_ops->GetText());
    tinyxml2::XMLElement* ycsb_read = root->FirstChildElement("ycsb_read");
    YCSB_Read = std::stoull(ycsb_read->GetText());
    tinyxml2::XMLElement* ycsb_write = root->FirstChildElement("ycsb_write");
    YCSB_Write = std::stoull(ycsb_write->GetText());

}