//
// Created by user on 25-3-24.
//

#include "crdt_context.h"
#include "tinyxml2.h"

uint64_t CRDTContext::kEpochSize_us = 10000 /** us */, CRDTContext::kCacheMaxLength = 200000,
        CRDTContext::print_mode_size = 1000,
        CRDTContext::kShardNum = 1, CRDTContext::kMergeThreadNum = 0;
IsolationLevel CRDTContext::kCRDTIsolation = IsolationLevel::SI;

void CRDTContext::GetCRDTConfig(const std::string &config_file_path){
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
}