// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <vector>
#include <exceptions/EasyAssert.h>
#include "segcore/reduce_c.h"

#include "segcore/Reduce.h"
#include "segcore/ReduceStructure.h"
#include "common/Types.h"
#include "pb/milvus.pb.h"

using SearchResult = milvus::SearchResult;

int
MergeInto(int64_t num_queries, int64_t topk, float* distances, int64_t* uids, float* new_distances, int64_t* new_uids) {
    auto status = milvus::segcore::merge_into(num_queries, topk, distances, uids, new_distances, new_uids);
    return status.code();
}

struct MarshaledHitsPeerGroup {
    std::vector<std::string> hits_;
    std::vector<int64_t> blob_length_;
};

struct MarshaledHits {
    explicit MarshaledHits(int64_t num_group) {
        marshaled_hits_.resize(num_group);
    }

    int
    get_num_group() {
        return marshaled_hits_.size();
    }

    std::vector<MarshaledHitsPeerGroup> marshaled_hits_;
};

void
DeleteMarshaledHits(CMarshaledHits c_marshaled_hits) {
    auto hits = (MarshaledHits*)c_marshaled_hits;
    delete hits;
}

void
GetResultData(std::vector<std::vector<int64_t>>& search_records,
              std::vector<SearchResult*>& search_results,
              int64_t query_offset,
              bool* is_selected,
              int64_t topk) {
    auto num_segments = search_results.size();
    AssertInfo(num_segments > 0, "num segment must greater than 0");
    std::vector<SearchResultPair> result_pairs;
    for (int j = 0; j < num_segments; ++j) {
        auto distance = search_results[j]->result_distances_[query_offset];
        auto search_result = search_results[j];
        AssertInfo(search_result != nullptr, "search result must not equal to nullptr");
        result_pairs.push_back(SearchResultPair(distance, search_result, query_offset, j));
    }
    int64_t loc_offset = query_offset;
    AssertInfo(topk > 0, "topk must greater than 0");
    for (int i = 0; i < topk; ++i) {
        result_pairs[0].reset_distance();
        std::sort(result_pairs.begin(), result_pairs.end(), std::greater<>());
        auto& result_pair = result_pairs[0];
        auto index = result_pair.index_;
        is_selected[index] = true;
        result_pair.search_result_->result_offsets_.push_back(loc_offset++);
        search_records[index].push_back(result_pair.offset_++);
    }
}

void
ResetSearchResult(std::vector<std::vector<int64_t>>& search_records,
                  std::vector<SearchResult*>& search_results,
                  bool* is_selected) {
    auto num_segments = search_results.size();
    AssertInfo(num_segments > 0, "num segment must greater than 0");
    for (int i = 0; i < num_segments; i++) {
        if (is_selected[i] == false) {
            continue;
        }
        auto search_result = search_results[i];
        AssertInfo(search_result != nullptr, "search result must not equal to nullptr");

        std::vector<float> result_distances;
        std::vector<int64_t> internal_seg_offsets;

        for (int j = 0; j < search_records[i].size(); j++) {
            auto& offset = search_records[i][j];
            auto distance = search_result->result_distances_[offset];
            auto internal_seg_offset = search_result->internal_seg_offsets_[offset];
            result_distances.push_back(distance);
            internal_seg_offsets.push_back(internal_seg_offset);
        }

        search_result->result_distances_ = result_distances;
        search_result->internal_seg_offsets_ = internal_seg_offsets;
    }
}

CStatus
ReduceSearchResults(CSearchResult* c_search_results, int64_t num_segments, bool* is_selected) {
    try {
        std::vector<SearchResult*> search_results;
        for (int i = 0; i < num_segments; ++i) {
            search_results.push_back((SearchResult*)c_search_results[i]);
        }
        auto topk = search_results[0]->topk_;
        auto num_queries = search_results[0]->num_queries_;
        std::vector<std::vector<int64_t>> search_records(num_segments);

        int64_t query_offset = 0;
        for (int j = 0; j < num_queries; ++j) {
            GetResultData(search_records, search_results, query_offset, is_selected, topk);
            query_offset += topk;
        }
        ResetSearchResult(search_records, search_results, is_selected);
        auto status = CStatus();
        status.error_code = Success;
        status.error_msg = "";
        return status;
    } catch (std::exception& e) {
        auto status = CStatus();
        status.error_code = UnexpectedError;
        status.error_msg = strdup(e.what());
        return status;
    }
}

CStatus
ReorganizeSearchResults(CMarshaledHits* c_marshaled_hits,
                        CPlaceholderGroup* c_placeholder_groups,
                        int64_t num_groups,
                        CSearchResult* c_search_results,
                        bool* is_selected,
                        int64_t num_segments,
                        CSearchPlan c_plan) {
    try {
        auto marshaledHits = std::make_unique<MarshaledHits>(num_groups);
        auto topk = GetTopK(c_plan);
        std::vector<int64_t> num_queries_peer_group(num_groups);
        int64_t total_num_queries = 0;
        for (int i = 0; i < num_groups; i++) {
            auto num_queries = GetNumOfQueries(c_placeholder_groups[i]);
            num_queries_peer_group[i] = num_queries;
            total_num_queries += num_queries;
        }

        std::vector<float> result_distances(total_num_queries * topk);
        std::vector<int64_t> result_ids(total_num_queries * topk);
        std::vector<std::vector<char>> row_datas(total_num_queries * topk);
        std::vector<char> temp_ids;

        std::vector<int64_t> counts(num_segments);
        for (int i = 0; i < num_segments; i++) {
            if (is_selected[i] == false) {
                continue;
            }
            auto search_result = (SearchResult*)c_search_results[i];
            AssertInfo(search_result != nullptr, "search result must not equal to nullptr");
            auto size = search_result->result_offsets_.size();
#pragma omp parallel for
            for (int j = 0; j < size; j++) {
                auto loc = search_result->result_offsets_[j];
                result_distances[loc] = search_result->result_distances_[j];
                row_datas[loc] = search_result->row_data_[j];
                memcpy(&result_ids[loc], search_result->row_data_[j].data(), sizeof(int64_t));
            }
            counts[i] = size;
        }

        int64_t total_count = 0;
        for (int i = 0; i < num_segments; i++) {
            total_count += counts[i];
        }
        AssertInfo(total_count == total_num_queries * topk,
                   "the reduces result's size less than total_num_queries*topk");

        int64_t last_offset = 0;
        for (int i = 0; i < num_groups; i++) {
            MarshaledHitsPeerGroup& hits_peer_group = (*marshaledHits).marshaled_hits_[i];
            hits_peer_group.hits_.resize(num_queries_peer_group[i]);
            hits_peer_group.blob_length_.resize(num_queries_peer_group[i]);
            std::vector<milvus::proto::milvus::Hits> hits(num_queries_peer_group[i]);
#pragma omp parallel for
            for (int m = 0; m < num_queries_peer_group[i]; m++) {
                for (int n = 0; n < topk; n++) {
                    int64_t result_offset = last_offset + m * topk + n;
                    hits[m].add_ids(result_ids[result_offset]);
                    hits[m].add_scores(result_distances[result_offset]);
                    auto& row_data = row_datas[result_offset];
                    hits[m].add_row_data(row_data.data(), row_data.size());
                }
            }
            last_offset = last_offset + num_queries_peer_group[i] * topk;

#pragma omp parallel for
            for (int j = 0; j < num_queries_peer_group[i]; j++) {
                auto blob = hits[j].SerializeAsString();
                hits_peer_group.hits_[j] = blob;
                hits_peer_group.blob_length_[j] = blob.size();
            }
        }

        auto status = CStatus();
        status.error_code = Success;
        status.error_msg = "";
        auto marshled_res = (CMarshaledHits)marshaledHits.release();
        *c_marshaled_hits = marshled_res;
        return status;
    } catch (std::exception& e) {
        auto status = CStatus();
        status.error_code = UnexpectedError;
        status.error_msg = strdup(e.what());
        *c_marshaled_hits = nullptr;
        return status;
    }
}

CStatus
ReorganizeSingleSearchResult(CMarshaledHits* c_marshaled_hits,
                             CPlaceholderGroup* c_placeholder_groups,
                             int64_t num_groups,
                             CSearchResult c_search_result,
                             CSearchPlan c_plan) {
    try {
        auto marshaledHits = std::make_unique<MarshaledHits>(num_groups);
        auto search_result = (SearchResult*)c_search_result;
        auto topk = GetTopK(c_plan);
        std::vector<int64_t> num_queries_peer_group;
        int64_t total_num_queries = 0;
        for (int i = 0; i < num_groups; i++) {
            auto num_queries = GetNumOfQueries(c_placeholder_groups[i]);
            num_queries_peer_group.push_back(num_queries);
        }

        int64_t last_offset = 0;
        for (int i = 0; i < num_groups; i++) {
            MarshaledHitsPeerGroup& hits_peer_group = (*marshaledHits).marshaled_hits_[i];
            hits_peer_group.hits_.resize(num_queries_peer_group[i]);
            hits_peer_group.blob_length_.resize(num_queries_peer_group[i]);
            std::vector<milvus::proto::milvus::Hits> hits(num_queries_peer_group[i]);
#pragma omp parallel for
            for (int m = 0; m < num_queries_peer_group[i]; m++) {
                for (int n = 0; n < topk; n++) {
                    int64_t result_offset = last_offset + m * topk + n;
                    hits[m].add_scores(search_result->result_distances_[result_offset]);
                    auto& row_data = search_result->row_data_[result_offset];
                    hits[m].add_row_data(row_data.data(), row_data.size());
                    int64_t result_id;
                    memcpy(&result_id, row_data.data(), sizeof(int64_t));
                    hits[m].add_ids(result_id);
                }
            }
            last_offset = last_offset + num_queries_peer_group[i] * topk;

#pragma omp parallel for
            for (int j = 0; j < num_queries_peer_group[i]; j++) {
                auto blob = hits[j].SerializeAsString();
                hits_peer_group.hits_[j] = blob;
                hits_peer_group.blob_length_[j] = blob.size();
            }
        }

        auto status = CStatus();
        status.error_code = Success;
        status.error_msg = "";
        auto marshled_res = (CMarshaledHits)marshaledHits.release();
        *c_marshaled_hits = marshled_res;
        return status;
    } catch (std::exception& e) {
        auto status = CStatus();
        status.error_code = UnexpectedError;
        status.error_msg = strdup(e.what());
        *c_marshaled_hits = nullptr;
        return status;
    }
}

int64_t
GetHitsBlobSize(CMarshaledHits c_marshaled_hits) {
    int64_t total_size = 0;
    auto marshaled_hits = (MarshaledHits*)c_marshaled_hits;
    auto num_group = marshaled_hits->get_num_group();
    for (int i = 0; i < num_group; i++) {
        auto& length_vector = marshaled_hits->marshaled_hits_[i].blob_length_;
        for (int j = 0; j < length_vector.size(); j++) {
            total_size += length_vector[j];
        }
    }
    return total_size;
}

void
GetHitsBlob(CMarshaledHits c_marshaled_hits, const void* hits) {
    auto byte_hits = (char*)hits;
    auto marshaled_hits = (MarshaledHits*)c_marshaled_hits;
    auto num_group = marshaled_hits->get_num_group();
    int offset = 0;
    for (int i = 0; i < num_group; i++) {
        auto& hits = marshaled_hits->marshaled_hits_[i];
        auto num_queries = hits.hits_.size();
        for (int j = 0; j < num_queries; j++) {
            auto blob_size = hits.blob_length_[j];
            memcpy(byte_hits + offset, hits.hits_[j].data(), blob_size);
            offset += blob_size;
        }
    }
}

int64_t
GetNumQueriesPeerGroup(CMarshaledHits c_marshaled_hits, int64_t group_index) {
    auto marshaled_hits = (MarshaledHits*)c_marshaled_hits;
    auto& hits = marshaled_hits->marshaled_hits_[group_index].hits_;
    return hits.size();
}

void
GetHitSizePeerQueries(CMarshaledHits c_marshaled_hits, int64_t group_index, int64_t* hit_size_peer_query) {
    auto marshaled_hits = (MarshaledHits*)c_marshaled_hits;
    auto& blob_lens = marshaled_hits->marshaled_hits_[group_index].blob_length_;
    for (int i = 0; i < blob_lens.size(); i++) {
        hit_size_peer_query[i] = blob_lens[i];
    }
}
