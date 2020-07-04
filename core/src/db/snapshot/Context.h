// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#pragma once

#include <iostream>
#include <map>
#include <string>
#include <vector>
#include "db/meta/MetaTypes.h"
#include "db/snapshot/Resources.h"
#include "db/snapshot/Snapshot.h"

namespace milvus {
namespace engine {
namespace snapshot {

struct PartitionContext {
    std::string name;
    ID_TYPE id = 0;
    LSN_TYPE lsn = 0;

    std::string
    ToString() const;
};

struct SegmentFileContext {
    std::string field_name;
    std::string field_element_name;
    ID_TYPE segment_id;
    ID_TYPE partition_id;
    ID_TYPE collection_id;
};

struct LoadOperationContext {
    ID_TYPE id = 0;
    State state = INVALID;
    std::string name;
};

struct OperationContext {
    explicit OperationContext(const ScopedSnapshotT& ss = ScopedSnapshotT()) : prev_ss(ss) {
    }

    ScopedSnapshotT latest_ss;
    ScopedSnapshotT prev_ss;
    SegmentPtr new_segment = nullptr;
    SegmentCommitPtr new_segment_commit = nullptr;
    std::vector<SegmentCommitPtr> new_segment_commits;
    PartitionPtr new_partition = nullptr;
    PartitionCommitPtr new_partition_commit = nullptr;
    std::vector<PartitionCommitPtr> new_partition_commits;
    SchemaCommitPtr new_schema_commit = nullptr;
    CollectionCommitPtr new_collection_commit = nullptr;
    CollectionPtr new_collection = nullptr;

    SegmentFilePtr stale_segment_file = nullptr;
    std::vector<SegmentPtr> stale_segments;

    FieldPtr prev_field = nullptr;
    FieldElementPtr prev_field_element = nullptr;
    FieldElementPtr stale_field_element = nullptr;

    SegmentPtr prev_segment = nullptr;
    SegmentCommitPtr prev_segment_commit = nullptr;
    PartitionPtr prev_partition = nullptr;
    PartitionCommitPtr prev_partition_commit = nullptr;
    CollectionCommitPtr prev_collection_commit = nullptr;
    PartitionCommitPtr stale_partition_commit = nullptr;

    SegmentFile::VecT new_segment_files;
    CollectionPtr collection = nullptr;
    LSN_TYPE lsn = 0;

    std::string
    ToString() const;
};

struct CreateCollectionContext {
    CollectionPtr collection = nullptr;
    std::map<FieldPtr, std::vector<FieldElementPtr>> fields_schema;
    CollectionCommitPtr collection_commit = nullptr;
    LSN_TYPE lsn = 0;

    std::string
    ToString() const;
};

}  // namespace snapshot
}  // namespace engine
}  // namespace milvus
