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

package allocator

import (
	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/tso"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type GIDAllocator interface {
	Alloc(count uint32) (UniqueID, UniqueID, error)
	AllocOne() (UniqueID, error)
	UpdateID() error
}

// GlobalTSOAllocator is the global single point TSO allocator.
type GlobalIDAllocator struct {
	allocator tso.Allocator
}

func NewGlobalIDAllocator(key string, base kv.TxnKV) *GlobalIDAllocator {
	allocator := tso.NewGlobalTSOAllocator(key, base)
	allocator.SetLimitMaxLogic(false)
	return &GlobalIDAllocator{
		allocator: allocator,
	}
}

// Initialize will initialize the created global TSO allocator.
func (gia *GlobalIDAllocator) Initialize() error {
	return gia.allocator.Initialize()
}

// GenerateTSO is used to generate a given number of TSOs.
// Make sure you have initialized the TSO allocator before calling.
func (gia *GlobalIDAllocator) Alloc(count uint32) (typeutil.UniqueID, typeutil.UniqueID, error) {
	timestamp, err := gia.allocator.GenerateTSO(count)
	if err != nil {
		return 0, 0, err
	}
	idEnd := typeutil.UniqueID(timestamp) + 1
	idStart := idEnd - int64(count)
	return idStart, idEnd, nil
}

func (gia *GlobalIDAllocator) AllocOne() (typeutil.UniqueID, error) {
	timestamp, err := gia.allocator.GenerateTSO(1)
	if err != nil {
		return 0, err
	}
	idStart := typeutil.UniqueID(timestamp)
	return idStart, nil
}

func (gia *GlobalIDAllocator) UpdateID() error {
	return gia.allocator.UpdateTSO()
}
