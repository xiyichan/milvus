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

package querycoord

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
)

func startQueryCoord(ctx context.Context) (*QueryCoord, error) {
	factory := msgstream.NewPmsFactory()

	coord, err := NewQueryCoord(ctx, factory)
	if err != nil {
		return nil, err
	}

	rootCoord := newRootCoordMock()
	rootCoord.createCollection(defaultCollectionID)
	rootCoord.createPartition(defaultCollectionID, defaultPartitionID)

	dataCoord, err := newDataCoordMock(ctx)
	if err != nil {
		return nil, err
	}

	coord.SetRootCoord(rootCoord)
	coord.SetDataCoord(dataCoord)

	err = coord.Register()
	if err != nil {
		return nil, err
	}
	err = coord.Init()
	if err != nil {
		return nil, err
	}
	err = coord.Start()
	if err != nil {
		return nil, err
	}
	return coord, nil
}

func TestQueryNode_MultiNode_stop(t *testing.T) {
	baseCtx := context.Background()

	queryCoord, err := startQueryCoord(baseCtx)
	assert.Nil(t, err)

	queryNode1, err := startQueryNodeServer(baseCtx)
	assert.Nil(t, err)

	//queryNode2, err := startQueryNodeServer(baseCtx)
	//assert.Nil(t, err)

	//queryNode3, err := startQueryNodeServer(baseCtx)
	//assert.Nil(t, err)

	//queryNode4, err := startQueryNodeServer(baseCtx)
	//assert.Nil(t, err)

	queryNode5, err := startQueryNodeServer(baseCtx)
	assert.Nil(t, err)

	time.Sleep(2 * time.Second)
	queryNode1.stop()
	//queryNode2.stop()
	//queryNode3.stop()
	//queryNode4.stop()

	queryCoord.LoadCollection(baseCtx, &querypb.LoadCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_LoadCollection,
		},
		CollectionID: defaultCollectionID,
		Schema:       genCollectionSchema(defaultCollectionID, false),
	})
	time.Sleep(2 * time.Second)
	_, err = queryCoord.ReleaseCollection(baseCtx, &querypb.ReleaseCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_ReleaseCollection,
		},
		CollectionID: defaultCollectionID,
	})
	assert.Nil(t, err)
	time.Sleep(2 * time.Second)
	nodes, err := queryCoord.cluster.onServiceNodes()
	assert.Nil(t, err)
	queryNode5.stop()

	for {
		allOffline := true
		for nodeID := range nodes {
			_, err = queryCoord.cluster.getNodeByID(nodeID)
			if err == nil {
				allOffline = false
				time.Sleep(time.Second)
				break
			}
		}
		if allOffline {
			break
		}
		log.Debug("wait all queryNode offline")
	}
	queryCoord.Stop()
}

func TestQueryNode_MultiNode_reStart(t *testing.T) {
	baseCtx := context.Background()

	queryCoord, err := startQueryCoord(baseCtx)
	assert.Nil(t, err)

	queryNode1, err := startQueryNodeServer(baseCtx)
	assert.Nil(t, err)

	//queryNode2, err := startQueryNodeServer(baseCtx)
	//assert.Nil(t, err)

	time.Sleep(2 * time.Second)
	queryCoord.LoadCollection(baseCtx, &querypb.LoadCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_LoadCollection,
		},
		CollectionID: defaultCollectionID,
		Schema:       genCollectionSchema(defaultCollectionID, false),
	})
	queryNode1.stop()
	//queryNode2.stop()
	queryNode3, err := startQueryNodeServer(baseCtx)
	assert.Nil(t, err)
	//queryNode4, err := startQueryNodeServer(baseCtx)
	//assert.Nil(t, err)
	//queryNode5, err := startQueryNodeServer(baseCtx)
	//assert.Nil(t, err)

	time.Sleep(2 * time.Second)
	_, err = queryCoord.ReleaseCollection(baseCtx, &querypb.ReleaseCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_ReleaseCollection,
		},
		CollectionID: defaultCollectionID,
	})
	assert.Nil(t, err)
	nodes, err := queryCoord.cluster.onServiceNodes()
	assert.Nil(t, err)
	queryNode3.stop()
	//queryNode4.stop()
	//queryNode5.stop()

	for {
		allOffline := true
		for nodeID := range nodes {
			_, err = queryCoord.cluster.getNodeByID(nodeID)
			if err == nil {
				allOffline = false
				time.Sleep(time.Second)
				break
			}
		}
		if allOffline {
			break
		}
		log.Debug("wait all queryNode offline")
	}
	queryCoord.Stop()
}

func TestQueryNode_getMetrics(t *testing.T) {
	log.Info("TestQueryNode_getMetrics, todo")
}
