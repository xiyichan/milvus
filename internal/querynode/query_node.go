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

package querynode

/*

#cgo CFLAGS: -I${SRCDIR}/../core/output/include

#cgo LDFLAGS: -L${SRCDIR}/../core/output/lib -lmilvus_segcore -Wl,-rpath=${SRCDIR}/../core/output/lib

#include "segcore/collection_c.h"
#include "segcore/segment_c.h"
#include "segcore/segcore_init_c.h"

*/
import "C"

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"

	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/kv"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/retry"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type QueryNode struct {
	queryNodeLoopCtx    context.Context
	queryNodeLoopCancel context.CancelFunc

	stateCode atomic.Value

	// internal components
	historical *historical
	streaming  *streaming

	// internal services
	queryService *queryService

	// clients
	rootCoord  types.RootCoord
	indexCoord types.IndexCoord

	msFactory msgstream.Factory
	scheduler *taskScheduler

	session *sessionutil.Session

	minioKV kv.BaseKV // minio minioKV
	etcdKV  *etcdkv.EtcdKV
}

func NewQueryNode(ctx context.Context, factory msgstream.Factory) *QueryNode {
	ctx1, cancel := context.WithCancel(ctx)
	node := &QueryNode{
		queryNodeLoopCtx:    ctx1,
		queryNodeLoopCancel: cancel,
		queryService:        nil,
		msFactory:           factory,
	}

	node.scheduler = newTaskScheduler(ctx1)
	node.UpdateStateCode(internalpb.StateCode_Abnormal)

	return node
}

// Register register query node at etcd
func (node *QueryNode) Register() error {
	log.Debug("query node session info", zap.String("metaPath", Params.MetaRootPath), zap.Strings("etcdEndPoints", Params.EtcdEndpoints))
	node.session = sessionutil.NewSession(node.queryNodeLoopCtx, Params.MetaRootPath, Params.EtcdEndpoints)
	node.session.Init(typeutil.QueryNodeRole, Params.QueryNodeIP+":"+strconv.FormatInt(Params.QueryNodePort, 10), false)
	Params.QueryNodeID = node.session.ServerID
	log.Debug("query nodeID", zap.Int64("nodeID", Params.QueryNodeID))
	log.Debug("query node address", zap.String("address", node.session.Address))

	// This param needs valid QueryNodeID
	Params.initMsgChannelSubName()
	return nil
}

func (node *QueryNode) Init() error {
	//ctx := context.Background()
	connectEtcdFn := func() error {
		etcdClient, err := clientv3.New(clientv3.Config{Endpoints: Params.EtcdEndpoints})
		if err != nil {
			return err
		}
		etcdKV := etcdkv.NewEtcdKV(etcdClient, Params.MetaRootPath)
		node.etcdKV = etcdKV
		return err
	}
	log.Debug("queryNode try to connect etcd")
	err := retry.Do(node.queryNodeLoopCtx, connectEtcdFn, retry.Attempts(300))
	if err != nil {
		log.Debug("queryNode try to connect etcd failed", zap.Error(err))
		return err
	}
	log.Debug("queryNode try to connect etcd success")

	node.historical = newHistorical(node.queryNodeLoopCtx,
		node.rootCoord,
		node.indexCoord,
		node.msFactory,
		node.etcdKV)
	node.streaming = newStreaming(node.queryNodeLoopCtx, node.msFactory, node.etcdKV)

	C.SegcoreInit()

	if node.rootCoord == nil {
		log.Error("null root coordinator detected")
	}

	if node.indexCoord == nil {
		log.Error("null index coordinator detected")
	}

	return nil
}

func (node *QueryNode) Start() error {
	var err error
	m := map[string]interface{}{
		"PulsarAddress":  Params.PulsarAddress,
		"KafkaAddress":   Params.KafkaAddress,
		"ReceiveBufSize": 1024,
		"PulsarBufSize":  1024}
	err = node.msFactory.SetParams(m)
	if err != nil {
		return err
	}

	// init services and manager
	// TODO: pass node.streaming.replica to search service
	node.queryService = newQueryService(node.queryNodeLoopCtx,
		node.historical,
		node.streaming,
		node.msFactory)

	// start task scheduler
	go node.scheduler.Start()

	// start services
	go node.historical.start()
	node.UpdateStateCode(internalpb.StateCode_Healthy)
	return nil
}

func (node *QueryNode) Stop() error {
	node.UpdateStateCode(internalpb.StateCode_Abnormal)
	node.queryNodeLoopCancel()

	// close services
	if node.historical != nil {
		node.historical.close()
	}
	if node.streaming != nil {
		node.streaming.close()
	}
	if node.queryService != nil {
		node.queryService.close()
	}
	if node.queryService != nil {
		node.queryService.close()
	}
	return nil
}

func (node *QueryNode) UpdateStateCode(code internalpb.StateCode) {
	node.stateCode.Store(code)
}

func (node *QueryNode) SetRootCoord(rc types.RootCoord) error {
	if rc == nil {
		return errors.New("null root coordinator interface")
	}
	node.rootCoord = rc
	return nil
}

func (node *QueryNode) SetIndexCoord(index types.IndexCoord) error {
	if index == nil {
		return errors.New("null index coordinator interface")
	}
	node.indexCoord = index
	return nil
}
