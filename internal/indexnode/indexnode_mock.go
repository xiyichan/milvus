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

package indexnode

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/log"

	"github.com/golang/protobuf/proto"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type Mock struct {
	Build   bool
	Failure bool

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	etcdKV *etcdkv.EtcdKV

	buildIndex chan *indexpb.CreateIndexRequest
}

func (inm *Mock) Init() error {
	if inm.Failure {
		return errors.New("IndexNode init failed")
	}
	inm.ctx, inm.cancel = context.WithCancel(context.Background())
	inm.buildIndex = make(chan *indexpb.CreateIndexRequest)
	return nil
}

func (inm *Mock) buildIndexTask() {
	log.Debug("IndexNodeMock wait for building index")
	defer inm.wg.Done()
	for {
		select {
		case <-inm.ctx.Done():
			return
		case req := <-inm.buildIndex:
			if inm.Failure && inm.Build {
				indexMeta := indexpb.IndexMeta{}

				_, values, versions, _ := inm.etcdKV.LoadWithPrefix2(req.MetaPath)
				_ = proto.UnmarshalText(values[0], &indexMeta)
				indexMeta.IndexFilePaths = []string{"IndexFilePath-1", "IndexFilePath-2"}
				indexMeta.State = commonpb.IndexState_Failed
				time.Sleep(time.Second)
				_ = inm.etcdKV.CompareVersionAndSwap(req.MetaPath, versions[0],
					proto.MarshalTextString(&indexMeta))
			}
			if inm.Build {
				indexMeta := indexpb.IndexMeta{}
				_, values, versions, _ := inm.etcdKV.LoadWithPrefix2(req.MetaPath)
				_ = proto.UnmarshalText(values[0], &indexMeta)
				indexMeta.IndexFilePaths = []string{"IndexFilePath-1", "IndexFilePath-2"}
				indexMeta.State = commonpb.IndexState_Failed
				time.Sleep(time.Second)
				_ = inm.etcdKV.CompareVersionAndSwap(req.MetaPath, versions[0],
					proto.MarshalTextString(&indexMeta))
				indexMeta.Version = indexMeta.Version + 1
				indexMeta.State = commonpb.IndexState_Finished
				_ = inm.etcdKV.CompareVersionAndSwap(req.MetaPath, versions[0]+1,
					proto.MarshalTextString(&indexMeta))
			}
		}
	}
}

func (inm *Mock) Start() error {
	inm.wg.Add(1)
	go inm.buildIndexTask()
	if inm.Failure {
		return errors.New("IndexNode start failed")
	}
	return nil
}

func (inm *Mock) Stop() error {
	inm.cancel()
	inm.wg.Wait()
	inm.etcdKV.RemoveWithPrefix("session/" + typeutil.IndexNodeRole)
	if inm.Failure {
		return errors.New("IndexNode stop failed")
	}
	return nil
}

func (inm *Mock) Register() error {
	if inm.Failure {
		return errors.New("IndexNode register failed")
	}
	inm.etcdKV, _ = etcdkv.NewEtcdKV(Params.EtcdEndpoints, Params.MetaRootPath)
	inm.etcdKV.RemoveWithPrefix("session/" + typeutil.IndexNodeRole)
	session := sessionutil.NewSession(context.Background(), Params.MetaRootPath, Params.EtcdEndpoints)
	session.Init(typeutil.IndexNodeRole, Params.IP+":"+strconv.Itoa(Params.Port), false)
	return nil
}

func (inm *Mock) GetComponentStates(ctx context.Context) (*internalpb.ComponentStates, error) {
	if inm.Failure {
		return &internalpb.ComponentStates{
			State: &internalpb.ComponentInfo{
				StateCode: internalpb.StateCode_Abnormal,
			},
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
			},
		}, nil
	}
	return &internalpb.ComponentStates{
		State: &internalpb.ComponentInfo{
			StateCode: internalpb.StateCode_Healthy,
		},
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
	}, nil
}

func (inm *Mock) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	if inm.Failure {
		return &milvuspb.StringResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
			},
		}, errors.New("IndexNode GetStatisticsChannel failed")
	}
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		Value: "",
	}, nil
}

func (inm *Mock) GetTimeTickChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	if inm.Failure {
		return &milvuspb.StringResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
			},
		}, errors.New("IndexNode GetTimeTickChannel failed")
	}
	return &milvuspb.StringResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		Value: "",
	}, nil
}

func (inm *Mock) CreateIndex(ctx context.Context, req *indexpb.CreateIndexRequest) (*commonpb.Status, error) {
	if inm.Build {
		inm.buildIndex <- req
	}

	if inm.Failure {
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
		}, errors.New("IndexNode CreateIndex failed")
	}

	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}, nil
}

func (inm *Mock) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	if inm.Failure {
		return &milvuspb.GetMetricsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    metricsinfo.MsgUnimplementedMetric,
			},
			Response: "",
		}, errors.New("IndexNode GetMetrics failed")
	}

	return &milvuspb.GetMetricsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Response:      "",
		ComponentName: "IndexNode",
	}, nil
}

//func getSystemInfoMetricsByIndexNodeMock(
//	ctx context.Context,
//	req *milvuspb.GetMetricsRequest,
//	in *IndexNodeMock,
//) (*milvuspb.GetMetricsResponse, error) {
//
//	id := UniqueID(16384)
//
//	nodeInfos := metricsinfo.IndexNodeInfos{
//		BaseComponentInfos: metricsinfo.BaseComponentInfos{
//			Name: metricsinfo.ConstructComponentName(typeutil.IndexNodeRole, id),
//		},
//	}
//	resp, err := metricsinfo.MarshalComponentInfos(nodeInfos)
//	if err != nil {
//		return &milvuspb.GetMetricsResponse{
//			Status: &commonpb.Status{
//				ErrorCode: commonpb.ErrorCode_UnexpectedError,
//				Reason:    err.Error(),
//			},
//			Response:      "",
//			ComponentName: metricsinfo.ConstructComponentName(typeutil.IndexNodeRole, id),
//		}, nil
//	}
//
//	return &milvuspb.GetMetricsResponse{
//		Status: &commonpb.Status{
//			ErrorCode: commonpb.ErrorCode_Success,
//			Reason:    "",
//		},
//		Response:      resp,
//		ComponentName: metricsinfo.ConstructComponentName(typeutil.IndexNodeRole, id),
//	}, nil
//}
