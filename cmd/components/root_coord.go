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

package components

import (
	"context"
	"io"

	"github.com/milvus-io/milvus/internal/proto/internalpb"

	rc "github.com/milvus-io/milvus/internal/distributed/rootcoord"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/opentracing/opentracing-go"
)

type RootCoord struct {
	ctx context.Context
	svr *rc.Server

	tracer opentracing.Tracer
	closer io.Closer
}

// NewRootCoord creates a new RoorCoord
func NewRootCoord(ctx context.Context, factory msgstream.Factory) (*RootCoord, error) {
	svr, err := rc.NewServer(ctx, factory)
	if err != nil {
		return nil, err
	}
	return &RootCoord{
		ctx: ctx,
		svr: svr,
	}, nil
}

// Run starts service
func (rc *RootCoord) Run() error {
	if err := rc.svr.Run(); err != nil {
		return err
	}
	return nil
}

// Stop terminates service
func (rc *RootCoord) Stop() error {
	if err := rc.svr.Stop(); err != nil {
		return err
	}
	return nil
}

func (rc *RootCoord) GetComponentStates(ctx context.Context, request *internalpb.GetComponentStatesRequest) (*internalpb.ComponentStates, error) {
	return rc.svr.GetComponentStates(ctx, request)
}
