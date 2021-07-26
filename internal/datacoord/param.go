// Copyright (C) 2019-2020 Zilliz. All rights reserved.//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.
package datacoord

import (
	"path"
	"strconv"
	"strings"
	"sync"

	"github.com/milvus-io/milvus/internal/log"

	"github.com/milvus-io/milvus/internal/util/paramtable"
)

type ParamTable struct {
	paramtable.BaseTable

	NodeID int64

	IP   string
	Port int

	// --- ETCD ---
	EtcdEndpoints           []string
	MetaRootPath            string
	KvRootPath              string
	SegmentBinlogSubPath    string
	CollectionBinlogSubPath string

	// --- Pulsar ---
	PulsarAddress string

	// --- Rocksmq ---
	RocksmqPath string

	FlushStreamPosSubPath string
	StatsStreamPosSubPath string

	// segment
	SegmentMaxSize          float64
	SegmentSealProportion   float64
	SegAssignmentExpiration int64

	InsertChannelPrefixName   string
	StatisticsChannelName     string
	TimeTickChannelName       string
	SegmentInfoChannelName    string
	DataCoordSubscriptionName string

	Log log.Config
}

var Params ParamTable
var once sync.Once

func (p *ParamTable) Init() {
	once.Do(func() {
		// load yaml
		p.BaseTable.Init()

		if err := p.LoadYaml("advanced/data_coord.yaml"); err != nil {
			panic(err)
		}

		// set members
		p.initEtcdEndpoints()
		p.initMetaRootPath()
		p.initKvRootPath()
		p.initSegmentBinlogSubPath()
		p.initCollectionBinlogSubPath()

		p.initPulsarAddress()
		p.initRocksmqPath()

		p.initSegmentMaxSize()
		p.initSegmentSealProportion()
		p.initSegAssignmentExpiration()
		p.initInsertChannelPrefixName()
		p.initStatisticsChannelName()
		p.initTimeTickChannelName()
		p.initSegmentInfoChannelName()
		p.initDataCoordSubscriptionName()
		p.initLogCfg()

		p.initFlushStreamPosSubPath()
		p.initStatsStreamPosSubPath()
	})
}

func (p *ParamTable) initEtcdEndpoints() {
	endpoints, err := p.Load("_EtcdEndpoints")
	if err != nil {
		panic(err)
	}
	p.EtcdEndpoints = strings.Split(endpoints, ",")
}

func (p *ParamTable) initPulsarAddress() {
	addr, err := p.Load("_PulsarAddress")
	if err != nil {
		panic(err)
	}
	p.PulsarAddress = addr
}

func (p *ParamTable) initRocksmqPath() {
	path, err := p.Load("_RocksmqPath")
	if err != nil {
		panic(err)
	}
	p.RocksmqPath = path
}

func (p *ParamTable) initMetaRootPath() {
	rootPath, err := p.Load("etcd.rootPath")
	if err != nil {
		panic(err)
	}
	subPath, err := p.Load("etcd.metaSubPath")
	if err != nil {
		panic(err)
	}
	p.MetaRootPath = rootPath + "/" + subPath
}

func (p *ParamTable) initKvRootPath() {
	rootPath, err := p.Load("etcd.rootPath")
	if err != nil {
		panic(err)
	}
	subPath, err := p.Load("etcd.kvSubPath")
	if err != nil {
		panic(err)
	}
	p.KvRootPath = rootPath + "/" + subPath
}

func (p *ParamTable) initSegmentBinlogSubPath() {
	subPath, err := p.Load("etcd.segmentBinlogSubPath")
	if err != nil {
		panic(err)
	}
	p.SegmentBinlogSubPath = subPath
}

func (p *ParamTable) initCollectionBinlogSubPath() {
	subPath, err := p.Load("etcd.collectionBinlogSubPath")
	if err != nil {
		panic(err)
	}
	p.CollectionBinlogSubPath = subPath
}

func (p *ParamTable) initSegmentMaxSize() {
	p.SegmentMaxSize = p.ParseFloat("datacoord.segment.maxSize")
}

func (p *ParamTable) initSegmentSealProportion() {
	p.SegmentSealProportion = p.ParseFloat("datacoord.segment.sealProportion")
}

func (p *ParamTable) initSegAssignmentExpiration() {
	p.SegAssignmentExpiration = p.ParseInt64("datacoord.segment.assignmentExpiration")
}

func (p *ParamTable) initInsertChannelPrefixName() {
	var err error
	p.InsertChannelPrefixName, err = p.Load("msgChannel.chanNamePrefix.dataCoordInsertChannel")
	if err != nil {
		panic(err)
	}
}

func (p *ParamTable) initStatisticsChannelName() {
	var err error
	p.StatisticsChannelName, err = p.Load("msgChannel.chanNamePrefix.dataCoordStatistic")
	if err != nil {
		panic(err)
	}
}

func (p *ParamTable) initTimeTickChannelName() {
	var err error
	p.TimeTickChannelName, err = p.Load("msgChannel.chanNamePrefix.dataCoordTimeTick")
	if err != nil {
		panic(err)
	}
}

func (p *ParamTable) initSegmentInfoChannelName() {
	var err error
	p.SegmentInfoChannelName, err = p.Load("msgChannel.chanNamePrefix.dataCoordSegmentInfo")
	if err != nil {
		panic(err)
	}
}

func (p *ParamTable) initDataCoordSubscriptionName() {
	var err error
	p.DataCoordSubscriptionName, err = p.Load("msgChannel.subNamePrefix.dataCoordSubNamePrefix")
	if err != nil {
		panic(err)
	}
}

func (p *ParamTable) initLogCfg() {
	p.Log = log.Config{}
	format, err := p.Load("log.format")
	if err != nil {
		panic(err)
	}
	p.Log.Format = format
	level, err := p.Load("log.level")
	if err != nil {
		panic(err)
	}
	p.Log.Level = level
	p.Log.File.MaxSize = p.ParseInt("log.file.maxSize")
	p.Log.File.MaxBackups = p.ParseInt("log.file.maxBackups")
	p.Log.File.MaxDays = p.ParseInt("log.file.maxAge")
	rootPath, err := p.Load("log.file.rootPath")
	if err != nil {
		panic(err)
	}
	if len(rootPath) != 0 {
		p.Log.File.Filename = path.Join(rootPath, "datacoord-"+strconv.FormatInt(p.NodeID, 10)+".log")
	} else {
		p.Log.File.Filename = ""
	}
}

func (p *ParamTable) initFlushStreamPosSubPath() {
	subPath, err := p.Load("etcd.flushStreamPosSubPath")
	if err != nil {
		panic(err)
	}
	p.FlushStreamPosSubPath = subPath
}

func (p *ParamTable) initStatsStreamPosSubPath() {
	subPath, err := p.Load("etcd.statsStreamPosSubPath")
	if err != nil {
		panic(err)
	}
	p.StatsStreamPosSubPath = subPath
}
