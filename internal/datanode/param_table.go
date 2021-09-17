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

package datanode

import (
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/paramtable"
)

type ParamTable struct {
	paramtable.BaseTable

	// === DataNode Internal Components Configs ===
	NodeID                  UniqueID
	IP                      string
	Port                    int
	FlowGraphMaxQueueLength int32
	FlowGraphMaxParallelism int32
	FlushInsertBufferSize   int64
	InsertBinlogRootPath    string
	StatsBinlogRootPath     string
	Log                     log.Config
	Alias                   string // Different datanode in one machine

	// === DataNode External Components Configs ===
	// --- Pulsar ---
	PulsarAddress string

	// --- Kafka ---
	KafkaAddress string

	// --- Rocksmq ---
	RocksmqPath string

	// - seg statistics channel -
	SegmentStatisticsChannelName string

	// - timetick channel -
	TimeTickChannelName string

	// - channel subname -
	MsgChannelSubName string

	// --- ETCD ---
	EtcdEndpoints []string
	MetaRootPath  string

	// --- MinIO ---
	MinioAddress         string
	MinioAccessKeyID     string
	MinioSecretAccessKey string
	MinioUseSSL          bool
	MinioBucketName      string

	CreatedTime time.Time
	UpdatedTime time.Time
}

var Params ParamTable
var once sync.Once

func (p *ParamTable) InitAlias(alias string) {
	p.Alias = alias
}

func (p *ParamTable) Init() {
	once.Do(func() {
		p.BaseTable.Init()
		err := p.LoadYaml("advanced/data_node.yaml")
		if err != nil {
			panic(err)
		}

		// === DataNode Internal Components Configs ===
		p.initFlowGraphMaxQueueLength()
		p.initFlowGraphMaxParallelism()
		p.initFlushInsertBufferSize()
		p.initInsertBinlogRootPath()
		p.initStatsBinlogRootPath()
		p.initLogCfg()

		// === DataNode External Components Configs ===
		// --- Pulsar ---
		p.initPulsarAddress()

		p.initRocksmqPath()

		// - seg statistics channel -
		p.initSegmentStatisticsChannelName()

		// - timetick channel -
		p.initTimeTickChannelName()

		// --- ETCD ---
		p.initEtcdEndpoints()
		p.initMetaRootPath()

		// --- MinIO ---
		p.initMinioAddress()
		p.initMinioAccessKeyID()
		p.initMinioSecretAccessKey()
		p.initMinioUseSSL()
		p.initMinioBucketName()
	})
}

// ==== DataNode internal components configs ====
// ---- flowgraph configs ----
func (p *ParamTable) initFlowGraphMaxQueueLength() {
	p.FlowGraphMaxQueueLength = p.ParseInt32("dataNode.dataSync.flowGraph.maxQueueLength")
}

func (p *ParamTable) initFlowGraphMaxParallelism() {
	p.FlowGraphMaxParallelism = p.ParseInt32("dataNode.dataSync.flowGraph.maxParallelism")
}

// ---- flush configs ----
func (p *ParamTable) initFlushInsertBufferSize() {
	p.FlushInsertBufferSize = p.ParseInt64("datanode.flush.insertBufSize")
}

func (p *ParamTable) initInsertBinlogRootPath() {
	// GOOSE TODO: rootPath change to  TenentID
	rootPath, err := p.Load("etcd.rootPath")
	if err != nil {
		panic(err)
	}
	p.InsertBinlogRootPath = path.Join(rootPath, "insert_log")
}

func (p *ParamTable) initStatsBinlogRootPath() {
	rootPath, err := p.Load("etcd.rootPath")
	if err != nil {
		panic(err)
	}
	p.StatsBinlogRootPath = path.Join(rootPath, "stats_log")
}

// ---- Pulsar ----
func (p *ParamTable) initPulsarAddress() {
	url, err := p.Load("_PulsarAddress")
	if err != nil {
		panic(err)
	}
	p.PulsarAddress = url
}

func (p *ParamTable) initKafkaAddress() {
	url, err := p.Load("_KafkaAddress")
	if err != nil {
		panic(err)
	}
	p.KafkaAddress = url
}

func (p *ParamTable) initRocksmqPath() {
	path, err := p.Load("_RocksmqPath")
	if err != nil {
		panic(err)
	}
	p.RocksmqPath = path
}

func (p *ParamTable) initSegmentStatisticsChannelName() {

	path, err := p.Load("msgChannel.chanNamePrefix.dataCoordStatistic")
	if err != nil {
		panic(err)
	}
	p.SegmentStatisticsChannelName = path
}

func (p *ParamTable) initTimeTickChannelName() {
	path, err := p.Load("msgChannel.chanNamePrefix.dataCoordTimeTick")
	if err != nil {
		panic(err)
	}
	p.TimeTickChannelName = path
}

// - msg channel subname -
func (p *ParamTable) initMsgChannelSubName() {
	name, err := p.Load("msgChannel.subNamePrefix.dataNodeSubNamePrefix")
	if err != nil {
		panic(err)
	}
	p.MsgChannelSubName = name + "-" + strconv.FormatInt(p.NodeID, 10)
}

// --- ETCD ---
func (p *ParamTable) initEtcdEndpoints() {
	endpoints, err := p.Load("_EtcdEndpoints")
	if err != nil {
		panic(err)
	}
	p.EtcdEndpoints = strings.Split(endpoints, ",")
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
	p.MetaRootPath = path.Join(rootPath, subPath)
}

// --- MinIO ---
func (p *ParamTable) initMinioAddress() {
	endpoint, err := p.Load("_MinioAddress")
	if err != nil {
		panic(err)
	}
	p.MinioAddress = endpoint
}

func (p *ParamTable) initMinioAccessKeyID() {
	keyID, err := p.Load("minio.accessKeyID")
	if err != nil {
		panic(err)
	}
	p.MinioAccessKeyID = keyID
}

func (p *ParamTable) initMinioSecretAccessKey() {
	key, err := p.Load("minio.secretAccessKey")
	if err != nil {
		panic(err)
	}
	p.MinioSecretAccessKey = key
}

func (p *ParamTable) initMinioUseSSL() {
	usessl, err := p.Load("minio.useSSL")
	if err != nil {
		panic(err)
	}
	p.MinioUseSSL, _ = strconv.ParseBool(usessl)
}

func (p *ParamTable) initMinioBucketName() {
	bucketName, err := p.Load("minio.bucketName")
	if err != nil {
		panic(err)
	}
	p.MinioBucketName = bucketName
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
		p.Log.File.Filename = path.Join(rootPath, "datanode"+p.Alias+".log")
	} else {
		p.Log.File.Filename = ""
	}
}
