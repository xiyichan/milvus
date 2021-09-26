

## 8. Query Coordinator

#### 8.1 Overview

<img src="./figs/query_coord.png" width=500>



#### 8.2 Query Coordinator Interface

```go
type QueryCoord interface {
	Component
	TimeTickProvider

	ShowCollections(ctx context.Context, req *querypb.ShowCollectionsRequest) (*querypb.ShowCollectionsResponse, error)
	LoadCollection(ctx context.Context, req *querypb.LoadCollectionRequest) (*commonpb.Status, error)
	ReleaseCollection(ctx context.Context, req *querypb.ReleaseCollectionRequest) (*commonpb.Status, error)
	ShowPartitions(ctx context.Context, req *querypb.ShowPartitionsRequest) (*querypb.ShowPartitionsResponse, error)
	LoadPartitions(ctx context.Context, req *querypb.LoadPartitionsRequest) (*commonpb.Status, error)
	ReleasePartitions(ctx context.Context, req *querypb.ReleasePartitionsRequest) (*commonpb.Status, error)
	CreateQueryChannel(ctx context.Context) (*querypb.CreateQueryChannelResponse, error)
	GetPartitionStates(ctx context.Context, req *querypb.GetPartitionStatesRequest) (*querypb.GetPartitionStatesResponse, error)
	GetSegmentInfo(ctx context.Context, req *querypb.GetSegmentInfoRequest) (*querypb.GetSegmentInfoResponse, error)
	GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error)
}
```



* *MsgBase*

```go
type MsgBase struct {
	MsgType MsgType
	MsgID	UniqueID
	Timestamp Timestamp
	SourceID UniqueID
}
```

* *ShowCollections*

```go
type ShowCollectionRequest struct {
	Base          *commonpb.MsgBase
	DbID          UniqueID
	CollectionIDs []int64
}

type ShowCollectionResponse struct {
	Status              *commonpb.Status
	CollectionIDs       []UniqueID
	InMemoryPercentages []int64
}
```

* *LoadCollection*

```go
type LoadCollectionRequest struct {
	Base         *commonpb.MsgBase
	DbID         UniqueID
	CollectionID UniqueID
	schema       *schemapb.CollectionSchema
}
```

* *ReleaseCollection*

```go
type ReleaseCollectionRequest struct {
	Base         *commonpb.MsgBase
	DbID         UniqueID
	CollectionID UniqueID
}
```

* *ShowPartitions*

```go
type ShowPartitionRequest struct {
	Base         *commonpb.MsgBase
	DbID         UniqueID
	CollectionID UniqueID
	PartitionIDs []int64
}

type ShowPartitionResponse struct {
	Status              *commonpb.Status
	PartitionIDs        []UniqueID
	InMemoryPercentages []int64
}
```

* *GetPartitionStates*

```go
type PartitionState = int

const (
	PartitionState_NotExist        PartitionState = 0
	PartitionState_NotPresent      PartitionState = 1
	PartitionState_OnDisk          PartitionState = 2
	PartitionState_PartialInMemory PartitionState = 3
	PartitionState_InMemory        PartitionState = 4
	PartitionState_PartialInGPU    PartitionState = 5
	PartitionState_InGPU           PartitionState = 6
)

type PartitionStatesRequest struct {
	Base         *commonpb.MsgBase
	DbID         UniqueID
	CollectionID UniqueID
	PartitionIDs []UniqueID
}

type PartitionStates struct {
	PartitionID UniqueID
	State       PartitionState
}

type PartitionStatesResponse struct {
	Status                *commonpb.Status
	PartitionDescriptions []*PartitionStates
}
```

* *LoadPartitions*

```go
type LoadPartitonRequest struct {
	Base         *commonpb.MsgBase
	DbID         UniqueID
	CollectionID UniqueID
	PartitionIDs []UniqueID
	Schema       *schemapb.CollectionSchema
}
```

* *ReleasePartitions*

```go
type ReleasePartitionRequest struct {
	Base         *commonpb.MsgBase
	DbID         UniqueID
	CollectionID UniqueID
	PartitionIDs []UniqueID
}
```

* *CreateQueryChannel*

```go
type CreateQueryChannelResponse struct {
	Status             *commonpb.Status
	RequestChannelName string
	ResultChannelName  string
}
```

* *GetSegmentInfo* *

```go
type GetSegmentInfoRequest struct {
	Base       *commonpb.MsgBase
	SegmentIDs []UniqueID
}

type SegmentInfo struct {
	SegmentID    UniqueID
	CollectionID UniqueID
	PartitionID  UniqueID
	MemSize      UniqueID
	NumRows      UniqueID
	IndexName    string
	IndexID      UniqueID
}

type GetSegmentInfoResponse struct {
	Status *commonpb.Status
	Infos  []*SegmentInfo
}
```

#### 8.3 Query Channel

* *SearchMsg*

```go
type SearchRequest struct {
	Base               *commonpb.MsgBase
	ResultChannelID    string
	DbID               int64
	CollectionID       int64
	PartitionIDs       []int64
	Dsl                string
	PlaceholderGroup   []byte
	DslType            commonpb.DslType
	SerializedExprPlan []byte
	OutputFieldsId     []int64
	TravelTimestamp    uint64
	GuaranteeTimestamp uint64
}

type SearchMsg struct {
	BaseMsg
	SearchRequest
}
```

* *RetriveMsg*
```go
type RetriveRequest struct {
	Base               *commonpb.MsgBase
	ResultChannelID    string
	DbID               int64
	CollectionID       int64
	PartitionIDs       []int64
	SerializedExprPlan []byte
	OutputFieldsId     []int64
	TravelTimestamp    uint64
	GuaranteeTimestamp uint64
}

type RetriveMsg struct {
	BaseMsg
	RetrieveRequest
}
```

#### 8.4 Query Node Interface

```go
type QueryNode interface {
	Component
	TimeTickProvider

	AddQueryChannel(ctx context.Context, req *querypb.AddQueryChannelRequest) (*commonpb.Status, error)
	RemoveQueryChannel(ctx context.Context, req *querypb.RemoveQueryChannelRequest) (*commonpb.Status, error)
	WatchDmChannels(ctx context.Context, req *querypb.WatchDmChannelsRequest) (*commonpb.Status, error)
	LoadSegments(ctx context.Context, req *querypb.LoadSegmentsRequest) (*commonpb.Status, error)
	ReleaseCollection(ctx context.Context, req *querypb.ReleaseCollectionRequest) (*commonpb.Status, error)
	ReleasePartitions(ctx context.Context, req *querypb.ReleasePartitionsRequest) (*commonpb.Status, error)
	ReleaseSegments(ctx context.Context, req *querypb.ReleaseSegmentsRequest) (*commonpb.Status, error)
	GetSegmentInfo(ctx context.Context, req *querypb.GetSegmentInfoRequest) (*querypb.GetSegmentInfoResponse, error)
	GetMetrics(ctx context.Context, in *milvuspb.GetMetricsRequest, opts ...grpc.CallOption) (*milvuspb.GetMetricsResponse, error)
}
```



* *AddQueryChannel*

```go
type AddQueryChannelRequest struct {
	Base             *commonpb.MsgBase
	NodeID           int64
	CollectionID     int64
	RequestChannelID string
	ResultChannelID  string
}
```

* *RemoveQueryChannel*

```go
type RemoveQueryChannelRequest struct {
	Base             *commonpb.MsgBase
	NodeID           int64
	CollectionID     int64
	RequestChannelID string
	ResultChannelID  string
}
```

* *WatchDmChannels*

```go

type WatchDmChannelsRequest struct {
	Base         *commonpb.MsgBase
	NodeID       int64
	CollectionID int64
	PartitionID  int64
	Infos        []*datapb.VchannelInfo
	Schema       *schemapb.CollectionSchema
	ExcludeInfos []*datapb.SegmentInfo
}
```

* *LoadSegments*

```go
type LoadSegmentsRequest struct {
	Base          *commonpb.MsgBase
	NodeID        int64
	Infos         []*SegmentLoadInfo
	Schema        *schemapb.CollectionSchema
	LoadCondition TriggerCondition 
}
```
* *ReleaseCollection*

```go
type ReleaseCollectionRequest struct {
	Base         *commonpb.MsgBase
	DbID         UniqueID
	CollectionID UniqueID
	NodeID       int64
}
```

* *ReleasePartitions*

```go
type ReleasePartitionsRequest struct {
	Base         *commonpb.MsgBase
	DbID         UniqueID
	CollectionID UniqueID
	PartitionIDs []UniqueID
	NodeID       int64
}
```

* *ReleaseSegments*

```go
type ReleaseSegmentsRequest struct {
	Base         *commonpb.MsgBase
	NodeID       int64
	DbID         UniqueID
	CollectionID UniqueID
	PartitionIDs []UniqueID
	SegmentIDs   []UniqueID
}
```

* *GetSegmentInfo*

```go
type GetSegmentInfoRequest struct {
	Base       *commonpb.MsgBase
	SegmentIDs []Unique
}

type GetSegmentInfoResponse struct {
	Status *commonpb.Status
	Infos  []*SegmentInfo
}
```


//TODO
#### 8.2 Collection Replica

$collectionReplica$ contains a in-memory local copy of persistent collections. In common cases, the system has multiple query nodes. Data of a collection will be distributed across all the available query nodes, and each query node's $collectionReplica$ will maintain its own share (only part of the collection).
Every replica tracks a value called tSafe which is the maximum timestamp that the replica is up-to-date.

* *Collection*

``` go
type collectionReplica struct {
	tSafes map[UniqueID]tSafer // map[collectionID]tSafer

	mu          sync.RWMutex // guards all
	collections map[UniqueID]*Collection
	partitions  map[UniqueID]*Partition
	segments    map[UniqueID]*Segment

	excludedSegments map[UniqueID][]*datapb.SegmentInfo // map[collectionID]segmentIDs
}
```



* *Collection*

```go
type FieldSchema struct {
	FieldID      int64
	Name         string
	IsPrimaryKey bool
	Description  string
	DataType     DataType
	TypeParams   []*commonpb.KeyValuePair
	IndexParams  []*commonpb.KeyValuePair
}

type CollectionSchema struct {
	Name        string
	Description string
	AutoID      bool
	Fields      []*FieldSchema
}

type Collection struct {
	collectionPtr C.CCollection
	id            UniqueID
	partitionIDs  []UniqueID
	schema        *schemapb.CollectionSchema
	vChannels     []Channel
	pChannels     []Channel
	loadType      loadType

	releaseMu          sync.RWMutex
	releasedPartitions map[UniqueID]struct{}
	releaseTime        Timestamp
}
```

* *Partition*

```go
type Partition struct {
	collectionID UniqueID
	partitionID  UniqueID
	segmentIDs   []UniqueID
}
```



* *Segment*

``` go
type segmentType int32

const (
	segmentTypeInvalid segmentType = iota
	segmentTypeGrowing
	segmentTypeSealed
	segmentTypeIndexing
)
type indexParam = map[string]string

type Segment struct {
	segmentPtr C.CSegmentInterface

	segmentID    UniqueID
	partitionID  UniqueID
	collectionID UniqueID

	onService bool

	vChannelID   Channel
	lastMemSize  int64
	lastRowCount int64

	once             sync.Once // guards enableIndex
	enableIndex      bool

	rmMutex          sync.Mutex // guards recentlyModified
	recentlyModified bool

	typeMu      sync.Mutex // guards builtIndex
	segmentType segmentType

	paramMutex sync.RWMutex // guards index
	indexInfos map[FieldID]*indexInfo

	idBinlogRowSizes []int64

	vectorFieldMutex sync.RWMutex // guards vectorFieldInfos
	vectorFieldInfos map[UniqueID]*VectorFieldInfo

	pkFilter *bloom.BloomFilter //  bloom filter of pk inside a segmen
}
```



* *Data Sync Service*

```go
type dataSyncService struct {
	ctx    context.Context

	mu                   sync.Mutex                                   // guards FlowGraphs
	collectionFlowGraphs map[UniqueID]map[Channel]*queryNodeFlowGraph // map[collectionID]flowGraphs
	partitionFlowGraphs  map[UniqueID]map[Channel]*queryNodeFlowGraph // map[partitionID]flowGraphs

	streamingReplica ReplicaInterface
	tSafeReplica     TSafeReplicaInterface
	msFactory        msgstream.Factory
}
```



