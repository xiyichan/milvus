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
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"path"
	"strconv"
	"sync"
	"unsafe"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/kv"
	miniokv "github.com/milvus-io/milvus/internal/kv/minio"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
	"github.com/milvus-io/milvus/internal/util/trace"
	"github.com/opentracing/opentracing-go"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
)

const (
	CollectionPrefix = "/collection/"
	SegmentPrefix    = "/segment/"
)

type (
	InsertData = storage.InsertData
	Blob       = storage.Blob
)
type insertBufferNode struct {
	BaseNode
	channelName  string
	insertBuffer *insertBuffer
	replica      Replica
	idAllocator  allocatorInterface
	flushMap     sync.Map
	flushChan    <-chan *flushMsg

	minIOKV kv.BaseKV

	timeTickStream          msgstream.MsgStream
	segmentStatisticsStream msgstream.MsgStream

	dsSaveBinlog          func(fu *segmentFlushUnit) error
	segmentCheckPoints    map[UniqueID]segmentCheckPoint
	segmentCheckPointLock sync.Mutex
}

type segmentCheckPoint struct {
	numRows int64
	pos     internalpb.MsgPosition
}

type segmentFlushUnit struct {
	collID         UniqueID
	segID          UniqueID
	field2Path     map[UniqueID]string
	checkPoint     map[UniqueID]segmentCheckPoint
	startPositions []*datapb.SegmentStartPosition
	flushed        bool
}

type insertBuffer struct {
	insertData map[UniqueID]*InsertData // SegmentID to InsertData
	maxSize    int64
}

func (ib *insertBuffer) size(segmentID UniqueID) int64 {
	if ib.insertData == nil || len(ib.insertData) <= 0 {
		return 0
	}
	idata, ok := ib.insertData[segmentID]
	if !ok {
		return 0
	}

	var maxSize int64 = 0
	for _, data := range idata.Data {
		fdata, ok := data.(*storage.FloatVectorFieldData)
		if ok {
			totalNumRows := int64(0)
			if fdata.NumRows != nil {
				for _, numRow := range fdata.NumRows {
					totalNumRows += numRow
				}
			}
			if totalNumRows > maxSize {
				maxSize = totalNumRows
			}
		}

		bdata, ok := data.(*storage.BinaryVectorFieldData)
		if ok {
			totalNumRows := int64(0)
			if bdata.NumRows != nil {
				for _, numRow := range bdata.NumRows {
					totalNumRows += numRow
				}
			}
			if totalNumRows > maxSize {
				maxSize = totalNumRows
			}
		}

	}
	return maxSize
}

func (ib *insertBuffer) full(segmentID UniqueID) bool {
	log.Debug("Segment size", zap.Any("segment", segmentID), zap.Int64("size", ib.size(segmentID)), zap.Int64("maxsize", ib.maxSize))
	return ib.size(segmentID) >= ib.maxSize
}

func (ibNode *insertBufferNode) Name() string {
	return "ibNode"
}

func (ibNode *insertBufferNode) Operate(in []flowgraph.Msg) []flowgraph.Msg {

	// log.Debug("InsertBufferNode Operating")

	if len(in) != 1 {
		log.Error("Invalid operate message input in insertBufferNode", zap.Int("input length", len(in)))
		// TODO: add error handling
	}

	iMsg, ok := in[0].(*insertMsg)
	if !ok {
		log.Error("type assertion failed for insertMsg")
		// TODO: add error handling
	}

	if iMsg == nil {
		ibNode.timeTickStream.Close()
		ibNode.segmentStatisticsStream.Close()
		return []Msg{}
	}

	var spans []opentracing.Span
	for _, msg := range iMsg.insertMessages {
		sp, ctx := trace.StartSpanFromContext(msg.TraceCtx())
		spans = append(spans, sp)
		msg.SetTraceCtx(ctx)
	}

	// replace pchannel with vchannel
	for _, pos := range iMsg.startPositions {
		pos.ChannelName = ibNode.channelName
	}
	for _, pos := range iMsg.endPositions {
		pos.ChannelName = ibNode.channelName
	}

	// Updating segment statistics
	uniqueSeg := make(map[UniqueID]int64)
	for _, msg := range iMsg.insertMessages {

		currentSegID := msg.GetSegmentID()
		collID := msg.GetCollectionID()
		partitionID := msg.GetPartitionID()

		if !ibNode.replica.hasSegment(currentSegID, true) {
			err := ibNode.replica.addNewSegment(currentSegID, collID, partitionID, msg.GetChannelID(),
				iMsg.startPositions[0], iMsg.endPositions[0])
			if err != nil {
				log.Error("add segment wrong",
					zap.Int64("segID", currentSegID),
					zap.Int64("collID", collID),
					zap.Int64("partID", partitionID),
					zap.String("chanName", msg.GetChannelID()),
					zap.Error(err))
			}
		}

		segNum := uniqueSeg[currentSegID]
		uniqueSeg[currentSegID] = segNum + int64(len(msg.RowIDs))
	}

	segToUpdate := make([]UniqueID, 0, len(uniqueSeg))
	for id, num := range uniqueSeg {
		segToUpdate = append(segToUpdate, id)

		err := ibNode.replica.updateStatistics(id, num)
		if err != nil {
			log.Error("update Segment Row number wrong", zap.Int64("segID", id), zap.Error(err))
		}
	}

	if len(segToUpdate) > 0 {
		err := ibNode.updateSegStatistics(segToUpdate)
		if err != nil {
			log.Error("update segment statistics error", zap.Error(err))
		}
	}

	// iMsg is insertMsg
	// 1. iMsg -> buffer
	for _, msg := range iMsg.insertMessages {
		if len(msg.RowIDs) != len(msg.Timestamps) || len(msg.RowIDs) != len(msg.RowData) {
			log.Error("misaligned messages detected")
			continue
		}
		currentSegID := msg.GetSegmentID()
		collectionID := msg.GetCollectionID()

		idata, ok := ibNode.insertBuffer.insertData[currentSegID]
		if !ok {
			idata = &InsertData{
				Data: make(map[UniqueID]storage.FieldData),
			}
		}

		// 1.1 Get Collection Schema
		collSchema, err := ibNode.replica.getCollectionSchema(collectionID, msg.EndTs())
		if err != nil {
			// GOOSE TODO add error handler
			log.Error("Get schema wrong:", zap.Error(err))
			continue
		}

		// 1.2 Get Fields
		var pos int = 0 // Record position of blob
		var fieldIDs []int64
		var fieldTypes []schemapb.DataType
		for _, field := range collSchema.Fields {
			fieldIDs = append(fieldIDs, field.FieldID)
			fieldTypes = append(fieldTypes, field.DataType)
		}

		for _, field := range collSchema.Fields {
			switch field.DataType {
			case schemapb.DataType_FloatVector:
				var dim int
				for _, t := range field.TypeParams {
					if t.Key == "dim" {
						dim, err = strconv.Atoi(t.Value)
						if err != nil {
							log.Error("strconv wrong on get dim", zap.Error(err))
						}
						break
					}
				}
				if dim <= 0 {
					log.Error("invalid dim")
					continue
					// TODO: add error handling
				}

				if _, ok := idata.Data[field.FieldID]; !ok {
					idata.Data[field.FieldID] = &storage.FloatVectorFieldData{
						NumRows: make([]int64, 0, 1),
						Data:    make([]float32, 0),
						Dim:     dim,
					}
				}

				fieldData := idata.Data[field.FieldID].(*storage.FloatVectorFieldData)

				var offset int
				for _, blob := range msg.RowData {
					offset = 0
					for j := 0; j < dim; j++ {
						var v float32
						buf := bytes.NewBuffer(blob.GetValue()[pos+offset:])
						if err := binary.Read(buf, binary.LittleEndian, &v); err != nil {
							log.Error("binary.read float32 wrong", zap.Error(err))
						}
						fieldData.Data = append(fieldData.Data, v)
						offset += int(unsafe.Sizeof(*(&v)))
					}
				}
				pos += offset
				fieldData.NumRows = append(fieldData.NumRows, int64(len(msg.RowData)))

			case schemapb.DataType_BinaryVector:
				var dim int
				for _, t := range field.TypeParams {
					if t.Key == "dim" {
						dim, err = strconv.Atoi(t.Value)
						if err != nil {
							log.Error("strconv wrong")
						}
						break
					}
				}
				if dim <= 0 {
					log.Error("invalid dim")
					// TODO: add error handling
				}

				if _, ok := idata.Data[field.FieldID]; !ok {
					idata.Data[field.FieldID] = &storage.BinaryVectorFieldData{
						NumRows: make([]int64, 0, 1),
						Data:    make([]byte, 0),
						Dim:     dim,
					}
				}
				fieldData := idata.Data[field.FieldID].(*storage.BinaryVectorFieldData)

				var offset int
				for _, blob := range msg.RowData {
					bv := blob.GetValue()[pos : pos+(dim/8)]
					fieldData.Data = append(fieldData.Data, bv...)
					offset = len(bv)
				}
				pos += offset
				fieldData.NumRows = append(fieldData.NumRows, int64(len(msg.RowData)))

			case schemapb.DataType_Bool:
				if _, ok := idata.Data[field.FieldID]; !ok {
					idata.Data[field.FieldID] = &storage.BoolFieldData{
						NumRows: make([]int64, 0, 1),
						Data:    make([]bool, 0),
					}
				}

				fieldData := idata.Data[field.FieldID].(*storage.BoolFieldData)
				var v bool
				for _, blob := range msg.RowData {
					buf := bytes.NewReader(blob.GetValue()[pos:])
					if err := binary.Read(buf, binary.LittleEndian, &v); err != nil {
						log.Error("binary.Read bool wrong", zap.Error(err))
					}
					fieldData.Data = append(fieldData.Data, v)

				}
				pos += int(unsafe.Sizeof(*(&v)))
				fieldData.NumRows = append(fieldData.NumRows, int64(len(msg.RowData)))

			case schemapb.DataType_Int8:
				if _, ok := idata.Data[field.FieldID]; !ok {
					idata.Data[field.FieldID] = &storage.Int8FieldData{
						NumRows: make([]int64, 0, 1),
						Data:    make([]int8, 0),
					}
				}

				fieldData := idata.Data[field.FieldID].(*storage.Int8FieldData)
				var v int8
				for _, blob := range msg.RowData {
					buf := bytes.NewReader(blob.GetValue()[pos:])
					if err := binary.Read(buf, binary.LittleEndian, &v); err != nil {
						log.Error("binary.Read int8 wrong", zap.Error(err))
					}
					fieldData.Data = append(fieldData.Data, v)
				}
				pos += int(unsafe.Sizeof(*(&v)))
				fieldData.NumRows = append(fieldData.NumRows, int64(len(msg.RowData)))

			case schemapb.DataType_Int16:
				if _, ok := idata.Data[field.FieldID]; !ok {
					idata.Data[field.FieldID] = &storage.Int16FieldData{
						NumRows: make([]int64, 0, 1),
						Data:    make([]int16, 0),
					}
				}

				fieldData := idata.Data[field.FieldID].(*storage.Int16FieldData)
				var v int16
				for _, blob := range msg.RowData {
					buf := bytes.NewReader(blob.GetValue()[pos:])
					if err := binary.Read(buf, binary.LittleEndian, &v); err != nil {
						log.Error("binary.Read int16 wrong", zap.Error(err))
					}
					fieldData.Data = append(fieldData.Data, v)
				}
				pos += int(unsafe.Sizeof(*(&v)))
				fieldData.NumRows = append(fieldData.NumRows, int64(len(msg.RowData)))

			case schemapb.DataType_Int32:
				if _, ok := idata.Data[field.FieldID]; !ok {
					idata.Data[field.FieldID] = &storage.Int32FieldData{
						NumRows: make([]int64, 0, 1),
						Data:    make([]int32, 0),
					}
				}

				fieldData := idata.Data[field.FieldID].(*storage.Int32FieldData)
				var v int32
				for _, blob := range msg.RowData {
					buf := bytes.NewReader(blob.GetValue()[pos:])
					if err := binary.Read(buf, binary.LittleEndian, &v); err != nil {
						log.Error("binary.Read int64 wrong", zap.Error(err))
					}
					fieldData.Data = append(fieldData.Data, v)
				}
				pos += int(unsafe.Sizeof(*(&v)))
				fieldData.NumRows = append(fieldData.NumRows, int64(len(msg.RowData)))

			case schemapb.DataType_Int64:
				if _, ok := idata.Data[field.FieldID]; !ok {
					idata.Data[field.FieldID] = &storage.Int64FieldData{
						NumRows: make([]int64, 0, 1),
						Data:    make([]int64, 0),
					}
				}

				fieldData := idata.Data[field.FieldID].(*storage.Int64FieldData)
				switch field.FieldID {
				case 0: // rowIDs
					fieldData.Data = append(fieldData.Data, msg.RowIDs...)
					fieldData.NumRows = append(fieldData.NumRows, int64(len(msg.RowData)))
				case 1: // Timestamps
					for _, ts := range msg.Timestamps {
						fieldData.Data = append(fieldData.Data, int64(ts))
					}
					fieldData.NumRows = append(fieldData.NumRows, int64(len(msg.RowData)))
				default:
					var v int64
					for _, blob := range msg.RowData {
						buf := bytes.NewBuffer(blob.GetValue()[pos:])
						if err := binary.Read(buf, binary.LittleEndian, &v); err != nil {
							log.Error("binary.Read int64 wrong", zap.Error(err))
						}
						fieldData.Data = append(fieldData.Data, v)
					}
					pos += int(unsafe.Sizeof(*(&v)))
					fieldData.NumRows = append(fieldData.NumRows, int64(len(msg.RowData)))
				}

			case schemapb.DataType_Float:
				if _, ok := idata.Data[field.FieldID]; !ok {
					idata.Data[field.FieldID] = &storage.FloatFieldData{
						NumRows: make([]int64, 0, 1),
						Data:    make([]float32, 0),
					}
				}

				fieldData := idata.Data[field.FieldID].(*storage.FloatFieldData)
				var v float32
				for _, blob := range msg.RowData {
					buf := bytes.NewBuffer(blob.GetValue()[pos:])
					if err := binary.Read(buf, binary.LittleEndian, &v); err != nil {
						log.Error("binary.Read float32 wrong", zap.Error(err))
					}
					fieldData.Data = append(fieldData.Data, v)
				}
				pos += int(unsafe.Sizeof(*(&v)))
				fieldData.NumRows = append(fieldData.NumRows, int64(len(msg.RowData)))

			case schemapb.DataType_Double:
				if _, ok := idata.Data[field.FieldID]; !ok {
					idata.Data[field.FieldID] = &storage.DoubleFieldData{
						NumRows: make([]int64, 0, 1),
						Data:    make([]float64, 0),
					}
				}

				fieldData := idata.Data[field.FieldID].(*storage.DoubleFieldData)
				var v float64
				for _, blob := range msg.RowData {
					buf := bytes.NewBuffer(blob.GetValue()[pos:])
					if err := binary.Read(buf, binary.LittleEndian, &v); err != nil {
						log.Error("binary.Read float64 wrong", zap.Error(err))
					}
					fieldData.Data = append(fieldData.Data, v)
				}

				pos += int(unsafe.Sizeof(*(&v)))
				fieldData.NumRows = append(fieldData.NumRows, int64(len(msg.RowData)))
			}
		}

		// 1.3 store in buffer
		ibNode.insertBuffer.insertData[currentSegID] = idata

		// store current endPositions as Segment->EndPostion
		ibNode.replica.updateSegmentEndPosition(currentSegID, iMsg.endPositions[0])
		// update segment pk filter
		ibNode.replica.updateSegmentPKRange(currentSegID, msg.GetRowIDs())
	}

	if len(iMsg.insertMessages) > 0 {
		log.Debug("---insert buffer status---")
		var stopSign int = 0
		for k := range ibNode.insertBuffer.insertData {
			if stopSign >= 10 {
				log.Debug("......")
				break
			}
			log.Debug("seg buffer status", zap.Int64("segmentID", k), zap.Int64("buffer size", ibNode.insertBuffer.size(k)))
			stopSign++
		}
	}

	finishCh := make(chan segmentFlushUnit, len(segToUpdate))
	finishCnt := sync.WaitGroup{}
	for _, segToFlush := range segToUpdate {
		// If full, auto flush
		if ibNode.insertBuffer.full(segToFlush) {
			log.Debug(". Insert Buffer full, auto flushing ",
				zap.Int64("num of rows", ibNode.insertBuffer.size(segToFlush)))

			collMeta, err := ibNode.getCollMetabySegID(segToFlush, iMsg.timeRange.timestampMax)
			if err != nil {
				log.Error("Auto flush failed .. cannot get collection meta ..", zap.Error(err))
				continue
			}

			ibNode.flushMap.Store(segToFlush, ibNode.insertBuffer.insertData[segToFlush])
			delete(ibNode.insertBuffer.insertData, segToFlush)

			collID, partitionID, err := ibNode.getCollectionandPartitionIDbySegID(segToFlush)
			if err != nil {
				log.Error("Auto flush failed .. cannot get collection ID or partition ID..", zap.Error(err))
				continue
			}
			finishCnt.Add(1)

			go flushSegment(collMeta, segToFlush, partitionID, collID,
				&ibNode.flushMap, ibNode.minIOKV, finishCh, &finishCnt, ibNode, ibNode.idAllocator)
		}
	}
	finishCnt.Wait()
	close(finishCh)
	for fu := range finishCh {
		if fu.field2Path == nil {
			log.Debug("segment is empty")
			continue
		}
		fu.checkPoint = ibNode.replica.listSegmentsCheckPoints()
		fu.flushed = false
		if err := ibNode.dsSaveBinlog(&fu); err != nil {
			log.Debug("data service save bin log path failed", zap.Error(err))
		}
	}

	// iMsg is Flush() msg from datacoord
	select {
	case fmsg := <-ibNode.flushChan:
		currentSegID := fmsg.segmentID
		log.Debug(". Receiving flush message",
			zap.Int64("segmentID", currentSegID),
			zap.Int64("collectionID", fmsg.collectionID),
		)

		if ibNode.insertBuffer.size(currentSegID) <= 0 {
			log.Debug(".. Buffer empty ...")
			ibNode.dsSaveBinlog(&segmentFlushUnit{
				collID:     fmsg.collectionID,
				segID:      currentSegID,
				field2Path: map[UniqueID]string{},
				checkPoint: ibNode.replica.listSegmentsCheckPoints(),
				flushed:    true,
			})
			ibNode.replica.segmentFlushed(currentSegID)
			fmsg.dmlFlushedCh <- []*datapb.FieldBinlog{{FieldID: currentSegID, Binlogs: []string{}}}
		} else { //insertBuffer(not empty) -> binLogs -> minIO/S3
			log.Debug(".. Buffer not empty, flushing ..")
			finishCh := make(chan segmentFlushUnit, 1)

			ibNode.flushMap.Store(currentSegID, ibNode.insertBuffer.insertData[currentSegID])
			delete(ibNode.insertBuffer.insertData, currentSegID)
			clearFn := func() {
				finishCh <- segmentFlushUnit{field2Path: nil}
				log.Debug(".. Clearing flush Buffer ..")
				ibNode.flushMap.Delete(currentSegID)
				close(finishCh)
				fmsg.dmlFlushedCh <- []*datapb.FieldBinlog{{FieldID: currentSegID, Binlogs: nil}}
			}

			collID, partitionID, err := ibNode.getCollectionandPartitionIDbySegID(currentSegID)
			if err != nil {
				log.Error("Flush failed .. cannot get segment ..", zap.Error(err))
				clearFn()
				break
				// TODO add error handling
			}

			collMeta, err := ibNode.getCollMetabySegID(currentSegID, iMsg.timeRange.timestampMax)
			if err != nil {
				log.Error("Flush failed .. cannot get collection schema ..", zap.Error(err))
				clearFn()
				break
				// TODO add error handling
			}

			flushSegment(collMeta, currentSegID, partitionID, collID,
				&ibNode.flushMap, ibNode.minIOKV, finishCh, nil, ibNode, ibNode.idAllocator)
			fu := <-finishCh
			close(finishCh)
			if fu.field2Path != nil {
				fu.checkPoint = ibNode.replica.listSegmentsCheckPoints()
				fu.flushed = true
				if err := ibNode.dsSaveBinlog(&fu); err != nil {
					log.Debug("Data service save binlog path failed", zap.Error(err))
				} else {
					ibNode.replica.segmentFlushed(fu.segID)
				}
			}
			fmsg.dmlFlushedCh <- []*datapb.FieldBinlog{{FieldID: currentSegID, Binlogs: []string{}}}
		}

	default:
	}

	// TODO write timetick
	if err := ibNode.writeHardTimeTick(iMsg.timeRange.timestampMax); err != nil {
		log.Error("send hard time tick into pulsar channel failed", zap.Error(err))
	}

	for _, sp := range spans {
		sp.Finish()
	}

	return nil
}

func flushSegment(
	collMeta *etcdpb.CollectionMeta,
	segID, partitionID, collID UniqueID,
	insertData *sync.Map,
	kv kv.BaseKV,
	flushUnit chan<- segmentFlushUnit,
	wgFinish *sync.WaitGroup,
	ibNode *insertBufferNode,
	idAllocator allocatorInterface) {

	if wgFinish != nil {
		defer wgFinish.Done()
	}

	clearFn := func(isSuccess bool) {
		if !isSuccess {
			flushUnit <- segmentFlushUnit{field2Path: nil}
		}

		log.Debug(".. Clearing flush Buffer ..")
		insertData.Delete(segID)
	}

	inCodec := storage.NewInsertCodec(collMeta)

	// buffer data to binlogs
	data, ok := insertData.Load(segID)
	if !ok {
		log.Error("Flush failed ... cannot load insertData ..")
		clearFn(false)
		return
	}

	binLogs, statsBinlogs, err := inCodec.Serialize(partitionID, segID, data.(*InsertData))
	if err != nil {
		log.Error("Flush failed ... cannot generate binlog ..", zap.Error(err))
		clearFn(false)
		return
	}

	log.Debug(".. Saving binlogs to MinIO ..", zap.Int("number", len(binLogs)))
	field2Path := make(map[UniqueID]string, len(binLogs))
	kvs := make(map[string]string, len(binLogs))
	paths := make([]string, 0, len(binLogs))
	field2Logidx := make(map[UniqueID]UniqueID, len(binLogs))

	// write insert binlog
	for _, blob := range binLogs {
		fieldID, err := strconv.ParseInt(blob.GetKey(), 10, 64)
		if err != nil {
			log.Error("Flush failed ... cannot parse string to fieldID ..", zap.Error(err))
			clearFn(false)
			return
		}
		log.Debug("save binlog", zap.Int64("fieldID", fieldID))

		logidx, err := idAllocator.allocID()
		if err != nil {
			log.Error("Flush failed ... cannot alloc ID ..", zap.Error(err))
			clearFn(false)
			return
		}

		// no error raise if alloc=false
		k, _ := idAllocator.genKey(false, collID, partitionID, segID, fieldID, logidx)

		key := path.Join(Params.InsertBinlogRootPath, k)
		paths = append(paths, key)
		kvs[key] = string(blob.Value[:])
		field2Path[fieldID] = key
		field2Logidx[fieldID] = logidx
	}

	// write stats binlog
	for _, blob := range statsBinlogs {
		fieldID, err := strconv.ParseInt(blob.GetKey(), 10, 64)
		if err != nil {
			log.Error("Flush failed ... cannot parse string to fieldID ..", zap.Error(err))
			clearFn(false)
			return
		}

		logidx := field2Logidx[fieldID]

		// no error raise if alloc=false
		k, _ := idAllocator.genKey(false, collID, partitionID, segID, fieldID, logidx)

		key := path.Join(Params.StatsBinlogRootPath, k)
		kvs[key] = string(blob.Value[:])
	}
	log.Debug("save binlog file to MinIO/S3")

	err = kv.MultiSave(kvs)
	if err != nil {
		log.Error("Flush failed ... cannot save to MinIO ..", zap.Error(err))
		_ = kv.MultiRemove(paths)
		clearFn(false)
		return
	}

	ibNode.replica.updateSegmentCheckPoint(segID)
	startPos := ibNode.replica.listNewSegmentsStartPositions()
	flushUnit <- segmentFlushUnit{collID: collID, segID: segID, field2Path: field2Path, startPositions: startPos}
	clearFn(true)
}

func (ibNode *insertBufferNode) writeHardTimeTick(ts Timestamp) error {
	msgPack := msgstream.MsgPack{}
	timeTickMsg := msgstream.DataNodeTtMsg{
		BaseMsg: msgstream.BaseMsg{
			BeginTimestamp: ts,
			EndTimestamp:   ts,
			HashValues:     []uint32{0},
		},
		DataNodeTtMsg: datapb.DataNodeTtMsg{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DataNodeTt,
				MsgID:     0,
				Timestamp: ts,
			},
			ChannelName: ibNode.channelName,
			Timestamp:   ts,
		},
	}
	msgPack.Msgs = append(msgPack.Msgs, &timeTickMsg)
	return ibNode.timeTickStream.Produce(&msgPack)
}

func (ibNode *insertBufferNode) updateSegStatistics(segIDs []UniqueID) error {
	log.Debug("Updating segments statistics...")
	statsUpdates := make([]*internalpb.SegmentStatisticsUpdates, 0, len(segIDs))
	for _, segID := range segIDs {
		updates, err := ibNode.replica.getSegmentStatisticsUpdates(segID)
		if err != nil {
			log.Error("get segment statistics updates wrong", zap.Int64("segmentID", segID), zap.Error(err))
			continue
		}

		log.Debug("Segment Statistics to Update",
			zap.Int64("Segment ID", updates.GetSegmentID()),
			zap.Int64("NumOfRows", updates.GetNumRows()),
		)

		statsUpdates = append(statsUpdates, updates)
	}

	segStats := internalpb.SegmentStatistics{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_SegmentStatistics,
			MsgID:     UniqueID(0),  // GOOSE TODO
			Timestamp: Timestamp(0), // GOOSE TODO
			SourceID:  Params.NodeID,
		},
		SegStats: statsUpdates,
	}

	var msg msgstream.TsMsg = &msgstream.SegmentStatisticsMsg{
		BaseMsg: msgstream.BaseMsg{
			HashValues: []uint32{0}, // GOOSE TODO
		},
		SegmentStatistics: segStats,
	}

	var msgPack = msgstream.MsgPack{
		Msgs: []msgstream.TsMsg{msg},
	}
	return ibNode.segmentStatisticsStream.Produce(&msgPack)
}

func (ibNode *insertBufferNode) getCollMetabySegID(segmentID UniqueID, ts Timestamp) (meta *etcdpb.CollectionMeta, err error) {
	if !ibNode.replica.hasSegment(segmentID, true) {
		return nil, fmt.Errorf("No such segment %d in the replica", segmentID)
	}

	collID := ibNode.replica.getCollectionID()
	sch, err := ibNode.replica.getCollectionSchema(collID, ts)
	if err != nil {
		return nil, err
	}

	meta = &etcdpb.CollectionMeta{
		ID:     collID,
		Schema: sch,
	}
	return
}

func (ibNode *insertBufferNode) getCollectionandPartitionIDbySegID(segmentID UniqueID) (collID, partitionID UniqueID, err error) {
	return ibNode.replica.getCollectionAndPartitionID(segmentID)
}

func newInsertBufferNode(
	ctx context.Context,
	replica Replica,
	factory msgstream.Factory,
	idAllocator allocatorInterface,
	flushCh <-chan *flushMsg,
	saveBinlog func(*segmentFlushUnit) error,
	channelName string,
) (*insertBufferNode, error) {

	maxQueueLength := Params.FlowGraphMaxQueueLength
	maxParallelism := Params.FlowGraphMaxParallelism

	baseNode := BaseNode{}
	baseNode.SetMaxQueueLength(maxQueueLength)
	baseNode.SetMaxParallelism(maxParallelism)

	maxSize := Params.FlushInsertBufferSize
	iBuffer := &insertBuffer{
		insertData: make(map[UniqueID]*InsertData),
		maxSize:    maxSize,
	}

	// MinIO
	option := &miniokv.Option{
		Address:           Params.MinioAddress,
		AccessKeyID:       Params.MinioAccessKeyID,
		SecretAccessKeyID: Params.MinioSecretAccessKey,
		UseSSL:            Params.MinioUseSSL,
		CreateBucket:      true,
		BucketName:        Params.MinioBucketName,
	}

	minIOKV, err := miniokv.NewMinIOKV(ctx, option)
	if err != nil {
		return nil, err
	}

	//input stream, data node time tick
	wTt, err := factory.NewMsgStream(ctx)
	if err != nil {
		return nil, err
	}
	wTt.AsProducer([]string{Params.TimeTickChannelName})
	log.Debug("datanode AsProducer", zap.String("TimeTickChannelName", Params.TimeTickChannelName))
	var wTtMsgStream msgstream.MsgStream = wTt
	wTtMsgStream.Start()

	// update statistics channel
	segS, err := factory.NewMsgStream(ctx)
	if err != nil {
		return nil, err
	}
	segS.AsProducer([]string{Params.SegmentStatisticsChannelName})
	log.Debug("datanode AsProducer", zap.String("SegmentStatisChannelName", Params.SegmentStatisticsChannelName))
	var segStatisticsMsgStream msgstream.MsgStream = segS
	segStatisticsMsgStream.Start()

	return &insertBufferNode{
		BaseNode:     baseNode,
		insertBuffer: iBuffer,
		minIOKV:      minIOKV,
		channelName:  channelName,

		timeTickStream:          wTtMsgStream,
		segmentStatisticsStream: segStatisticsMsgStream,

		replica:            replica,
		flushMap:           sync.Map{},
		flushChan:          flushCh,
		idAllocator:        idAllocator,
		dsSaveBinlog:       saveBinlog,
		segmentCheckPoints: make(map[UniqueID]segmentCheckPoint),
	}, nil
}
