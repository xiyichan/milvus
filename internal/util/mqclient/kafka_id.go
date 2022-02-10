package mqclient

import (
	"github.com/milvus-io/milvus/internal/common"
)

type kafkaID struct {
	messageID int64
}

var _ MessageID = &kafkaID{}

func (kid *kafkaID) Serialize() []byte {
	//log.Info("123456")
	//log.Info("SerializeKafkaID", zap.Any("kafkaID", kid.messageID))
	return SerializeKafkaID(kid.messageID)
}

func (kid *kafkaID) LedgerID() int64 {
	return 0
}

func (kid *kafkaID) EntryID() int64 {
	return 0
}

func (kid *kafkaID) BatchIdx() int32 {
	return 0
}

func (kid *kafkaID) PartitionIdx() int32 {
	return 0
}

func SerializeKafkaID(messageID int64) []byte {
	b := make([]byte, 8)
	//log.Info("SerializeKafkaID1", zap.Any("kafkaID", messageID))
	common.Endian.PutUint64(b, uint64(messageID))
	return b
}

func DeserializeKafkaID(messageID []byte) (int64, error) {
	return int64(common.Endian.Uint64(messageID)), nil
}
