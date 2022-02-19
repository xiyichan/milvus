package kafka

import (
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/msgstream/mqclient"
)

type kafkaID struct {
	messageID int64
}

var _ mqclient.MessageID = &kafkaID{}

func (kid *kafkaID) Serialize() []byte {
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
	common.Endian.PutUint64(b, uint64(messageID))
	return b
}

func DeserializeKafkaID(messageID []byte) (int64, error) {
	return int64(common.Endian.Uint64(messageID)), nil
}
