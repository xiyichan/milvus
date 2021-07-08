package mqclient

import (
	"encoding/binary"
	"github.com/milvus-io/milvus/internal/util/kafka/server/kafka"
)

type kafkaID struct {
	messageID kafka.Offset
}

func (kid *kafkaID) Serialize() []byte {
	return SerializeKafkaID(kid.messageID)
}

func SerializeKafkaID(messageID int64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(messageID))
	return b
}

func DeserializeKafkaID(messageID []byte) (int64, error) {
	return int64(binary.LittleEndian.Uint64(messageID)), nil
}
