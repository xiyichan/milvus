package mqclient

import (
	"encoding/binary"

	"github.com/Shopify/sarama"
)

type kafkaID struct {
	messageID *sarama.ConsumerMessage
}

func (kid *kafkaID) Serialize() []byte {
	return SerializeKafkaID(kid.messageID.Offset)
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
	return kid.messageID.Partition
}

func SerializeKafkaID(messageID int64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(messageID))
	return b
}

func DeserializeKafkaID(messageID []byte) (int64, error) {
	return int64(binary.LittleEndian.Uint64(messageID)), nil
}
