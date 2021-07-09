package mqclient

import (
	"encoding/binary"
	"github.com/Shopify/sarama"
)

type kafkaID struct {
	messageID sarama.ConsumerMessage
}

func (kid *kafkaID) Serialize() []byte {
	return SerializeKafkaID(kid.messageID.Offset)
}

func SerializeKafkaID(messageID int64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(messageID))
	return b
}

func DeserializeKafkaID(messageID []byte) (int64, error) {
	return int64(binary.LittleEndian.Uint64(messageID)), nil
}
