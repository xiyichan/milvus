package kafka

import (
	sarama "github.com/Shopify/sarama"
	"github.com/milvus-io/milvus/internal/msgstream/mqclient"
)

type kafkaMessage struct {
	msg *sarama.ConsumerMessage
}

func (km *kafkaMessage) Topic() string {
	return km.msg.Topic
}

func (km *kafkaMessage) Properties() map[string]string {
	return nil
}

func (km *kafkaMessage) Payload() []byte {
	return km.msg.Value
}

func (km *kafkaMessage) ID() mqclient.MessageID {
	kid := &kafkaID{messageID: km.msg.Offset}
	return kid
}
