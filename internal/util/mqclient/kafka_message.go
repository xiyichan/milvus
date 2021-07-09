package mqclient

import (
	"github.com/Shopify/sarama"
)

type kafkaMessage struct {
	msg sarama.ConsumerMessage
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

func (km *kafkaMessage) ID() MessageID {
	kid := &kafkaID{messageID: km.msg}
	return kid
}
