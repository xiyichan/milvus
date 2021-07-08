package mqclient

import "github.com/milvus-io/milvus/internal/util/kafka/client/kafka"

type kafkaMessage struct {
	msg kafka.ConsumerMessage
}

func (km *kafkaMessage) Topic() string {
	return km.msg.Topic
}

func (km *kafkaMessage) Properties() map[string]string {
	return nil
}

func (km *kafkaMessage) Payload() []byte {
	return km.msg.Payload
}

func (km *kafkaMessage) ID() MessageID {
	return &rmqID{messageID: km.msg.MsgID}
}
