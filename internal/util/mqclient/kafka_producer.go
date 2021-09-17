package mqclient

import (
	"context"
	"github.com/Shopify/sarama"
	"github.com/milvus-io/milvus/internal/log"
	"go.uber.org/zap"
)

type kafkaProducer struct {
	p     sarama.SyncProducer
	c     sarama.Client
	topic string
}

func (kp *kafkaProducer) Topic() string {
	return kp.topic
}
func (kp *kafkaProducer) Send(ctx context.Context, message *ProducerMessage) error {
	msg := &sarama.ProducerMessage{Topic: kp.topic, Value: sarama.ByteEncoder(message.Payload), Partition: 0}
	_, _, err := kp.p.SendMessage(msg)
	if err != nil {
		log.Error("FAILED to send message", zap.Error(err))
	}

	return nil
}

func (kp *kafkaProducer) Close() {
	kp.p.Close()
}
