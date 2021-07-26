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
	log.Info("send message topic ", zap.Any("topic", kp.topic))
	msg := &sarama.ProducerMessage{Topic: kp.topic, Value: sarama.ByteEncoder(message.Payload)}
	partition, offset, err := kp.p.SendMessage(msg)
	if err != nil {
		//log.Printf("FAILED to send message: %s\n", err)
		log.Error("FAILED to send message", zap.Error(err))
	} else {
		log.Debug("> message sent to ", zap.Any("partition", partition), zap.Any("offset", offset))
	}

	return nil
}

func (kp *kafkaProducer) Close() {
	kp.p.Close()
}
