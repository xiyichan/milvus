package mqclient

import (
	"context"
	"github.com/Shopify/sarama"
	"github.com/milvus-io/milvus/internal/log"
	"go.uber.org/zap"
)

type kafkaProducer struct {
	p sarama.SyncProducer
	c sarama.Client
}

func (kp *kafkaProducer) Topic() string {
	return ""
}
func (kp *kafkaProducer) Send(ctx context.Context, message *ProducerMessage) error {
	msg := &sarama.ProducerMessage{Topic: message.Topic, Value: sarama.ByteEncoder(message.Payload)}
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
