package kafka

import (
	"context"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream/mqclient"
	"go.uber.org/zap"

	"github.com/Shopify/sarama"
)

type kafkaProducer struct {
	p     sarama.SyncProducer
	topic string
}

func (kp *kafkaProducer) Topic() string {
	return kp.topic
}
func (kp *kafkaProducer) Send(ctx context.Context, message *mqclient.ProducerMessage) (mqclient.MessageID, error) {
	msg := &sarama.ProducerMessage{Topic: kp.topic, Value: sarama.ByteEncoder(message.Payload), Partition: 0}
	partition, offset, err := kp.p.SendMessage(msg)
	if err != nil {
		log.Error("FAILED to send message", zap.Error(err))
	} else {
		log.Debug("> message sent to ", zap.Any("message length", len(message.Payload)), zap.Any("topic", kp.topic), zap.Any("partition", partition), zap.Any("offset", offset))
	}

	return &kafkaID{messageID: offset}, err
}

func (kp *kafkaProducer) Close() {
	log.Info("kafka producer close")
	kp.p.Close()
	log.Info("kafka producer close success")
}
