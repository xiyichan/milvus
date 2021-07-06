package mqclient

import (
	"context"
	"github.com/Shopify/sarama"
)

type kafkaProducer struct {
	k sarama.SyncProducer
}

func (kk *kafkaProducer) Send(ctx context.Context, message *ProducerMessage) error {
	//msgX:=&sarama.ProducerMessage{}

	return nil
}

func (kk *kafkaProducer) Close() {
	kk.k.Close()
}
