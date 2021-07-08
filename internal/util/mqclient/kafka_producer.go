package mqclient

import (
	"context"
	"github.com/milvus-io/milvus/internal/util/kafka/client/kafka"
)

type kafkaProducer struct {
	p kafka.Producer
}

func (kp *kafkaProducer) Topic() string {
	return kp.p.Topic()
}
func (kp *kafkaProducer) Send(ctx context.Context, message *ProducerMessage) error {
	pm := &kafka.ProducerMessage{Payload: message.Payload}
	return kp.p.Send(pm)
}

func (kp *kafkaProducer) Close() {

}
