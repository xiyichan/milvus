package mqclient

import (
	"context"
	sarama "github.com/Shopify/sarama"
)

type kafkaReader struct {
	r                       sarama.Consumer
	cp                      sarama.PartitionConsumer
	name                    string
	offset                  int64
	topicName               string
	StartMessageIDInclusive bool
	SubscriptionRolePrefix  string
}

func (kr *kafkaReader) Topic() string {
	return kr.topicName
}
func (kr *kafkaReader) Next(ctx context.Context) (Message, error) {
	select {
	case kMsg := <-kr.cp.Messages():
		msg := &kafkaMessage{msg: kMsg}
		kr.offset = msg.msg.Offset
		return msg, nil
	case err := <-kr.cp.Errors():
		return nil, err
	}
}

func (kr *kafkaReader) HasNext() bool {

	hwmo := kr.cp.HighWaterMarkOffset()
	if kr.offset <= hwmo {
		return true
	} else {
		return false
	}
}

func (kr *kafkaReader) Close() {
	kr.r.Close()
}
func (kr *kafkaReader) Seek(id MessageID) error {
	var err error
	kr.offset = id.(*kafkaID).messageID
	kr.cp, err = kr.r.ConsumePartition(kr.topicName, 0, kr.offset)
	return err
}
