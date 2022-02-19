package kafka

import (
	"context"
	"github.com/Shopify/sarama"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream/mqclient"
	"go.uber.org/zap"
)

type kafkaReader struct {
	cg                      sarama.ConsumerGroup
	name                    string
	offset                  int64
	topicName               string
	msgChannel              chan mqclient.Message
	closeCh                 chan struct{}
	highWaterMarkOffset     int64
	readFlag                bool
	startMessageIDInclusive bool
	subscriptionRolePrefix  string
}

func (kr *kafkaReader) Setup(sess sarama.ConsumerGroupSession) error {
	sess.ResetOffset(kr.topicName, 0, kr.offset, "modified_meta")

	return nil
}
func (kr *kafkaReader) Cleanup(sess sarama.ConsumerGroupSession) error {
	return nil

}
func (kr *kafkaReader) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		kr.msgChannel <- &kafkaMessage{msg: msg}
		sess.MarkMessage(msg, "")
		if msg.Offset == kr.highWaterMarkOffset {
			close(kr.closeCh)
		}
	}

	return nil
}

func (kr *kafkaReader) Topic() string {
	return kr.topicName
}
func (kr *kafkaReader) Next(ctx context.Context) (mqclient.Message, error) {
	var err error
	if kr.readFlag == true {
		for {
			topics := []string{kr.topicName}
			err = kr.cg.Consume(ctx, topics, kr)
			if err != nil {
				log.Error("kafka reader consume err", zap.Any("topic", topics), zap.Error(err))
				panic(err)
			}
			if ctx.Err() != nil {
				log.Info("ctx err", zap.Any("ctx", ctx.Err()))
				return nil, ctx.Err()
			}
			_, ok := <-kr.closeCh
			if !ok {
				close(kr.msgChannel)
				kr.readFlag = false
				break
			}
		}
	}

	select {
	case msg := <-kr.msgChannel:
		kr.offset++
		return msg, nil
	}
}

func (kr *kafkaReader) HasNext() bool {
	if kr.offset <= kr.highWaterMarkOffset {
		return true
	}
	return false
}

func (kr *kafkaReader) Close() {
	err := kr.cg.Close()
	if err != nil {
		log.Error("err", zap.Any("err", err))
	}
}

func (kr *kafkaReader) Seek(id mqclient.MessageID) error {
	kr.offset = id.(*kafkaID).messageID
	return nil
}
