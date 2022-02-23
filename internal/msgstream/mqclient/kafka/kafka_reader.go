package kafka

import (
	"context"
	"fmt"
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
	closeFlag               bool
	highWaterMarkOffset     int64
	readFlag                bool
	startMessageIDInclusive bool
	subscriptionRolePrefix  string
	ctx                     context.Context
}

func (kr *kafkaReader) Setup(sess sarama.ConsumerGroupSession) error {
	fmt.Println("resetoffset", kr.topicName, kr.name)
	sess.ResetOffset(kr.topicName, 0, kr.offset, "test")

	return nil
}
func (kr *kafkaReader) Cleanup(sess sarama.ConsumerGroupSession) error {
	return nil

}
func (kr *kafkaReader) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	kr.highWaterMarkOffset = claim.HighWaterMarkOffset()
	fmt.Println("higtWater", claim.HighWaterMarkOffset(), kr.offset)
	for msg := range claim.Messages() {
		kr.msgChannel <- &kafkaMessage{msg: msg}
		sess.MarkMessage(msg, "")
		//fmt.Println("msg", msg.Value)
		fmt.Println(msg.Offset, kr.highWaterMarkOffset)
		if msg.Offset == kr.highWaterMarkOffset-1 {
			fmt.Println("close")
			//close(kr.closeCh)
			kr.closeFlag = true
		}
	}
	fmt.Println(333)
	return nil
}

func (kr *kafkaReader) Topic() string {
	return kr.topicName
}
func (kr *kafkaReader) Next(ctx context.Context) (mqclient.Message, error) {
	var err error
	if kr.readFlag == true {
		kr.readFlag = false
		go func() {
			for {
				//fmt.Println("11111")
				topics := []string{kr.topicName}
				err = kr.cg.Consume(ctx, topics, kr)
				//fmt.Println("22222")
				//fmt.Println(kr.name)
				if err != nil {
					//clf: close will err
					log.Error("kafka reader consume err", zap.Any("topic", topics), zap.Error(err))
					//panic(err)
				}
				//if ctx.Err() != nil {
				//	log.Info("ctx err", zap.Any("ctx", ctx.Err()))
				//	return nil, ctx.Err()
				//}
				if kr.closeFlag == true {
					return
				}
			}
		}()

	}

	select {
	case msg := <-kr.msgChannel:
		kr.offset++
		return msg, nil
	}
}

func (kr *kafkaReader) HasNext() bool {
	fmt.Println(kr.name, kr.offset, kr.highWaterMarkOffset)
	///clf: if dont consume highwater martoffset == 0

	//if kr.readFlag == true {
	//	kr.readFlag = false
	//	go func() {
	//		for {
	//			//fmt.Println("11111")
	//			topics := []string{kr.topicName}
	//			_ = kr.cg.Consume(context.TODO(), topics, kr)
	//			//fmt.Println("22222")
	//			//fmt.Println(kr.name)
	//		}
	//	}()
	//
	//}

	if kr.highWaterMarkOffset == 0 {
		return true
	}
	if kr.offset <= kr.highWaterMarkOffset {
		return true
	}
	return false
}

func (kr *kafkaReader) Close() {
	kr.cg.PauseAll()
	//stop := make(map[string][]int32)
	//stop[kr.topicName] = []int32{0}
	//kr.cg.Pause(stop)
	err := kr.cg.Close()
	if err != nil {
		log.Error("err", zap.Any("err", err))
	}
	//close(kr.closeCh)
	close(kr.msgChannel)
}

func (kr *kafkaReader) Seek(id mqclient.MessageID) error {
	kr.offset = id.(*kafkaID).messageID
	return nil
}
