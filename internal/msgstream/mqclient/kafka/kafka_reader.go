package kafka

import (
	"context"
	"github.com/Shopify/sarama"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream/mqclient"
	"go.uber.org/zap"
)

//type kafkaReader struct {
//	r                       sarama.Consumer
//	cp                      sarama.PartitionConsumer
//	cg sarama.ConsumerGroup
//	name                    string
//	offset                  int64
//	topicName               string
//	StartMessageIDInclusive bool
//	SubscriptionRolePrefix  string
//}
//
//func (kr *kafkaReader) Topic() string {
//	return kr.topicName
//}
//func (kr *kafkaReader) Next(ctx context.Context) (Message, error) {
//	select {
//	case kMsg := <-kr.cp.Messages():
//		msg := &kafkaMessage{msg: kMsg}
//		kr.offset = msg.msg.Offset
//		return msg, nil
//	case err := <-kr.cp.Errors():
//		return nil, err
//	}
//}
//
//func (kr *kafkaReader) HasNext() bool {
//
//	hwmo := kr.cp.HighWaterMarkOffset()
//	if kr.offset <= hwmo {
//		return true
//	} else {
//		return false
//	}
//}
//
//func (kr *kafkaReader) Close() {
//	kr.r.Close()
//}
//func (kr *kafkaReader) Seek(id MessageID) error {
//	var err error
//	kr.offset = id.(*kafkaID).messageID
//	kr.cp, err = kr.r.ConsumePartition(kr.topicName, 0, kr.offset)
//	return err
//}

type kafkaReader struct {
	//r                       sarama.Consumer
	//cp                      sarama.PartitionConsumer
	cg                  sarama.ConsumerGroup
	name                string
	offset              int64
	topicName           string
	msgChannel          chan mqclient.Message
	closeCh             chan struct{}
	highWaterMarkOffset int64
	readFlag            bool
	//readChannel chan struct{}
	StartMessageIDInclusive bool
	SubscriptionRolePrefix  string
}

func (kr *kafkaReader) Setup(sess sarama.ConsumerGroupSession) error {
	sess.ResetOffset(kr.topicName, 0, kr.offset, "modified_meta")

	//log.Info("setup")
	return nil
}
func (kr *kafkaReader) Cleanup(sess sarama.ConsumerGroupSession) error {
	//log.Info("Clean up")
	//所有claim推出之后 关闭msgChan
	//close(kc.msgChannel)

	return nil

}
func (kr *kafkaReader) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	//log.Info("consumer claim start")
	//log.Info("message length", zap.Any("length", len(claim.Messages())), zap.Any("topic", claim.Topic()))

	for msg := range claim.Messages() {
		//fmt.Printf("Message topic:%q partition:%d offset:%d\n", msg.Topic, msg.Partition, msg.Offset)
		kr.msgChannel <- &kafkaMessage{msg: msg}
		sess.MarkMessage(msg, "")
		//log.Info("receive msg", zap.Any("msg", msg.Value))
		//fmt.Println(string(msg.Value))
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
	log.Info("kafka reader next")
	var err error
	if kr.readFlag == true {
		for {
			topics := []string{kr.topicName}
			err = kr.cg.Consume(ctx, topics, kr)
			if err != nil {
				log.Info("err reader topic", zap.Any("topic", topics))
				log.Error("kafka reader consume err", zap.Error(err))
				panic(err)
			}
			if ctx.Err() != nil {
				log.Info("ctx err", zap.Any("ctx", ctx.Err()))
				return nil, ctx.Err()
			}
			_, ok := <-kr.closeCh
			if !ok {
				//close(kc.closeClaim)
				//等所有协程claim退出在退出for
				log.Info("关闭线程")
				//kc.wg.Done()
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

	//log.Info("关闭信号")

	//log.Info("协程所有关闭")
	err := kr.cg.Close()
	if err != nil {
		log.Error("err", zap.Any("err", err))
	}

	log.Info("close consumer success")
}
func (kr *kafkaReader) Seek(id mqclient.MessageID) error {
	kr.offset = id.(*kafkaID).messageID
	return nil
}
