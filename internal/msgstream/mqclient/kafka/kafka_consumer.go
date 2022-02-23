package kafka

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream/mqclient"
	"go.uber.org/zap"
)

//
//type kafkaConsumer struct {
//	c          sarama.Consumer
//	msgChannel chan mqclient.Message
//	offset     int64
//	topicName  string
//	groupID    string
//	closeCh    chan struct{}
//}
//
//func (kc *kafkaConsumer) Subscription() string {
//	return kc.groupID
//}
//func (kc *kafkaConsumer) Chan() <-chan mqclient.Message {
//	if kc.msgChannel == nil {
//		kc.msgChannel = make(chan mqclient.Message)
//		partitionConsumer, err := kc.c.ConsumePartition(kc.topicName, 0, kc.offset)
//		if err != nil {
//			log.Error("[Kafka] consumePartition err", zap.String("topic", kc.topicName), zap.Int64("offset", kc.offset))
//			panic(err)
//		}
//		go func(pc *sarama.PartitionConsumer) {
//			defer (*pc).Close()
//			for {
//				select {
//				case msg, ok := <-(*pc).Messages():
//					if !ok {
//						return
//					}
//					kc.msgChannel <- &kafkaMessage{msg: msg}
//				case <-kc.closeCh:
//					close(kc.msgChannel)
//					return
//				}
//			}
//
//		}(&partitionConsumer)
//	}
//	return kc.msgChannel
//}
//
//func (kc *kafkaConsumer) Seek(id mqclient.MessageID, inclusive bool) error {
//	kc.offset = id.(*kafkaID).messageID
//
//	return nil
//}
//
//func (kc *kafkaConsumer) Ack(message mqclient.Message) {
//	//log.Info("ack msg", zap.Any("msg", len(message.Payload())))
//}
//
//func (kc *kafkaConsumer) Close() {
//	close(kc.closeCh)
//	kc.c.Close()
//
//}

//
type kafkaConsumer struct {
	c          sarama.ConsumerGroup
	msgChannel chan mqclient.Message
	offset     int64
	topicName  string
	groupID    string
	closeCh    chan struct{}
	closeFlag  bool
}

func (kc *kafkaConsumer) Setup(sess sarama.ConsumerGroupSession) error {
	fmt.Println("resetoffset", kc.topicName, kc.Subscription())
	sess.ResetOffset(kc.topicName, 0, kc.offset, "test")

	return nil
}
func (kc *kafkaConsumer) Cleanup(sess sarama.ConsumerGroupSession) error {
	return nil

}
func (kc *kafkaConsumer) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	fmt.Println("higtWater", claim.HighWaterMarkOffset(), kc.offset)
	for msg := range claim.Messages() {
		kc.msgChannel <- &kafkaMessage{msg: msg}
		sess.MarkMessage(msg, "")
		//fmt.Println("msg", msg.Value)
		//fmt.Println(msg.Offset, kc.highWaterMarkOffset)
		//if msg.Offset == kc.highWaterMarkOffset-1 {
		//	fmt.Println("close")
		//	//close(kc.closeCh)
		//	kc.closeFlag = true
		//}
	}
	fmt.Println(333)
	return nil
}

func (kc *kafkaConsumer) Subscription() string {
	return kc.groupID
}
func (kc *kafkaConsumer) Chan() <-chan mqclient.Message {
	if kc.msgChannel == nil {
		kc.msgChannel = make(chan mqclient.Message)
		go func() {
			for {
				//fmt.Println("11111")
				topics := []string{kc.topicName}
				err := kc.c.Consume(context.Background(), topics, kc)
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
				if kc.closeFlag == true {
					return
				}
			}
		}()
	}
	return kc.msgChannel
}

func (kc *kafkaConsumer) Seek(id mqclient.MessageID, inclusive bool) error {
	kc.offset = id.(*kafkaID).messageID

	return nil
}

func (kc *kafkaConsumer) Ack(message mqclient.Message) {
	//log.Info("ack msg", zap.Any("msg", len(message.Payload())))
}

func (kc *kafkaConsumer) Close() {
	close(kc.closeCh)
	kc.c.Close()

}
