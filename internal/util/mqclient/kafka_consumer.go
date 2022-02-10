package mqclient

import (
	"context"
	"github.com/Shopify/sarama"
	"github.com/milvus-io/milvus/internal/log"
	"go.uber.org/zap"
	"sync"
)

//
//type kafkaConsumer struct {
//	c          sarama.Consumer
//	msgChannel chan Message
//	offset     int64
//	topicName  string
//	groupID    string
//	closeCh    chan struct{}
//}
//
//func (kc *kafkaConsumer) Subscription() string {
//	return kc.groupID
//}
//func (kc *kafkaConsumer) Chan() <-chan Message {
//	if kc.msgChannel == nil {
//		kc.msgChannel = make(chan Message)
//		partitionConsumer, err := kc.c.ConsumePartition(kc.topicName, 0, kc.offset)
//		if err != nil {
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
//func (kc *kafkaConsumer) Seek(id MessageID, inclusive bool) error {
//	kc.offset = id.(*kafkaID).messageID
//
//	return nil
//}
//func (kc *kafkaConsumer) Ack(message Message) {
//	//log.Info("ack msg", zap.Any("msg", len(message.Payload())))
//}
//func (kc *kafkaConsumer) Close() {
//	close(kc.closeCh)
//	kc.c.Close()
//
//}
type kafkaConsumer struct {
	g          sarama.ConsumerGroup
	c          sarama.Client
	msgChannel chan Message
	wg         sync.WaitGroup
	lock       sync.Mutex
	topicName  string
	//end        chan bool
	offset  int64
	groupID string
	closeCh chan struct{}
	//	closeClaim chan struct{}
}

func (kc *kafkaConsumer) Setup(sess sarama.ConsumerGroupSession) error {
	sess.ResetOffset(kc.topicName, 0, kc.offset, "modified_meta")

	log.Info("setup")
	return nil
}
func (kc *kafkaConsumer) Cleanup(sess sarama.ConsumerGroupSession) error {
	log.Info("Clean up")
	//所有claim推出之后 关闭msgChan
	close(kc.msgChannel)

	return nil

}
func (kc *kafkaConsumer) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	log.Info("consumer claim start")
	log.Info("message length", zap.Any("length", len(claim.Messages())), zap.Any("topic", claim.Topic()))
	for msg := range claim.Messages() {
		//fmt.Printf("Message topic:%q partition:%d offset:%d\n", msg.Topic, msg.Partition, msg.Offset)
		kc.msgChannel <- &kafkaMessage{msg: msg}
		sess.MarkMessage(msg, "")
		log.Info("receive msg", zap.Any("msg", msg.Value))
		//fmt.Println(string(msg.Value))

	}

	return nil
}

func (kc *kafkaConsumer) Subscription() string {
	return kc.groupID
}
func (kc *kafkaConsumer) Chan() <-chan Message {
	log.Info("kafka groupID", zap.Any("group_id", kc.groupID), zap.Any("topic", kc.topicName))
	var err error
	if kc.msgChannel == nil {
		kc.msgChannel = make(chan Message)
		ctx := context.Background()

		go func() {

			log.Info("kafka start consume")
			//kc.closeClaim = make(chan struct{})
			//kc.g, err = sarama.NewConsumerGroupFromClient(kc.groupID, kc.c)
			for {
				//kc.lock.Lock()

				topics := []string{kc.topicName}
				log.Debug("Before consume", zap.Any("topic", topics))
				//	kc.lock.Lock()
				err = kc.g.Consume(ctx, topics, kc)
				//	kc.lock.Unlock()
				log.Debug("After consume", zap.Any("topic", topics))
				if err != nil {
					log.Info("err topic", zap.Any("topic", topics))
					log.Error("kafka consume err", zap.Error(err))
					panic(err)
				}
				if ctx.Err() != nil {
					log.Info("ctx err", zap.Any("ctx", ctx.Err()))
					return
				}
				_, ok := <-kc.closeCh
				if !ok {
					//close(kc.closeClaim)
					//等所有协程claim退出在退出for
					log.Info("关闭线程")
					//kc.wg.Done()

					break
				}

			}
		}()
	}
	if kc.msgChannel == nil {
		log.Debug("consume finish error")
	} else {
		log.Debug("consume finish success")
	}
	return kc.msgChannel
}
func (kc *kafkaConsumer) Seek(id MessageID, inclusive bool) error {
	kc.offset = id.(*kafkaID).messageID

	return nil
}
func (kc *kafkaConsumer) Ack(message Message) {
	log.Info("ack msg", zap.Any("msg", len(message.Payload())))
}
func (kc *kafkaConsumer) Close() {
	//加锁为了退出时消费消息已经消费完
	log.Info("close consumer")

	kc.lock.Lock()
	close(kc.closeCh)
	log.Info("关闭信号")
	kc.wg.Wait()
	log.Info("协程所有关闭")
	err := kc.g.Close()
	if err != nil {
		log.Error("err", zap.Any("err", err))
	}
	kc.lock.Unlock()
	log.Info("close consumer success")
	//kc.c.Close()
}
