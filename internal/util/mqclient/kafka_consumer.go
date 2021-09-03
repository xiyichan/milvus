package mqclient

import (
	"context"
	"errors"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/milvus-io/milvus/internal/log"
	"go.uber.org/zap"
)

type kafkaConsumer struct {
	g          sarama.ConsumerGroup
	c          sarama.Client
	msgChannel chan ConsumerMessage
	wg         sync.WaitGroup
	lock       sync.Mutex
	topicName  string
	//end        chan bool
	groupID string
	closeCh chan struct{}
}

func (kc *kafkaConsumer) Setup(sess sarama.ConsumerGroupSession) error {
	log.Info("setup")
	return nil
}
func (kc *kafkaConsumer) Cleanup(sess sarama.ConsumerGroupSession) error {
	log.Info("Clean up")
	//close(kc.msgChannel)
	close(kc.closeCh)
	//
	return nil

}
func (kc *kafkaConsumer) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	log.Info("consumer claim start")

	log.Info("message length", zap.Any("length", len(claim.Messages())), zap.Any("topic", claim.Topic()))
	kc.lock.Lock()

	for msg := range claim.Messages() {
		//fmt.Printf("Message topic:%q partition:%d offset:%d\n", msg.Topic, msg.Partition, msg.Offset)
		kc.msgChannel <- &kafkaMessage{msg: msg}
		sess.MarkMessage(msg, "")
		log.Info("receive msg", zap.Any("msg", msg.Value))
		//fmt.Println(string(msg.Value))
		if len(claim.Messages()) == 0 {
			log.Info("close msgChannel success")
			close(kc.msgChannel)
			//close(kc.end)
			break
		}

	}

	kc.lock.Unlock()
	return nil
}

func (kc *kafkaConsumer) Subscription() string {
	return kc.groupID
}
func (kc *kafkaConsumer) Chan() <-chan ConsumerMessage {
	log.Info("kafka groupID", zap.Any("group_id", kc.groupID), zap.Any("topic", kc.topicName))
	var err error
	if kc.msgChannel == nil {
		kc.msgChannel = make(chan ConsumerMessage)
		ctx := context.Background()
		kc.wg.Add(1)
		go func() {
			log.Info("kafka start consume")

			//kc.g, err = sarama.NewConsumerGroupFromClient(kc.groupID, kc.c)
			for {
				//kc.lock.Lock()

				topics := []string{kc.topicName}
				log.Debug("Before consume", zap.Any("topic", topics))
				err = kc.g.Consume(ctx, topics, kc)
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
				select {
				case <-kc.closeCh:
					kc.wg.Done()
					return

				}
				//_, ok := <-kc.closeCh:
				//if !ok {
				//	log.Info("close kafka consume")
				//	kc.wg.Done()
				//	return
				//}
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
func (kc *kafkaConsumer) Seek(id MessageID) error {
	log.Info("function seek")
	//TODO:consumerGroup need close
	kc.lock.Lock()
	log.Info("kafka start seek")
	kc.g.Close()
	of, err := sarama.NewOffsetManagerFromClient(kc.groupID, kc.c)
	if err != nil {
		return err
	}
	pom, err := of.ManagePartition(kc.topicName, 0)
	if err != nil {
		return err
	}
	expected := id.(*kafkaID).messageID.Offset
	log.Debug("reset offset", zap.Any("offset", expected), zap.Any("topic", kc.topicName), zap.Any("groupID", kc.groupID))
	pom.ResetOffset(expected, "modified_meta")
	actual, meta := pom.NextOffset()

	log.Debug("reset offset after", zap.Any("actual", actual), zap.Any("meta", meta))
	if actual != expected {
		log.Error("kafka seek err")

		kc.g, _ = sarama.NewConsumerGroupFromClient(kc.groupID, kc.c)
		kc.lock.Unlock()
		return errors.New("seek error")
	}
	if meta != "modified_meta" {
		log.Error("kafka seek err")

		kc.g, _ = sarama.NewConsumerGroupFromClient(kc.groupID, kc.c)
		kc.lock.Unlock()
		return errors.New("seek error")
	}
	err = pom.Close()
	if err != nil {
		return err
	}
	err = of.Close()
	if err != nil {
		return err
	}
	kc.g, _ = sarama.NewConsumerGroupFromClient(kc.groupID, kc.c)
	kc.lock.Unlock()
	return nil
}
func (kc *kafkaConsumer) Ack(message ConsumerMessage) {
	log.Info("ack msg", zap.Any("msg", len(message.Payload())))
}
func (kc *kafkaConsumer) Close() {
	//加锁为了退出时消费消息已经消费完
	kc.lock.Lock()
	//	close(kc.closeCh)
	kc.wg.Wait()
	kc.g.Close()
	kc.lock.Unlock()
	//kc.c.Close()
}
