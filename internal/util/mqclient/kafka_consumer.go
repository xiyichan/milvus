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
	//	closeClaim chan struct{}
}

func (kc *kafkaConsumer) Setup(sess sarama.ConsumerGroupSession) error {
	log.Info("setup")
	return nil
}
func (kc *kafkaConsumer) Cleanup(sess sarama.ConsumerGroupSession) error {
	log.Info("Clean up")
	//所有claim推出之后 关闭msgChan
	close(kc.msgChannel)
	log.Info("close kc.msgChannel")
	//close(kc.closeCh)
	//log.Info("close kc.closeCh")
	//
	return nil

}
func (kc *kafkaConsumer) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	log.Info("consumer claim start")

	log.Info("message length", zap.Any("length", len(claim.Messages())), zap.Any("topic", claim.Topic()))

	kc.wg.Add(1)
	for msg := range claim.Messages() {
		//fmt.Printf("Message topic:%q partition:%d offset:%d\n", msg.Topic, msg.Partition, msg.Offset)
		kc.msgChannel <- &kafkaMessage{msg: msg}
		sess.MarkMessage(msg, "")
		log.Info("receive msg", zap.Any("msg", msg.Value))
		//fmt.Println(string(msg.Value))
		//if len(claim.Messages()) == 0 {
		//	log.Info("close msgChannel success")
		//	close(kc.msgChannel)
		//	//close(kc.end)
		//	break
		//}

		//_,ok:= <-kc.closeClaim
		//if !ok{
		//	log.Info("clos msgChannel success")
		//	close(kc.msgChannel)
		//	break
		//}
		//收到了关闭的请求,所有协程都得退出
		_, ok := <-kc.closeCh
		if !ok {
			//close(kc.closeClaim)
			log.Info("关闭协程")
			break
		}
	}
	kc.wg.Done()
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
				//select {
				//case <-kc.closeCh:
				//	log.Info("consumer close")
				//
				//	kc.wg.Done()
				//	return
				//
				//}
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
func (kc *kafkaConsumer) Seek(id MessageID) error {
	log.Info("function seek")
	//TODO:consumerGroup need close
	kc.lock.Lock()
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Consumer.Offsets.Initial = -2
	config.Version = sarama.V2_8_0_0
	log.Info("kafka start seek")
	log.Info("kc status", zap.Any("kc status", kc.c.Closed()))
	err := kc.g.Close()
	if err != nil {
		return err
	}
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

		kc.g, _ = sarama.NewConsumerGroup([]string{"47.106.76.166:9092"}, kc.groupID, config)
		kc.lock.Unlock()
		return errors.New("seek error")
	}
	if meta != "modified_meta" {
		log.Error("kafka seek err")
		kc.g, _ = sarama.NewConsumerGroup([]string{"47.106.76.166:9092"}, kc.groupID, config)
		kc.lock.Unlock()
		return errors.New("seek error")
	}

	log.Info("pom close")
	err = pom.Close()
	if err != nil {
		log.Error("", zap.Any("err", err))
		return err
	}
	log.Info("of close")
	//TODO:不知道为什么这个为什么会关不了
	err = of.Close()
	if err != nil {
		log.Error("", zap.Any("err", err))
		return err
	}

	log.Info("reconnect consumerGroup")

	//不能使用newconsumerGroupfromclent
	kc.g, _ = sarama.NewConsumerGroup([]string{"47.106.76.166:9092"}, kc.groupID, config)
	log.Info("reset offset success")
	kc.lock.Unlock()
	return nil
}
func (kc *kafkaConsumer) Ack(message ConsumerMessage) {
	log.Info("ack msg", zap.Any("msg", len(message.Payload())))
}
func (kc *kafkaConsumer) Close() {
	//加锁为了退出时消费消息已经消费完
	log.Info("close consumer")

	kc.lock.Lock()
	//TODO：我认为这个也有bug

	close(kc.closeCh)
	log.Info("关闭信号")
	kc.wg.Wait()
	log.Info("协程全关闭")
	//	kc.wg.Wait()
	err := kc.g.Close()
	if err != nil {
		log.Error("err", zap.Any("err", err))
	}
	kc.lock.Unlock()
	log.Info("close consumer success")
	//kc.c.Close()
}
