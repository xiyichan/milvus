package mqclient

import (
	"context"
	"errors"
	"github.com/Shopify/sarama"
	"github.com/milvus-io/milvus/internal/log"
	"go.uber.org/zap"
	"sync"
)

type kafkaConsumer struct {
	g          sarama.ConsumerGroup
	c          sarama.Client
	msgChannel chan ConsumerMessage
	lock       sync.Mutex
	topicName  string
	groupID    string
	hasSeek    bool
}

func (kc *kafkaConsumer) Setup(sess sarama.ConsumerGroupSession) error {

	return nil
}
func (kc *kafkaConsumer) Cleanup(sess sarama.ConsumerGroupSession) error {

	return nil

}
func (kc *kafkaConsumer) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	log.Info("consumer claim start")

	log.Info("topic", zap.Any("t", claim.Topic()))
	log.Info("message length", zap.Any("l", len(claim.Messages())))
	for msg := range claim.Messages() {
		//fmt.Printf("Message topic:%q partition:%d offset:%d\n", msg.Topic, msg.Partition, msg.Offset)
		kc.msgChannel <- &kafkaMessage{msg: msg}
		sess.MarkMessage(msg, "")
		log.Info("receive msg", zap.Any("msg", msg))
		//fmt.Println(string(msg.Value))
	}

	return nil
}

func (kc *kafkaConsumer) Subscription() string {
	return kc.groupID
}
func (kc *kafkaConsumer) Chan() <-chan ConsumerMessage {
	log.Info("kafka groupID", zap.Any("group_id", kc.groupID))
	var err error
	kc.g, err = sarama.NewConsumerGroupFromClient(kc.groupID, kc.c)
	if err != nil {
		log.Error("kafka init consumer", zap.Any("err", err))
	}
	//defer func() { _ = kc.g.Close() }()
	go func() {
		for err := range kc.g.Errors() {
			log.Error("ERROR", zap.Error(err))
		}
	}()

	if kc.msgChannel == nil {
		kc.msgChannel = make(chan ConsumerMessage)
		//if !kc.hasSeek {
		//	kc.c.SeekByTime(time.Unix(0, 0))
		//}
		ctx := context.Background()
		go func() {
			log.Info("kafka start consume")

			for {
				kc.g, err = sarama.NewConsumerGroupFromClient(kc.groupID, kc.c)
				topics := []string{kc.topicName}
				handler := kafkaConsumer{}

				// `Consume` should be called inside an infinite loop, when a
				// server-side rebalance happens, the consumer session will need to be
				// recreated to get the new claims
				kc.lock.Lock()
				err = kc.g.Consume(ctx, topics, &handler)
				if err != nil {
					log.Info("err topic", zap.Any("topic", topics))
					log.Error("kafka consume err", zap.Error(err))
					//panic(err)
				}
				kc.lock.Unlock()

			}
		}()

	}
	return kc.msgChannel
}
func (kc *kafkaConsumer) Seek(id MessageID) error {
	log.Info("kafka start seek")
	//TODO:consumerGroup need close
	kc.lock.Lock()
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
	pom.ResetOffset(expected, "modified_meta")
	actual, meta := pom.NextOffset()
	if actual != expected {
		log.Error("kafka seek err")
		return errors.New("seek error")
	}
	if meta != "modified_meta" {
		log.Error("kafka seek err")
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

}
func (kc *kafkaConsumer) Close() {
	kc.g.Close()
	kc.c.Close()
}
