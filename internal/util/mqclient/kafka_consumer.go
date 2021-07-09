package mqclient

import (
	"errors"
	"github.com/Shopify/sarama"
)

type kafkaConsumer struct {
	g          sarama.ConsumerGroup
	c          sarama.Client
	msgChannel chan ConsumerMessage
	topicName  string
	groupID    string
}
type exampleConsumerGroupHandler struct{}

func (exampleConsumerGroupHandler) Setup(sess sarama.ConsumerGroupSession) error {

	return nil
}
func (exampleConsumerGroupHandler) Cleanup(sess sarama.ConsumerGroupSession) error {

	return nil

}
func (h exampleConsumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		//fmt.Printf("Message topic:%q partition:%d offset:%d\n", msg.Topic, msg.Partition, msg.Offset)
		sess.MarkMessage(msg, "")
		//fmt.Println(string(msg.Value))
	}
	return nil
}

func (kc *kafkaConsumer) Subscription() string {

	return ""
}
func (kc *kafkaConsumer) Chan() <-chan ConsumerMessage {

	return kc.msgChannel
}
func (kc *kafkaConsumer) Seek(id MessageID) error {
	//TODO:consumerGroup need close
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
		return errors.New("seek error")
	}
	if meta != "modified_meta" {
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
	return nil
}
func (kc *kafkaConsumer) Ack(message ConsumerMessage) {

}
func (kc *kafkaConsumer) Close() {
	kc.c.Close()
}
