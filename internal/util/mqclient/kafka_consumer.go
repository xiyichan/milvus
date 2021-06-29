package mqclient

import "gopkg.in/Shopify/sarama.v1"

type kafkaConsumer struct {
	c          sarama.ConsumerGroup
	msgChannel chan ConsumerMessage
}

func (kc *kafkaConsumer) Subscription() string {

	return ""
}
func (kc *kafkaConsumer) Chan() <-chan ConsumerMessage {

	return kc.msgChannel
}
func (kc *kafkaConsumer) Seek(id MessageID) error {

	return nil
}
func (kc *kafkaConsumer) Ack(message ConsumerMessage) {

}
func (kc *kafkaConsumer) Close() {
	kc.c.Close()
}
