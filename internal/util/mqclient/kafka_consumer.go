package mqclient

import (
	"github.com/Shopify/sarama"
)

type kafkaConsumer struct {
	c          sarama.Consumer
	msgChannel chan ConsumerMessage
	offset     int64
	topicName  string
	groupID    string
	closeCh    chan struct{}
}

func (kc *kafkaConsumer) Subscription() string {
	return kc.groupID
}
func (kc *kafkaConsumer) Chan() <-chan ConsumerMessage {
	if kc.msgChannel == nil {
		kc.msgChannel = make(chan ConsumerMessage)
		partitionConsumer, err := kc.c.ConsumePartition(kc.topicName, 0, kc.offset)
		if err != nil {
			panic(err)
		}
		go func(pc *sarama.PartitionConsumer) {
			defer (*pc).Close()
			for {
				select {
				case msg, ok := <-(*pc).Messages():
					if !ok {
						return
					}
					kc.msgChannel <- &kafkaMessage{msg: msg}
				case <-kc.closeCh:
					close(kc.msgChannel)
					return
				}
			}

		}(&partitionConsumer)
	}
	return kc.msgChannel
}
func (kc *kafkaConsumer) Seek(id MessageID) error {
	kc.offset = id.(*kafkaID).messageID.Offset
	return nil
}
func (kc *kafkaConsumer) Ack(message ConsumerMessage) {
	//log.Info("ack msg", zap.Any("msg", len(message.Payload())))
}
func (kc *kafkaConsumer) Close() {
	close(kc.closeCh)
	kc.c.Close()

}
