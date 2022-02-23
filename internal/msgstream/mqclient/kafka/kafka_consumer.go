package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream/mqclient"
	"go.uber.org/zap"
)

//
type kafkaConsumer struct {
	c          sarama.Consumer
	msgChannel chan mqclient.Message
	offset     int64
	topicName  string
	groupID    string
	closeCh    chan struct{}
}

func (kc *kafkaConsumer) Subscription() string {
	return kc.groupID
}
func (kc *kafkaConsumer) Chan() <-chan mqclient.Message {
	if kc.msgChannel == nil {
		kc.msgChannel = make(chan mqclient.Message)
		partitionConsumer, err := kc.c.ConsumePartition(kc.topicName, 0, kc.offset)
		if err != nil {
			log.Error("[Kafka] consumePartition err", zap.String("topic", kc.topicName), zap.Int64("offset", kc.offset))
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
