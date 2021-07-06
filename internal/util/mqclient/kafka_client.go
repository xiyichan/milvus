package mqclient

import (
	"errors"
	"github.com/Shopify/sarama"
	"github.com/milvus-io/milvus/internal/log"
	"go.uber.org/zap"
	"sync"
)

type kafkaClient struct {
	client sarama.Client
}

var kc *kafkaClient
var kafkaOnce sync.Once

func GetKafkaClientInstance(kafkaAddress []string, config *sarama.Config) (*kafkaClient, error) {
	once.Do(func() {
		c, err := sarama.NewClient(kafkaAddress, config)
		if err != nil {
			log.Error("Set pulsar client failed, error", zap.Error(err))
			return
		}
		cli := &kafkaClient{client: c}
		kc = cli
	})
	return kc, nil
}

func (kc *kafkaClient) CreateProducer(options ProducerOptions) (Producer, error) {
	kk, err := sarama.NewSyncProducerFromClient(kc.client)
	if err != nil {
		return nil, err
	}
	if kk == nil {
		return nil, errors.New("kafka is not ready, producer is nil")
	}
	producer := &kafkaProducer{k: kk}
	return producer, nil

}
func (kc *kafkaClient) Subscribe(options ConsumerOptions) (Consumer, error) {
	kk, err := sarama.NewConsumerGroupFromClient(options.SubscriptionName, kc.client)
	if err != nil {
		return nil, err
	}
	consumer := &kafkaConsumer{c: kk}
	return consumer, nil
}

func (kc *kafkaClient) EarliestMessageID() MessageID {

}

func (kc *kafkaClient) StringToMsgID(id string) (MessageID, error) {

}

func (kc *kafkaClient) BytesToMsgID(id []byte) (MessageID, error) {

}

func (kc *kafkaClient) Close() {
	kc.client.Close()
}
