package mqclient

import (
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/milvus-io/milvus/internal/log"
	"go.uber.org/zap"
	"gopkg.in/Shopify/sarama.v1"
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

}
func (kc *kafkaClient) Subscribe(options ConsumerOptions) (Consumer, error) {

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
