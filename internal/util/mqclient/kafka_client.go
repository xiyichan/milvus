package mqclient

import (
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/kafka/client/kafka"
	"go.uber.org/zap"
	"strconv"
	"sync"
)

type kafkaClient struct {
	client kafka.Client
}

var kc *kafkaClient
var kafkaOnce sync.Once

func GetKafkaClientInstance(opts kafka.ClientOptions) (*kafkaClient, error) {
	once.Do(func() {
		c, err := kafka.NewClient(opts)
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
	kOpts := kafka.ProducerOptions{Topic: options.Topic}
	pp, err := kc.client.CreateProducer(kOpts)
	if err != nil {
		return nil, err
	}
	kp := kafkaProducer{p: pp}
	return &kp, nil

}
func (kc *kafkaClient) Subscribe(options ConsumerOptions) (Consumer, error) {
	receiveChannel := make(chan kafka.ConsumerMessage, options.BufSize)
	cli, err := kc.client.Subscribe(kafka.ConsumerOptions{
		Topic:            options.Topic,
		SubscriptionName: options.SubscriptionName,
		MessageChannel:   receiveChannel,
	})
	if err != nil {
		return nil, err
	}

	consumer := &kafkaConsumer{c: cli}
	return consumer, nil
}

func (kc *kafkaClient) EarliestMessageID(options ConsumerOptions) MessageID {
	offset := kafka.EarliestMessageID(options.Topic)
	return &kafkaID{messageID: offset}
}

func (kc *kafkaClient) StringToMsgID(id string) (MessageID, error) {
	offset, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		return nil, err
	}
	return &kafkaID{messageID: offset}, nil
}

func (kc *kafkaClient) BytesToMsgID(id []byte) (MessageID, error) {
	offset, err := DeserializeRmqID(id)
	if err != nil {
		return nil, err
	}
	return &kafkaID{messageID: offset}, nil
}

func (kc *kafkaClient) Close() {
	kc.client.Close()
}
