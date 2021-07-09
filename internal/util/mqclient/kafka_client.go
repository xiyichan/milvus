package mqclient

import (
	"errors"
	"github.com/Shopify/sarama"
	"github.com/milvus-io/milvus/internal/log"
	"go.uber.org/zap"
	"strconv"
	"sync"
)

type kafkaClient struct {
	client sarama.Client
}

var kc *kafkaClient
var kafkaOnce sync.Once

func GetKafkaClientInstance(broker []string, opts sarama.Config) (*kafkaClient, error) {
	once.Do(func() {
		c, err := sarama.NewClient(broker, &opts)
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
	pp, err := sarama.NewSyncProducerFromClient(kc.client)
	if err != nil {
		return nil, err
	}
	if pp == nil {
		return nil, errors.New("kafka is not ready, producer is nil")
	}
	producer := &kafkaProducer{p: pp, c: kc.client}
	return producer, nil
}

func (kc *kafkaClient) Subscribe(options ConsumerOptions) (Consumer, error) {
	group, err := sarama.NewConsumerGroupFromClient(options.SubscriptionName, kc.client)
	if err != nil {
		panic(err)
	}
	defer func() { _ = group.Close() }()

	// Track errors
	go func() {
		for _ = range group.Errors() {
			//log.Debug(err)
		}
	}()

	//ctx := context.Background()
	//for {
	//	topics := []string{options.Topic}
	//	handler := exampleConsumerGroupHandler{}
	//
	//	// `Consume` should be called inside an infinite loop, when a
	//	// server-side rebalance happens, the consumer session will need to be
	//	// recreated to get the new claims
	//	err := group.Consume(ctx, topics, handler)
	//	if err != nil {
	//		panic(err)
	//	}
	//}

	consumer := &kafkaConsumer{g: group, c: kc.client, topicName: options.Topic}
	return consumer, nil

}

func (kc *kafkaClient) EarliestMessageID() MessageID {
	return &kafkaID{messageID: &sarama.ConsumerMessage{Offset: sarama.OffsetNewest}}
}

func (kc *kafkaClient) StringToMsgID(id string) (MessageID, error) {
	offset, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		return nil, err
	}
	return &kafkaID{messageID: &sarama.ConsumerMessage{Offset: offset}}, nil
}

func (kc *kafkaClient) BytesToMsgID(id []byte) (MessageID, error) {
	offset, err := DeserializeRmqID(id)
	if err != nil {
		return nil, err
	}
	return &kafkaID{messageID: &sarama.ConsumerMessage{Offset: offset}}, nil
}

func (kc *kafkaClient) Close() {
	kc.client.Close()
}
