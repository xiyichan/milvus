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
	broker []string
}

var kc *kafkaClient
var kafkaOnce sync.Once

func GetKafkaClientInstance(broker []string, opts *sarama.Config) (*kafkaClient, error) {
	once.Do(func() {
		//broker = []string{"47.106.76.166:9092"}
		c, err := sarama.NewClient(broker, opts)
		if err != nil {
			log.Error("Set kafka client failed, error", zap.Error(err))
			return
		}
		cli := &kafkaClient{client: c, broker: broker}
		kc = cli
	})
	return kc, nil
}

func (kc *kafkaClient) CreateProducer(options ProducerOptions) (Producer, error) {
	c := kc.client
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Version = sarama.V2_8_0_0
	pp, err := sarama.NewSyncProducer(kc.broker, config)

	if err != nil {
		log.Error("kafka create sync producer , error", zap.Error(err))
		return nil, err
	}
	if pp == nil {
		return nil, errors.New("kafka is not ready, producer is nil")
	}
	producer := &kafkaProducer{p: pp, c: c, topic: options.Topic}
	return producer, nil
}

func (kc *kafkaClient) Subscribe(options ConsumerOptions) (Consumer, error) {
	log.Info("kafka consumer name", zap.Any("name", options.SubscriptionName))
	c := kc.client
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Producer.Return.Successes = true
	//group, err := sarama.NewConsumerGroupFromClient(options.SubscriptionName, c)
	group, err := sarama.NewConsumerGroup(kc.broker, options.SubscriptionName, config)

	if err != nil {
		log.Error("kafka create sync producer , error", zap.Error(err))
		panic(err)
	}
	//defer func() { _ = group.Close() }()

	// Track errors
	go func() {
		for err = range group.Errors() {
			log.Error("kafka create sync producer , error", zap.Error(err))
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

	consumer := &kafkaConsumer{g: group, c: c, topicName: options.Topic, groupID: options.SubscriptionName}
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
