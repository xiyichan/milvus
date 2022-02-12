package kafka

import (
	"errors"
	sarama "github.com/Shopify/sarama"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream/mqclient"
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
	kafkaOnce.Do(func() {
		log.Info("kafka broker", zap.Any("broker", broker))
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

func NewKafkaConfig() *sarama.Config {
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Producer.Return.Successes = true
	config.Consumer.Return.Errors = true
	return config
}

func (kc *kafkaClient) CreateProducer(options mqclient.ProducerOptions) (mqclient.Producer, error) {
	pp, err := sarama.NewSyncProducer(kc.broker, NewKafkaConfig())
	if err != nil {
		log.Error("kafka create sync producer , error", zap.Error(err))
		return nil, err
	}

	if pp == nil {
		return nil, errors.New("kafka is not ready, producer is nil")
	}
	producer := &kafkaProducer{p: pp, topic: options.Topic}
	return producer, nil
}

func (kc *kafkaClient) CreateReader(options mqclient.ReaderOptions) (mqclient.Reader, error) {
	g, err := sarama.NewConsumerGroup(kc.broker, options.Name, NewKafkaConfig())
	if err != nil {
		log.Error("kafka create consumer error", zap.Error(err))
		panic(err)
	}

	//cp, err := c.ConsumePartition(options.Topic, 0, options.StartMessageID.(*kafkaID).messageID)
	//reader := &kafkaReader{r: c, cp: cp, offset: options.StartMessageID.(*kafkaID).messageID, topicName: options.Topic}
	reader := &kafkaReader{
		cg:         g,
		readFlag:   true,
		closeCh:    make(chan struct{}),
		msgChannel: make(chan mqclient.Message),
		offset:     options.StartMessageID.(*kafkaID).messageID,
		topicName:  options.Topic}

	return reader, nil
}

func (kc *kafkaClient) Subscribe(options mqclient.ConsumerOptions) (mqclient.Consumer, error) {
	log.Info("kafka consumer name", zap.Any("name", options.SubscriptionName))
	c, err := sarama.NewConsumer(kc.broker, NewKafkaConfig())
	//g, err := sarama.NewConsumerGroup(kc.broker, options.SubscriptionName, config)
	if err != nil {
		log.Error("kafka create consumer error", zap.Error(err))
		panic(err)
	}

	consumer := &kafkaConsumer{
		c:         c,
		offset:    sarama.OffsetOldest,
		topicName: options.Topic,
		closeCh:   make(chan struct{}),
		groupID:   options.SubscriptionName}
	//consumer := &kafkaConsumer{c: kc.client, g: g, offset: sarama.OffsetOldest, topicName: options.Topic, closeCh: make(chan struct{}), groupID: options.SubscriptionName}
	return consumer, nil

}

func (kc *kafkaClient) EarliestMessageID() mqclient.MessageID {
	return &kafkaID{messageID: sarama.OffsetNewest}
}

func (kc *kafkaClient) StringToMsgID(id string) (mqclient.MessageID, error) {
	offset, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		return nil, err
	}

	return &kafkaID{messageID: offset}, nil
}

func (kc *kafkaClient) BytesToMsgID(id []byte) (mqclient.MessageID, error) {
	offset, err := DeserializeKafkaID(id)
	if err != nil {
		return nil, err
	}

	return &kafkaID{messageID: offset}, nil
}

func (kc *kafkaClient) Close() {
	kc.client.Close()
}
