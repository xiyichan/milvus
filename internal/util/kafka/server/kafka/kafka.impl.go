package kafka

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/milvus-io/milvus/internal/kv"
	memkv "github.com/milvus-io/milvus/internal/kv/mem"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
	"time"
)

type Offset = typeutil.UniqueID

type kafkaClient struct {
	c  sarama.Client
	kv kv.BaseKV
}

func NewKafkaClient(brokers []string) (*kafkaClient, error) {

	config := sarama.NewConfig()
	config.Net.ReadTimeout = 180 * time.Second
	config.Consumer.Return.Errors = true
	//config.Consumer.Offsets.AutoCommit.Enable=false

	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		return nil, err
	}
	mkv := memkv.NewMemoryKV()
	kc := &kafkaClient{
		c:  client,
		kv: mkv,
	}
	return kc, nil
}

func (k *kafkaClient) CreateTopic(topicName string) error {
	retention := "-1"
	req := &sarama.CreateTopicsRequest{
		TopicDetails: map[string]*sarama.TopicDetail{
			topicName: {
				NumPartitions:     -1,
				ReplicationFactor: -1,
				ReplicaAssignment: map[int32][]int32{
					0: {0, 1, 2},
				},
				ConfigEntries: map[string]*string{
					"retention.ms": &retention,
				},
			},
		},
		Timeout: 100 * time.Millisecond,
	}

	// default broker 0
	b, err := k.c.Broker(0)
	if err != nil {
		return nil
	}
	rep, err := b.CreateTopics(req)
	if err != nil {
		return err
	}
	log.Debug("topic_errors:", zap.Any("topic_errors", rep.TopicErrors))
	return nil
}

func (k *kafkaClient) DestroyTopic(topicName string) error {
	req := &sarama.DeleteTopicsRequest{
		Version: 1,
		Topics:  []string{topicName},
		Timeout: 100 * time.Millisecond,
	}
	b, err := k.c.Broker(0)
	if err != nil {
		return nil
	}
	rep, err := b.DeleteTopics(req)
	if err != nil {
		return err
	}
	log.Debug("topic_errors:", zap.Any("topic_errors", rep.TopicErrorCodes))
	return nil
}
func (k *kafkaClient) CreateConsumerGroup(topic string, groupID string) error {

	return nil
}
func (k *kafkaClient) Produce(topicName string, messages []ProducerMessage) error {
	producer, err := sarama.NewSyncProducerFromClient(k.c)
	if err != nil {
		return nil
	}
	//defer func() {
	//	if err := producer.Close(); err != nil {
	//		return
	//	}
	//}()
	defer producer.Close()
	for i := 0; i < len(messages); i++ {
		msg := &sarama.ProducerMessage{Topic: topicName, Value: sarama.ByteEncoder(messages[i].Payload)}
		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			return nil
		}
		log.Debug(fmt.Sprintf("> message sent to partition %d at offset %d\n", partition, offset))
	}
	return nil
}
func (k *kafkaClient) Consume(topicName string, groupID string) ([]ConsumerMessage, error) {

}

func (k *kafkaClient) Seek(topicName string, groupID string, offset Offset) error {
	om, err := sarama.NewOffsetManagerFromClient(groupID, k.c)
	if err != nil {
		return err
	}
	mp, err := om.ManagePartition(topicName, 0)
	if err != nil {
		return err
	}
	mp.ResetOffset(offset, "")

	return nil
}
