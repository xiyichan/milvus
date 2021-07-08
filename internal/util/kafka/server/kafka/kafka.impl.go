package kafka

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/milvus-io/milvus/internal/kv"
	memkv "github.com/milvus-io/milvus/internal/kv/mem"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
	"sync"
	"time"
)

type Offset = typeutil.UniqueID

type kafkaClient struct {
	c         sarama.Client
	kv        kv.BaseKV
	channelMu sync.Map

	consumers sync.Map
}

func NewKafkaClient(brokers []string) (*kafkaClient, error) {

	config := sarama.NewConfig()
	config.Net.ReadTimeout = 180 * time.Second
	config.Version = sarama.V2_8_0_0

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
func (k *kafkaClient) DestroyConsumerGroup(groupID string) error {
	req := &sarama.DeleteGroupsRequest{}
	req.AddGroup(groupID)

	b, err := k.c.Broker(0)
	if err != nil {
		return nil
	}
	rep, err := b.DeleteGroups(req)
	if err != nil {
		return err
	}
	log.Debug("topic_errors:", zap.Any("topic_errors", rep.GroupErrorCodes))
	return nil
}

func (k *kafkaClient) CreateConsumerGroup(groupID string) error {
	cg, err := sarama.NewConsumerGroupFromClient(groupID, k.c)
	if err != nil {
		return err
	}
	return err
	err = cg.Close()
	if err != nil {
		return err
	}
	return nil
}

func (k *kafkaClient) ExistConsumerGroup(groupID string) (bool, *Consumer) {
	req := &sarama.DescribeGroupsRequest{}
	req.AddGroup(groupID)
	b, err := k.c.Broker(0)
	if err != nil {
		return false, nil
	}
	rep, err := b.DescribeGroups(req)
	if err != nil {
		return false, nil
	}
	log.Debug("topic_errors:", zap.Any("topic_errors", rep.Groups))
	if rep.Groups[0].State == "Dead" {
		return false, &Consumer{GroupName: groupID}
	}
	return true, nil
}

func (k *kafkaClient) Produce(topicName string, messages []ProducerMessage) error {
	producer, err := sarama.NewSyncProducerFromClient(k.c)
	if err != nil {
		return nil
	}

	for i := 0; i < len(messages); i++ {
		msg := &sarama.ProducerMessage{Topic: topicName, Value: sarama.ByteEncoder(messages[i].Payload)}
		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			return nil
		}
		log.Debug(fmt.Sprintf("> message sent to partition %d at offset %d\n", partition, offset))
	}

	err = producer.Close()
	if err != nil {
		return err
	}
	return nil
}

type exampleConsumerGroupHandler struct{}

func (exampleConsumerGroupHandler) Setup(sess sarama.ConsumerGroupSession) error {
	sess.ResetOffset("test_topic_1", 0, 3, "modified_meta")
	return nil
}
func (exampleConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil

}
func (h exampleConsumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	fmt.Println(claim.InitialOffset())

	for msg := range claim.Messages() {
		//fmt.Printf("Message topic:%q partition:%d offset:%d\n", msg.Topic, msg.Partition, msg.Offset)
		sess.MarkMessage(msg, "")
		//fmt.Println(string(msg.Value))

	}
	return nil
}

func (k *kafkaClient) Consume(topicName string, groupID string) ([]ConsumerMessage, error) {
	ll, ok := k.channelMu.Load(topicName)
	if !ok {
		return nil, fmt.Errorf("topic name = %s not exist", topicName)
	}
	lock, ok := ll.(*sync.Mutex)
	if !ok {
		return nil, fmt.Errorf("get mutex failed, topic name = %s", topicName)
	}
	lock.Lock()
	defer lock.Unlock()

	group, err := sarama.NewConsumerGroupFromClient(groupID, k.c)
	if err != nil {
		panic(err)
	}
	defer func() { _ = group.Close() }()

	// Track errors
	go func() {
		for err := range group.Errors() {
			fmt.Println("ERROR", err)
		}
	}()

	// Iterate over consumer sessions.
	ctx := context.Background()
	for {
		topics := []string{topicName}
		handler := exampleConsumerGroupHandler{}

		// `Consume` should be called inside an infinite loop, when a
		// server-side rebalance happens, the consumer session will need to be
		// recreated to get the new claims
		err := group.Consume(ctx, topics, handler)
		if err != nil {
			panic(err)
		}
	}
}

func (k *kafkaClient) RegisterConsumer(consumer *Consumer) {
	if vals, ok := k.consumers.Load(consumer.Topic); ok {
		for _, v := range vals.([]*Consumer) {
			if v.GroupName == consumer.GroupName {
				return
			}
		}
		consumers := vals.([]*Consumer)
		consumers = append(consumers, consumer)
		k.consumers.Store(consumer.Topic, consumers)
	} else {
		consumers := make([]*Consumer, 1)
		consumers[0] = consumer
		k.consumers.Store(consumer.Topic, consumers)
	}
}

func (k *kafkaClient) EarliestMessageID(topicName string) (Offset, error) {
	offset, err := k.c.GetOffset(topicName, 0, sarama.OffsetNewest)
	return offset, err
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
func (k *kafkaClient) Notify(topicName, groupName string) {
	if vals, ok := k.consumers.Load(topicName); ok {
		for _, v := range vals.([]*Consumer) {
			if v.GroupName == groupName {
				select {
				case v.MsgMutex <- struct{}{}:
					continue
				default:
					continue
				}
			}
		}
	}
}
