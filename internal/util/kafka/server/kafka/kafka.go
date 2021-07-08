package kafka

type ProducerMessage struct {
	Payload []byte
}

type Consumer struct {
	Topic     string
	GroupName string
	MsgMutex  chan struct{}
}

type ConsumerMessage struct {
	MsgID   Offset
	Payload []byte
}

type Kafka interface {
	CreateTopic(topicName string) error
	DestroyTopic(topicName string) error
	CreateConsumerGroup(groupID string) error
	DestroyConsumerGroup(groupID string) error
	ExistConsumerGroup(groupID string) (bool, *Consumer)
	RegisterConsumer(consumer *Consumer)

	Produce(topicName string, messages []ProducerMessage) error
	Consume(topicName string, groupName string) ([]ConsumerMessage, error)
	Seek(topicName string, groupName string, msgID Offset) error
	EarliestMessageID(topicName string) (Offset, error)

	Notify(topicName, groupName string)
}
