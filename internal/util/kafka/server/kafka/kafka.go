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

type RocksMQ interface {
	CreateTopic(topicName string) error
	DestroyTopic(topicName string) error
	CreateConsumerGroup(topicName string, groupName string) error
	DestroyConsumerGroup(topicName string, groupName string) error

	RegisterConsumer(consumer *Consumer)

	Produce(topicName string, messages []ProducerMessage) error
	Consume(topicName string, groupName string, n int) ([]ConsumerMessage, error)
	Seek(topicName string, groupName string, msgID Offset) error
	ExistConsumerGroup(topicName string, groupName string) (bool, *Consumer)

	Notify(topicName, groupName string)
}
