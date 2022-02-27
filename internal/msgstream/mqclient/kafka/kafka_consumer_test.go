package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestKafkaConsumer_Subscription(t *testing.T) {
	kafkaAddress, _ := Params.Load("_KafkaAddress")
	kc, err := NewKafkaClient([]string{kafkaAddress}, NewKafkaConfig(), Ctx)
	assert.Nil(t, err)
	defer kc.Close()

	consumer, err := sarama.NewConsumer(kc.broker, NewKafkaConfig())
	assert.Nil(t, err)
	assert.NotNil(t, consumer)
	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition("topic", 0, 0)
	defer partitionConsumer.Close()
	assert.NotNil(t, partitionConsumer)
}

func TestKafkaConsumer_Close(t *testing.T) {
	kafkaAddress, _ := Params.Load("_KafkaAddress")
	kc, err := NewKafkaClient([]string{kafkaAddress}, NewKafkaConfig(), Ctx)
	assert.Nil(t, err)
	defer kc.Close()

	consumer, err := sarama.NewConsumer(kc.broker, NewKafkaConfig())
	assert.Nil(t, err)
	assert.NotNil(t, consumer)
	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition("topic", 0, 0)
	defer partitionConsumer.Close()
	assert.NotNil(t, partitionConsumer)
}
