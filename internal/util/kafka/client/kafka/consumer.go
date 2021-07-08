package kafka

import (
	server "github.com/milvus-io/milvus/internal/util/kafka/server/kafka"
)

type SubscriptionInitialPosition int
type Offset = server.Offset

const (
	SubscriptionPositionLatest SubscriptionInitialPosition = iota
	SubscriptionPositionEarliest
)

func EarliestMessageID(topicName string) Offset {
	offset, _ := server.KC.EarliestMessageID(topicName)
	return offset
}

type ConsumerOptions struct {
	// The topic that this consumer will subscribe on
	Topic string

	// The subscription name for this consumer
	SubscriptionName string

	// InitialPosition at which the cursor will be set when subscribe
	// Default is `Latest`
	SubscriptionInitialPosition

	// Message for this consumer
	// When a message is received, it will be pushed to this channel for consumption
	MessageChannel chan ConsumerMessage
}

type ConsumerMessage struct {
	Consumer
	MsgID   Offset
	Topic   string
	Payload []byte
}

type Consumer interface {
	// returns the subscription for the consumer
	Subscription() string

	// returns the topic for the consumer
	Topic() string

	// Signal channel
	MsgMutex() chan struct{}

	// Message channel
	Chan() <-chan ConsumerMessage

	// Seek to the uniqueID position
	Seek(offset Offset) error //nolint:govet
}
