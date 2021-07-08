package kafka

import (
	server "github.com/milvus-io/milvus/internal/util/kafka/server/kafka"
)

type Kafka = server.Kafka

func NewClient(options ClientOptions) (Client, error) {
	options.Server = server.KC
	return NewClient(options)
}

type ClientOptions struct {
	Server Kafka
}

type Client interface {
	// Create a producer instance
	CreateProducer(options ProducerOptions) (Producer, error)

	// Create a consumer instance and subscribe a topic
	Subscribe(options ConsumerOptions) (Consumer, error)

	// Close the client and free associated resources
	Close()
}
