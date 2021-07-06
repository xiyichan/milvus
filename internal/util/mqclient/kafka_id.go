package mqclient

import "github.com/Shopify/sarama"

type kafkaID struct {
	messageID sarama.Message
}
