package mqclient

import "github.com/Shopify/sarama"

type kafkaMessage struct {
	msg sarama.Message
}
