package mqclient

import "gopkg.in/Shopify/sarama.v1"

type kafkaMessage struct {
	msg sarama.Message
}
