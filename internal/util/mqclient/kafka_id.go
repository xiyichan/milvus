package mqclient

import "gopkg.in/Shopify/sarama.v1"

type kafkaID struct {
	messageID sarama.Message
}
