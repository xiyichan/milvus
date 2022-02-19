package kafka

import (
	"context"
	"github.com/milvus-io/milvus/internal/msgstream/mqclient"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPulsarProducer(t *testing.T) {
	kafkaAddress, _ := Params.Load("_KafkaAddress")
	kc, err := GetKafkaClientInstance([]string{kafkaAddress}, NewKafkaConfig())
	assert.Nil(t, err)
	defer kc.Close()
	assert.NoError(t, err)
	assert.NotNil(t, kc)

	topic := "TEST"
	producer, err := kc.CreateProducer(mqclient.ProducerOptions{Topic: topic})
	assert.Nil(t, err)
	assert.NotNil(t, producer)

	pulsarProd := producer.(*kafkaProducer)
	assert.Equal(t, pulsarProd.Topic(), topic)

	msg := &mqclient.ProducerMessage{
		Payload:    []byte{},
		Properties: map[string]string{},
	}
	_, err = producer.Send(context.TODO(), msg)
	assert.Nil(t, err)

	pulsarProd.Close()
}
