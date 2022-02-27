package kafka

import (
	"context"
	"fmt"
	"github.com/milvus-io/milvus/internal/msgstream/mqclient"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"testing"
	"time"
)

func TestKafkaReader(t *testing.T) {
	ctx := context.Background()
	kafkaAddress, _ := Params.Load("_KafkaAddress")
	kc, err := NewKafkaClient([]string{kafkaAddress}, NewKafkaConfig(), Ctx)
	assert.Nil(t, err)
	defer kc.Close()

	rand.Seed(time.Now().UnixNano())
	topic := fmt.Sprintf("test-%d", rand.Int())

	producer, err := kc.CreateProducer(mqclient.ProducerOptions{Topic: topic})
	assert.Nil(t, err)
	assert.NotNil(t, producer)

	defer producer.Close()

	const N = 10
	var seekID mqclient.MessageID
	for i := 0; i < N; i++ {
		msg := &mqclient.ProducerMessage{
			Payload:    []byte(fmt.Sprintf("helloworld-%d", i)),
			Properties: map[string]string{},
		}

		id, err := producer.Send(ctx, msg)
		assert.Nil(t, err)
		if i == 4 {
			seekID = &kafkaID{messageID: id.(*kafkaID).messageID}
		}
	}

	reader, err := kc.CreateReader(mqclient.ReaderOptions{
		Topic:          topic,
		Name:           "reader_test",
		StartMessageID: kc.EarliestMessageID(),
	})
	assert.Nil(t, err)
	assert.NotNil(t, reader)
	defer reader.Close()

	str := reader.Topic()
	assert.NotNil(t, str)
	for i := 0; i < N; i++ {
		revMsg, err := reader.Next(ctx)
		assert.Nil(t, err)
		assert.NotNil(t, revMsg)
	}
	fmt.Println("22222", seekID)
	readerOfStartMessageID, err := kc.CreateReader(mqclient.ReaderOptions{
		Topic:                   topic,
		Name:                    "reader_of_test",
		StartMessageID:          seekID,
		StartMessageIDInclusive: true,
	})
	assert.Nil(t, err)
	defer readerOfStartMessageID.Close()

	for i := 4; i < N; i++ {
		assert.True(t, readerOfStartMessageID.HasNext())
		revMsg, err := readerOfStartMessageID.Next(ctx)
		assert.Nil(t, err)
		assert.NotNil(t, revMsg)
	}

	readerOfSeek, err := kc.CreateReader(mqclient.ReaderOptions{
		Topic:          topic,
		Name:           "reader_of_seek",
		StartMessageID: kc.EarliestMessageID(),
	})
	assert.Nil(t, err)
	defer readerOfSeek.Close()

	fmt.Println(seekID, "=========== 44444")

	err = reader.Seek(seekID)
	fmt.Println(seekID, "=========== 44444 seek")

	assert.Nil(t, err)
	for readerOfSeek.HasNext() {
		assert.True(t, readerOfSeek.HasNext())
		revMsg, err := readerOfSeek.Next(ctx)
		assert.Nil(t, err)
		assert.NotNil(t, revMsg)
	}

	fmt.Println(seekID, "=========== 44444 finished")

}
