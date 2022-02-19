package kafka

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream/mqclient"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"math/rand"
	"os"
	"testing"
	"time"
)

var Params paramtable.BaseTable

func TestMain(m *testing.M) {
	Params.Init()
	exitCode := m.Run()
	os.Exit(exitCode)
}
func IntToBytes(n int) []byte {
	tmp := int32(n)
	bytesBuffer := bytes.NewBuffer([]byte{})
	binary.Write(bytesBuffer, common.Endian, tmp)
	return bytesBuffer.Bytes()
}
func BytesToInt(b []byte) int {
	bytesBuffer := bytes.NewBuffer(b)
	var tmp int32
	binary.Read(bytesBuffer, common.Endian, &tmp)
	return int(tmp)
}

func Produce(ctx context.Context, t *testing.T, kc *kafkaClient, topic string, arr []int) {
	producer, err := kc.CreateProducer(mqclient.ProducerOptions{Topic: topic})
	assert.Nil(t, err)
	assert.NotNil(t, producer)

	log.Info("Produce start")
	for _, v := range arr {
		msg := &mqclient.ProducerMessage{
			Payload:    IntToBytes(v),
			Properties: map[string]string{},
		}
		_, err = producer.Send(ctx, msg)
		assert.Nil(t, err)
		log.Info("Pub", zap.Any("SND", v))
	}
	log.Info("Produce done")
}
func VerifyMessage(t *testing.T, msg mqclient.Message) {
	pload := BytesToInt(msg.Payload())
	log.Info("RECV", zap.Any("v", pload))
	pm := msg.(*kafkaMessage)
	topic := pm.Topic()
	assert.NotEmpty(t, topic)
	log.Info("RECV", zap.Any("t", topic))
	prop := pm.Properties()
	log.Info("RECV", zap.Any("p", len(prop)))
}

// Consume1 will consume random messages and record the last MessageID it received
func Consume1(ctx context.Context, t *testing.T, kc *kafkaClient, topic string, subName string, c chan mqclient.MessageID, total *int) {
	consumer, err := kc.Subscribe(mqclient.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            subName,
		BufSize:                     1024,
		Type:                        mqclient.KeyShared,
		SubscriptionInitialPosition: mqclient.SubscriptionPositionEarliest,
	})
	assert.Nil(t, err)
	assert.NotNil(t, consumer)
	defer consumer.Close()

	log.Info("Consume1 start")

	// get random number between 1 ~ 5
	rand.Seed(time.Now().UnixNano())
	cnt := 1 + rand.Int()%5

	var msg mqclient.Message
	for i := 0; i < cnt; i++ {
		select {
		case <-ctx.Done():
			log.Info("Consume1 channel closed")
			return
		case msg = <-consumer.Chan():
			consumer.Ack(msg)
			VerifyMessage(t, msg)
			(*total)++
			//log.Debug("total", zap.Int("val", *total))
		}
	}
	c <- msg.ID()

	log.Info("Consume1 randomly RECV", zap.Any("number", cnt))
	log.Info("Consume1 done")
}

// Consume2 will consume messages from specified MessageID
func Consume2(ctx context.Context, t *testing.T, kc *kafkaClient, topic string, subName string, msgID mqclient.MessageID, total *int) {
	consumer, err := kc.Subscribe(mqclient.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            subName,
		BufSize:                     1024,
		Type:                        mqclient.KeyShared,
		SubscriptionInitialPosition: mqclient.SubscriptionPositionEarliest,
	})
	assert.Nil(t, err)
	assert.NotNil(t, consumer)
	defer consumer.Close()

	err = consumer.Seek(msgID, true)
	assert.Nil(t, err)

	// skip the last received message
	mm := <-consumer.Chan()
	consumer.Ack(mm)

	log.Info("Consume2 start")

	for {
		select {
		case <-ctx.Done():
			log.Info("Consume2 channel closed")
			return
		case msg := <-consumer.Chan():
			consumer.Ack(msg)
			VerifyMessage(t, msg)
			(*total)++
			//log.Debug("total", zap.Int("val", *total))
		}
	}
}

func Consume3(ctx context.Context, t *testing.T, kc *kafkaClient, topic string, subName string, total *int) {
	consumer, err := kc.Subscribe(mqclient.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            subName,
		BufSize:                     1024,
		Type:                        mqclient.KeyShared,
		SubscriptionInitialPosition: mqclient.SubscriptionPositionEarliest,
	})
	assert.Nil(t, err)
	assert.NotNil(t, consumer)
	defer consumer.Close()

	log.Info("Consume3 start")

	for {
		select {
		case <-ctx.Done():
			log.Info("Consume3 channel closed")
			return
		case msg := <-consumer.Chan():
			consumer.Ack(msg)
			VerifyMessage(t, msg)
			(*total)++
			//log.Debug("total", zap.Int("val", *total))
		}
	}
}

func TestKafkalient_Consume1(t *testing.T) {
	kafkaAddress, _ := Params.Load("_KafkaAddress")
	kc, err := GetKafkaClientInstance([]string{kafkaAddress}, NewKafkaConfig())
	defer kc.Close()
	assert.NoError(t, err)
	assert.NotNil(t, kc)
	rand.Seed(time.Now().UnixNano())

	topic := fmt.Sprintf("test-topic-%d", rand.Int())
	subName := fmt.Sprintf("test-subname-%d", rand.Int())
	arr := []int{111, 222, 333, 444, 555, 666, 777}
	c := make(chan mqclient.MessageID, 1)

	ctx, cancel := context.WithCancel(context.Background())

	var total1 int
	var total2 int
	var total3 int

	// launch produce
	Produce(ctx, t, kc, topic, arr)
	time.Sleep(100 * time.Millisecond)

	// launch consume1
	ctx1, cancel1 := context.WithTimeout(ctx, 2*time.Second)
	defer cancel1()
	Consume1(ctx1, t, kc, topic, subName, c, &total1)

	// record the last received message id
	lastMsgID := <-c
	log.Info("msg", zap.Any("lastMsgID", lastMsgID))

	// launch consume2
	ctx2, cancel2 := context.WithTimeout(ctx, 2*time.Second)
	defer cancel2()
	Consume2(ctx2, t, kc, topic, subName, lastMsgID, &total2)

	// launch consume3
	ctx3, cancel3 := context.WithTimeout(ctx, 2*time.Second)
	defer cancel3()
	Consume3(ctx3, t, kc, topic, subName, &total3)

	// stop Consume2
	cancel()
	assert.Equal(t, len(arr), total1+total2)
	assert.Equal(t, len(arr), total3)

	log.Info("main done")
}

func Consume21(ctx context.Context, t *testing.T, kc *kafkaClient, topic string, subName string, c chan mqclient.MessageID, total *int) {
	consumer, err := sarama.NewConsumer(kc.broker, NewKafkaConfig())
	assert.Nil(t, err)
	assert.NotNil(t, consumer)
	defer consumer.Close()
	partitionConsumer, err := consumer.ConsumePartition(topic, 0, 0)
	assert.Nil(t, err)
	defer partitionConsumer.Close()
	log.Info("Consume1 start")

	// get random number between 1 ~ 5
	rand.Seed(time.Now().UnixNano())
	cnt := 1 + rand.Int()%5
	var msg *sarama.ConsumerMessage
	var ok bool
	for i := 0; i < cnt; i++ {
		select {
		case <-ctx.Done():
			log.Info("Consume1 channel closed")
			return
		case msg, ok = <-partitionConsumer.Messages():
			if !ok {
				return
			}
			v := BytesToInt(msg.Value)
			log.Info("RECV", zap.Any("v", v))
			(*total)++
		}
	}

	c <- &kafkaID{messageID: msg.Offset}

	log.Info("Consume1 randomly RECV", zap.Any("number", cnt))
	log.Info("Consume1 done")
}

// Consume2 will consume messages from specified MessageID
func Consume22(ctx context.Context, t *testing.T, kc *kafkaClient, topic string, subName string, msgID mqclient.MessageID, total *int) {
	consumer, err := sarama.NewConsumer(kc.broker, NewKafkaConfig())
	assert.Nil(t, err)
	assert.NotNil(t, consumer)
	defer consumer.Close()
	partitionConsumer, err := consumer.ConsumePartition(topic, 0, msgID.(*kafkaID).messageID)
	assert.Nil(t, err)
	defer partitionConsumer.Close()
	// skip the last received message
	_ = <-partitionConsumer.Messages()

	log.Info("Consume2 start")

	for {
		select {
		case <-ctx.Done():
			log.Info("Consume2 channel closed")
			return
		case msg, ok := <-partitionConsumer.Messages():
			if !ok {
				return
			}
			v := BytesToInt(msg.Value)
			log.Info("RECV", zap.Any("v", v))
			(*total)++
		}
	}

}

func Consume23(ctx context.Context, t *testing.T, kc *kafkaClient, topic string, subName string, total *int) {
	consumer, err := sarama.NewConsumer(kc.broker, NewKafkaConfig())
	assert.Nil(t, err)
	assert.NotNil(t, consumer)
	defer consumer.Close()
	partitionConsumer, err := consumer.ConsumePartition(topic, 0, 0)
	assert.Nil(t, err)
	defer partitionConsumer.Close()
	log.Info("Consume3 start")

	for {
		select {
		case <-ctx.Done():
			log.Info("Consume3 channel closed")
			return
		case msg, ok := <-partitionConsumer.Messages():
			if !ok {
				return
			}
			v := BytesToInt(msg.Value)
			log.Info("RECV", zap.Any("v", v))
			(*total)++
		}
	}
}

func TestKafkaClient_Consume2(t *testing.T) {
	kafkaAddress, _ := Params.Load("_KafkaAddress")
	kc, err := GetKafkaClientInstance([]string{kafkaAddress}, NewKafkaConfig())
	defer kc.Close()
	assert.NoError(t, err)
	assert.NotNil(t, kc)
	rand.Seed(time.Now().UnixNano())

	topic := fmt.Sprintf("test-topic-%d", rand.Int())
	subName := fmt.Sprintf("test-subname-%d", rand.Int())
	arr := []int{111, 222, 333, 444, 555, 666, 777}
	c := make(chan mqclient.MessageID, 1)

	ctx, cancel := context.WithCancel(context.Background())

	var total1 int
	var total2 int
	var total3 int

	// launch produce
	Produce(ctx, t, kc, topic, arr)
	time.Sleep(100 * time.Millisecond)

	// launch consume1
	ctx1, cancel1 := context.WithTimeout(ctx, 2*time.Second)
	defer cancel1()
	Consume21(ctx1, t, kc, topic, subName, c, &total1)

	// record the last received message id
	lastMsgID := <-c
	log.Info("msg", zap.Any("lastMsgID", lastMsgID))

	// launch consume2
	ctx2, cancel2 := context.WithTimeout(ctx, 2*time.Second)
	defer cancel2()
	Consume22(ctx2, t, kc, topic, subName, lastMsgID, &total2)

	// launch consume3
	ctx3, cancel3 := context.WithTimeout(ctx, 2*time.Second)
	defer cancel3()
	Consume23(ctx3, t, kc, topic, subName, &total3)

	// stop Consume2
	cancel()
	assert.Equal(t, len(arr), total1+total2)
	assert.Equal(t, len(arr), total3) //TODO:different pulsar

	log.Info("main done")
}

func TestKafkaClient_SeekPosition(t *testing.T) {
	kafkaAddress, _ := Params.Load("_KafkaAddress")
	kc, err := GetKafkaClientInstance([]string{kafkaAddress}, NewKafkaConfig())
	defer kc.Close()
	assert.NoError(t, err)
	assert.NotNil(t, kc)
	rand.Seed(time.Now().UnixNano())

	ctx := context.Background()
	topic := fmt.Sprintf("test-topic-%d", rand.Int())
	//subName := fmt.Sprintf("test-subname-%d", rand.Int())

	producer, err := kc.CreateProducer(mqclient.ProducerOptions{Topic: topic})
	assert.Nil(t, err)
	assert.NotNil(t, producer)

	log.Info("Produce start")
	ids := []mqclient.MessageID{}
	arr := []int{1, 2, 3}
	for _, v := range arr {
		msg := &mqclient.ProducerMessage{
			Payload:    IntToBytes(v),
			Properties: map[string]string{},
		}
		id, err := producer.Send(ctx, msg)
		ids = append(ids, id)
		assert.Nil(t, err)
	}

	log.Info("Produced")

	consumer, err := sarama.NewConsumer(kc.broker, NewKafkaConfig())
	assert.Nil(t, err)
	assert.NotNil(t, consumer)
	defer consumer.Close()
	seekID := ids[2].(*kafkaID).messageID

	partitionConsumer, err := consumer.ConsumePartition(topic, 0, seekID)
	assert.Nil(t, err)

	msgChan := partitionConsumer.Messages()

	select {
	case msg := <-msgChan:
		assert.Equal(t, 3, BytesToInt(msg.Value))
	case <-time.After(2 * time.Second):
		assert.FailNow(t, "should not wait")
	}
	partitionConsumer.Close()
	seekID = ids[1].(*kafkaID).messageID
	partitionConsumer, err = consumer.ConsumePartition(topic, 0, seekID)
	assert.Nil(t, err)

	msgChan = partitionConsumer.Messages()

	select {
	case msg := <-msgChan:

		assert.Equal(t, 2, BytesToInt(msg.Value))
	case <-time.After(2 * time.Second):
		assert.FailNow(t, "should not wait")
	}
	partitionConsumer.Close()
}

func TestKafkaClient_SeekLatest(t *testing.T) {
	kafkaAddress, _ := Params.Load("_KafkaAddress")
	kc, err := GetKafkaClientInstance([]string{kafkaAddress}, NewKafkaConfig())
	defer kc.Close()
	assert.NoError(t, err)
	assert.NotNil(t, kc)
	rand.Seed(time.Now().UnixNano())

	ctx := context.Background()
	topic := fmt.Sprintf("test-topic-%d", rand.Int())
	//subName := fmt.Sprintf("test-subname-%d", rand.Int())

	producer, err := kc.CreateProducer(mqclient.ProducerOptions{Topic: topic})
	assert.Nil(t, err)
	assert.NotNil(t, producer)

	log.Info("Produce start")

	arr := []int{1, 2, 3}
	for _, v := range arr {
		msg := &mqclient.ProducerMessage{
			Payload:    IntToBytes(v),
			Properties: map[string]string{},
		}
		_, err = producer.Send(ctx, msg)
		assert.Nil(t, err)
	}

	log.Info("Produced")

	consumer, err := sarama.NewConsumer(kc.broker, NewKafkaConfig())
	assert.Nil(t, err)
	assert.NotNil(t, consumer)
	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	assert.Nil(t, err)

	msgChan := partitionConsumer.Messages()

	loop := true
	for loop {
		select {
		case msg := <-msgChan:
			v := BytesToInt(msg.Value)
			log.Info("RECV", zap.Any("v", v))
			assert.Equal(t, v, 4)
			loop = false
		case <-time.After(2 * time.Second):
			log.Info("after 2 seconds")
			msg := &mqclient.ProducerMessage{
				Payload:    IntToBytes(4),
				Properties: map[string]string{},
			}
			_, err = producer.Send(ctx, msg)
			assert.Nil(t, err)
		}
	}
}

func TestKafkaClient_EarliestMessageID(t *testing.T) {
	kafkaAddress, _ := Params.Load("_KafkaAddress")
	client, _ := GetKafkaClientInstance([]string{kafkaAddress}, NewKafkaConfig())
	defer client.Close()

	mid := client.EarliestMessageID()
	assert.NotNil(t, mid)
}
