package kafka

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream/mqclient"
	"go.uber.org/zap"
	"math/rand"
	"sync"
)

type kafkaReader struct {
	addrs                   []string
	options                 mqclient.ReaderOptions
	config                  *sarama.Config
	startMessageIDInclusive bool
	subscriptionRolePrefix  string

	ctx context.Context
	kcg *kafkaConsumeGroup
}

type kafkaConsumeGroup struct {
	groupName  string
	cg         sarama.ConsumerGroup
	ctx        context.Context
	msgChannel chan mqclient.Message

	readyConsume    chan struct{}
	readyNext       chan bool
	consumeFinished chan struct{}
	isClosed        bool

	h    *handler
	once sync.Once
}

func newKafkaConsumeGroup(ctx context.Context, addrs []string, options mqclient.ReaderOptions) (*kafkaConsumeGroup, error) {
	NewKafkaConfig()
	if options.Name == "" {
		options.Name = fmt.Sprintf("%s_%d", options.Topic, rand.Int())
	}

	cg, err := sarama.NewConsumerGroup(addrs, options.Name, NewKafkaConfig())
	if err != nil {
		log.Error("kafka create consumer error", zap.Error(err))
		return nil, err
	}

	readyNext := make(chan bool, 1)
	readyConsume := make(chan struct{}, 1)
	consumeFinished := make(chan struct{}, 1)
	msgChannel := make(chan mqclient.Message, 1)

	// Track errors
	go func() {
		for err := range cg.Errors() {
			fmt.Println("kafkaReader cg ERROR", err)
		}
	}()

	kcgObj := &kafkaConsumeGroup{
		groupName:       options.Name,
		ctx:             ctx,
		cg:              cg,
		msgChannel:      msgChannel,
		readyNext:       readyNext,
		readyConsume:    readyConsume,
		consumeFinished: consumeFinished,
		isClosed:        false,
	}

	go kcgObj.init(options)
	<-kcgObj.readyConsume

	return kcgObj, nil
}

func (kcg *kafkaConsumeGroup) init(options mqclient.ReaderOptions) {
	offset := options.StartMessageID.(*kafkaID).messageID

	for {
		if kcg.isClosed {
			break
		}

		topics := []string{options.Topic}
		kcg.h = &handler{
			topicName:       options.Topic,
			msgChannel:      kcg.msgChannel,
			readyNext:       kcg.readyNext,
			readyConsume:    kcg.readyConsume,
			consumeFinished: kcg.consumeFinished,
			groupName:       kcg.groupName,
			hasNext:         true,
			offset:          offset,
		}
		fmt.Println("["+kcg.groupName+"]", "kafkaReader Init Consume start", offset)

		err := kcg.cg.Consume(kcg.ctx, topics, kcg.h)
		if err != nil {
			//clf: close will err
			log.Error("kafka reader consume err", zap.Any("topic", topics), zap.Error(err))
			break
		}

		fmt.Println("["+kcg.groupName+"]", "kafkaReader Init Consume end", offset)
	}
}

func (kcg *kafkaConsumeGroup) close() {
	fmt.Println("["+kcg.groupName+"]", "kafkaConsumeGroup close start")
	if kcg.isClosed {
		return
	}

	kcg.readyNext <- false
	<-kcg.consumeFinished

	err := kcg.cg.Close()
	if err != nil {
		log.Error("["+kcg.groupName+"] err", zap.Any("err", err))
	}

	close(kcg.msgChannel)
	close(kcg.readyConsume)
	close(kcg.readyNext)

	kcg.isClosed = true
	fmt.Println("["+kcg.groupName+"]", "kafkaConsumeGroup close end")
}

type handler struct {
	offset     int64
	topicName  string
	msgChannel chan mqclient.Message
	hasNext    bool
	groupName  string

	readyNext       chan bool
	readyConsume    chan struct{}
	consumeFinished chan struct{}
}

func (h *handler) Setup(sess sarama.ConsumerGroupSession) error {
	sess.ResetOffset(h.topicName, 0, h.offset, h.groupName)
	sess.MarkOffset(h.topicName, 0, h.offset, h.groupName)
	return nil
}

func (h *handler) Cleanup(sess sarama.ConsumerGroupSession) error {
	return nil

}

func (h *handler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	h.readyConsume <- struct{}{}
	highWaterMarkOffset := claim.HighWaterMarkOffset()

	for msg := range claim.Messages() {
		isReady := <-h.readyNext
		if !isReady {
			break
		}
		h.msgChannel <- &kafkaMessage{msg: msg}
		sess.MarkMessage(msg, "")

		if msg.Offset >= highWaterMarkOffset-1 {
			break
		}
	}

	h.hasNext = false
	h.consumeFinished <- struct{}{}
	return nil
}

func (kr *kafkaReader) init() error {
	var err error
	kr.kcg, err = newKafkaConsumeGroup(kr.ctx, kr.addrs, kr.options)
	return err
}

func (kr *kafkaReader) Topic() string {
	return kr.kcg.h.topicName
}

func (kr *kafkaReader) Next(ctx context.Context) (mqclient.Message, error) {
	kr.kcg.readyNext <- true
	select {
	case msg := <-kr.kcg.msgChannel:
		return msg, nil
	}
}

func (kr *kafkaReader) HasNext() bool {
	return kr.kcg.h.hasNext
}

func (kr *kafkaReader) Close() {
	kr.kcg.close()
}

func (kr *kafkaReader) Seek(id mqclient.MessageID) error {
	if kr.kcg != nil {
		fmt.Println("["+kr.kcg.groupName+"]", "reader seek close start===")
		kr.kcg.close()
	}

	kr.options.StartMessageID = id
	offset := id.(*kafkaID).messageID
	kr.options.Name = fmt.Sprintf("%s_reset_seek_%d", kr.options.Name, offset)

	err := kr.init()
	return err
}
