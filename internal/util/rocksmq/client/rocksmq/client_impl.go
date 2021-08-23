// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package rocksmq

import (
	"context"
	"reflect"

	"github.com/milvus-io/milvus/internal/log"
	server "github.com/milvus-io/milvus/internal/util/rocksmq/server/rocksmq"
)

type client struct {
	server          RocksMQ
	producerOptions []ProducerOptions
	consumerOptions []ConsumerOptions
	context         context.Context
	cancel          context.CancelFunc
}

func newClient(options ClientOptions) (*client, error) {
	if options.Server == nil {
		return nil, newError(InvalidConfiguration, "Server is nil")
	}

	ctx, cancel := context.WithCancel(context.Background())
	c := &client{
		server:          options.Server,
		producerOptions: []ProducerOptions{},
		context:         ctx,
		cancel:          cancel,
	}
	return c, nil
}

func (c *client) CreateProducer(options ProducerOptions) (Producer, error) {
	// Create a producer
	producer, err := newProducer(c, options)
	if err != nil {
		return nil, err
	}

	if reflect.ValueOf(c.server).IsNil() {
		return nil, newError(0, "rmq server is nil")
	}
	// Create a topic in rocksmq, ignore if topic exists
	err = c.server.CreateTopic(options.Topic)
	if err != nil {
		return nil, err
	}
	c.producerOptions = append(c.producerOptions, options)

	return producer, nil
}

func (c *client) Subscribe(options ConsumerOptions) (Consumer, error) {
	// Create a consumer
	if reflect.ValueOf(c.server).IsNil() {
		return nil, newError(0, "rmq server is nil")
	}
	if exist, con := c.server.ExistConsumerGroup(options.Topic, options.SubscriptionName); exist {
		log.Debug("EXISTED")
		consumer, err := newConsumer1(c, options, con.MsgMutex)
		if err != nil {
			return nil, err
		}
		go consume(c.context, consumer)
		return consumer, nil
	}
	consumer, err := newConsumer(c, options)
	if err != nil {
		return nil, err
	}

	// Create a consumergroup in rocksmq, raise error if consumergroup exists
	err = c.server.CreateConsumerGroup(options.Topic, options.SubscriptionName)
	if err != nil {
		return nil, err
	}

	// Register self in rocksmq server
	cons := &server.Consumer{
		Topic:     consumer.topic,
		GroupName: consumer.consumerName,
		MsgMutex:  consumer.msgMutex,
	}
	c.server.RegisterConsumer(cons)

	// Take messages from RocksDB and put it into consumer.Chan(),
	// trigger by consumer.MsgMutex which trigger by producer
	go consume(c.context, consumer)
	c.consumerOptions = append(c.consumerOptions, options)

	return consumer, nil
}

func consume(ctx context.Context, consumer *consumer) {
	for {
		select {
		case <-ctx.Done():
			log.Debug("client finished")
			return
		case _, ok := <-consumer.MsgMutex():
			if !ok {
				// consumer MsgMutex closed, goroutine exit
				log.Debug("consumer MsgMutex closed")
				return
			}

			for {
				msg, err := consumer.client.server.Consume(consumer.topic, consumer.consumerName, 1)
				if err != nil {
					log.Debug("Consumer's goroutine cannot consume from (" + consumer.topic +
						"," + consumer.consumerName + "): " + err.Error())
					break
				}

				if len(msg) != 1 {
					//log.Debug("Consumer's goroutine cannot consume from (" + consumer.topic +
					//	"," + consumer.consumerName + "): message len(" + strconv.Itoa(len(msg)) +
					//	") is not 1")
					break
				}

				consumer.messageCh <- ConsumerMessage{
					MsgID:   msg[0].MsgID,
					Payload: msg[0].Payload,
					Topic:   consumer.Topic(),
				}
			}
		}
	}
}

func (c *client) Close() {
	// TODO: free resources
	for _, opt := range c.consumerOptions {
		log.Debug("Close" + opt.Topic + "+" + opt.SubscriptionName)
		_ = c.server.DestroyConsumerGroup(opt.Topic, opt.SubscriptionName)
		//TODO(yukun): Should topic be closed?
		_ = c.server.DestroyTopic(opt.Topic)
	}
	c.cancel()
}
