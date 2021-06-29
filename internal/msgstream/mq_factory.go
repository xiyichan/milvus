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

package msgstream

import (
	"context"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/mqclient"
	"github.com/milvus-io/milvus/internal/util/rocksmq/client/rocksmq"
	rocksmqserver "github.com/milvus-io/milvus/internal/util/rocksmq/server/rocksmq"
	"github.com/mitchellh/mapstructure"
	"gopkg.in/Shopify/sarama.v1"
)

type PmsFactory struct {
	dispatcherFactory ProtoUDFactory
	// the following members must be public, so that mapstructure.Decode() can access them
	PulsarAddress  string
	ReceiveBufSize int64
	PulsarBufSize  int64
}

func (f *PmsFactory) SetParams(params map[string]interface{}) error {
	err := mapstructure.Decode(params, f)
	if err != nil {
		return err
	}
	return nil
}

func (f *PmsFactory) NewMsgStream(ctx context.Context) (MsgStream, error) {
	pulsarClient, err := mqclient.GetPulsarClientInstance(pulsar.ClientOptions{URL: f.PulsarAddress})
	if err != nil {
		return nil, err
	}
	return NewMqMsgStream(ctx, f.ReceiveBufSize, f.PulsarBufSize, pulsarClient, f.dispatcherFactory.NewUnmarshalDispatcher())
}

func (f *PmsFactory) NewTtMsgStream(ctx context.Context) (MsgStream, error) {
	pulsarClient, err := mqclient.GetPulsarClientInstance(pulsar.ClientOptions{URL: f.PulsarAddress})
	if err != nil {
		return nil, err
	}
	return NewMqTtMsgStream(ctx, f.ReceiveBufSize, f.PulsarBufSize, pulsarClient, f.dispatcherFactory.NewUnmarshalDispatcher())
}

func (f *PmsFactory) NewQueryMsgStream(ctx context.Context) (MsgStream, error) {
	return f.NewMsgStream(ctx)
}

func NewPmsFactory() Factory {
	f := &PmsFactory{
		dispatcherFactory: ProtoUDFactory{},
		ReceiveBufSize:    64,
		PulsarBufSize:     64,
	}
	return f
}

type RmsFactory struct {
	dispatcherFactory ProtoUDFactory
	// the following members must be public, so that mapstructure.Decode() can access them
	ReceiveBufSize int64
	RmqBufSize     int64
}

func (f *RmsFactory) SetParams(params map[string]interface{}) error {
	err := mapstructure.Decode(params, f)
	if err != nil {
		return err
	}
	return nil
}

func (f *RmsFactory) NewMsgStream(ctx context.Context) (MsgStream, error) {
	rmqClient, err := mqclient.NewRmqClient(rocksmq.ClientOptions{Server: rocksmqserver.Rmq})
	if err != nil {
		return nil, err
	}
	return NewMqMsgStream(ctx, f.ReceiveBufSize, f.RmqBufSize, rmqClient, f.dispatcherFactory.NewUnmarshalDispatcher())
}

func (f *RmsFactory) NewTtMsgStream(ctx context.Context) (MsgStream, error) {
	rmqClient, err := mqclient.NewRmqClient(rocksmq.ClientOptions{Server: rocksmqserver.Rmq})
	if err != nil {
		return nil, err
	}
	return NewMqTtMsgStream(ctx, f.ReceiveBufSize, f.RmqBufSize, rmqClient, f.dispatcherFactory.NewUnmarshalDispatcher())
}

func (f *RmsFactory) NewQueryMsgStream(ctx context.Context) (MsgStream, error) {
	rmqClient, err := mqclient.NewRmqClient(rocksmq.ClientOptions{Server: rocksmqserver.Rmq})
	if err != nil {
		return nil, err
	}
	return NewMqMsgStream(ctx, f.ReceiveBufSize, f.RmqBufSize, rmqClient, f.dispatcherFactory.NewUnmarshalDispatcher())
}

func NewRmsFactory(rocksmqPath string) Factory {
	f := &RmsFactory{
		dispatcherFactory: ProtoUDFactory{},
		ReceiveBufSize:    1024,
		RmqBufSize:        1024,
	}

	log.Debug("RocksmqPath=" + rocksmqPath)
	rocksmqserver.InitRocksMQ(rocksmqPath)
	return f
}

type KmsFactory struct {
	dispatcherFactory ProtoUDFactory
	// the following members must be public, so that mapstructure.Decode() can access them
	KafkaAddress   []string
	ReceiveBufSize int64
	KafkaBufSize   int64
}

func (f *KmsFactory) SetParams(params map[string]interface{}) error {
	err := mapstructure.Decode(params, f)
	if err != nil {
		return err
	}
	return nil
}

func (f *KmsFactory) NewMsgStream(ctx context.Context) (MsgStream, error) {
	kafkaClient, err := mqclient.GetKafkaClientInstance(f.KafkaAddress, &sarama.Config{})
	if err != nil {
		return nil, err
	}
	return NewMqMsgStream(ctx, f.ReceiveBufSize, f.KafkaBufSize, kafkaClient, f.dispatcherFactory.NewUnmarshalDispatcher())
}

func (f *KmsFactory) NewTtMsgStream(ctx context.Context) (MsgStream, error) {
	kafkaClient, err := mqclient.GetKafkaClientInstance(f.KafkaAddress, &sarama.Config{})
	if err != nil {
		return nil, err
	}
	return NewMqTtMsgStream(ctx, f.ReceiveBufSize, f.KafkaBufSize, kafkaClient, f.dispatcherFactory.NewUnmarshalDispatcher())
}

func (f *KmsFactory) NewQueryMsgStream(ctx context.Context) (MsgStream, error) {
	return f.NewMsgStream(ctx)
}
func NewKmsFactory() Factory {
	f := &PmsFactory{
		dispatcherFactory: ProtoUDFactory{},
		ReceiveBufSize:    64,
		PulsarBufSize:     64,
	}
	return f
}
