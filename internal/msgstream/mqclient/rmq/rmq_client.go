// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rmq

import (
	"errors"
	"github.com/milvus-io/milvus/internal/msgstream/mqclient"
	rocksmq2 "github.com/milvus-io/milvus/internal/msgstream/mqimpl/rocksmq/client/rocksmq"
	"strconv"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
)

// rmqClient contains a rocksmq client
type rmqClient struct {
	client rocksmq2.Client
}

// NewRmqClient returns a new rmqClient object
func NewRmqClient(opts rocksmq2.ClientOptions) (*rmqClient, error) {
	c, err := rocksmq2.NewClient(opts)
	if err != nil {
		log.Error("Failed to set rmq client: ", zap.Error(err))
		return nil, err
	}
	return &rmqClient{client: c}, nil
}

// CreateProducer creates a producer for rocksmq client
func (rc *rmqClient) CreateProducer(options mqclient.ProducerOptions) (mqclient.Producer, error) {
	rmqOpts := rocksmq2.ProducerOptions{Topic: options.Topic}
	pp, err := rc.client.CreateProducer(rmqOpts)
	if err != nil {
		return nil, err
	}
	rp := rmqProducer{p: pp}
	return &rp, nil
}

// CreateReader creates a rocksmq reader from reader options
func (rc *rmqClient) CreateReader(options mqclient.ReaderOptions) (mqclient.Reader, error) {
	opts := rocksmq2.ReaderOptions{
		Topic:                   options.Topic,
		StartMessageID:          options.StartMessageID.(*rmqID).messageID,
		StartMessageIDInclusive: options.StartMessageIDInclusive,
		SubscriptionRolePrefix:  options.SubscriptionRolePrefix,
	}
	pr, err := rc.client.CreateReader(opts)
	if err != nil {
		return nil, err
	}
	if pr == nil {
		return nil, errors.New("pulsar is not ready, producer is nil")
	}
	reader := &rmqReader{r: pr}
	return reader, nil
}

// Subscribe subscribes a consumer in rmq client
func (rc *rmqClient) Subscribe(options mqclient.ConsumerOptions) (mqclient.Consumer, error) {
	receiveChannel := make(chan rocksmq2.Message, options.BufSize)

	cli, err := rc.client.Subscribe(rocksmq2.ConsumerOptions{
		Topic:                       options.Topic,
		SubscriptionName:            options.SubscriptionName,
		MessageChannel:              receiveChannel,
		SubscriptionInitialPosition: rocksmq2.SubscriptionInitialPosition(options.SubscriptionInitialPosition),
	})
	if err != nil {
		return nil, err
	}

	rConsumer := &RmqConsumer{c: cli, closeCh: make(chan struct{})}

	return rConsumer, nil
}

// EarliestMessageID returns the earliest message ID for rmq client
func (rc *rmqClient) EarliestMessageID() mqclient.MessageID {
	rID := rocksmq2.EarliestMessageID()
	return &rmqID{messageID: rID}
}

// StringToMsgID converts string id to MessageID
func (rc *rmqClient) StringToMsgID(id string) (mqclient.MessageID, error) {
	rID, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		return nil, err
	}
	return &rmqID{messageID: rID}, nil
}

// BytesToMsgID converts a byte array to messageID
func (rc *rmqClient) BytesToMsgID(id []byte) (mqclient.MessageID, error) {
	rID, err := DeserializeRmqID(id)
	if err != nil {
		return nil, err
	}
	return &rmqID{messageID: rID}, nil
}

func (rc *rmqClient) Close() {
	rc.client.Close()
}
