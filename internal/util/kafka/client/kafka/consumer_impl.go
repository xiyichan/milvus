package kafka

type consumer struct {
	topic        string
	client       *client
	consumerName string
	options      ConsumerOptions

	msgMutex  chan struct{}
	messageCh chan ConsumerMessage
}

func newConsumer(c *client, options ConsumerOptions) (*consumer, error) {
	if c == nil {
		return nil, newError(InvalidConfiguration, "client is nil")
	}

	if options.Topic == "" {
		return nil, newError(InvalidConfiguration, "Topic is empty")
	}

	if options.SubscriptionName == "" {
		return nil, newError(InvalidConfiguration, "SubscriptionName is empty")
	}

	messageCh := options.MessageChannel
	if options.MessageChannel == nil {
		messageCh = make(chan ConsumerMessage, 1)
	}

	return &consumer{
		topic:        options.Topic,
		client:       c,
		consumerName: options.SubscriptionName,
		options:      options,
		msgMutex:     make(chan struct{}, 1),
		messageCh:    messageCh,
	}, nil
}

func newConsumer1(c *client, options ConsumerOptions, msgMutex chan struct{}) (*consumer, error) {
	if c == nil {
		return nil, newError(InvalidConfiguration, "client is nil")
	}

	if options.Topic == "" {
		return nil, newError(InvalidConfiguration, "Topic is empty")
	}

	if options.SubscriptionName == "" {
		return nil, newError(InvalidConfiguration, "SubscriptionName is empty")
	}

	messageCh := options.MessageChannel
	if options.MessageChannel == nil {
		messageCh = make(chan ConsumerMessage, 1)
	}

	return &consumer{
		topic:        options.Topic,
		client:       c,
		consumerName: options.SubscriptionName,
		options:      options,
		msgMutex:     msgMutex,
		messageCh:    messageCh,
	}, nil
}

func (c *consumer) Subscription() string {
	return c.consumerName
}

func (c *consumer) Topic() string {
	return c.topic
}

func (c *consumer) MsgMutex() chan struct{} {
	return c.msgMutex
}

func (c *consumer) Chan() <-chan ConsumerMessage {
	return c.messageCh
}

func (c *consumer) Seek(id Offset) error { //nolint:govet
	err := c.client.server.Seek(c.topic, c.consumerName, id)
	if err != nil {
		return err
	}
	c.client.server.Notify(c.topic, c.consumerName)
	return nil
}
