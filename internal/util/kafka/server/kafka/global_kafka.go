package kafka

import "sync"

var KC *kafkaClient
var once sync.Once

func InitKafka(broker []string) error {
	var err error
	once.Do(func() {
		KC, err = NewKafkaClient(broker)
		if err != nil {
			panic(err)
		}
	})

	return err
}

func CloseKafka() {
	if KC != nil && KC.c != nil {
		KC.c.Close()
	}
}
