# Streams


## Kafka

```go
package stream

import (
	"context"
	"sync"
	"time"

	"github.com/spy16/fusion"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

// Kafka implements a stream for a kafka topic. This implementation uses
// manual offset commit to ensure message is handled by readFn at-least
// once.
type Kafka struct {
	// Config is the librdkafka config map.
	// Refer https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
	Config kafka.ConfigMap

	// ReadTimeout to be used during every ReadMessage().
	ReadTimeout time.Duration

	// Topic name to subscribe for.
	Topic string

	once     sync.Once
	consumer *kafka.Consumer
}

// Read reads a message from Kafka, calls readFn with it and commits if it
// succeeds.
func (k *Kafka) Read(ctx context.Context, readFn fusion.ReadFn) error {
	err := k.init()
	if err != nil {
		return err
	}

	kafkaMsg, err := k.consumer.ReadMessage(k.ReadTimeout)
	if err != nil {
		e, ok := err.(kafka.Error)
		if ok && e.Code() == kafka.ErrTimedOut {
			return fusion.ErrNoMessage
		}
		return err
	}

	err = readFn(ctx, fusion.Message{
		Key: kafkaMsg.Key,
		Val: kafkaMsg.Value,
	})
	if err == nil {
		_, _ = k.consumer.CommitMessage(kafkaMsg)
	}
	return nil
}

// Close closes the underlying consumer.
func (k *Kafka) Close() error { return k.consumer.Close() }

func (k *Kafka) init() (err error) {
	k.once.Do(func() {
		if k.ReadTimeout == 0 {
			k.ReadTimeout = 100 * time.Millisecond
		}
		k.Config["enable.auto.commit"] = false
		k.consumer, err = kafka.NewConsumer(&k.Config)
		if err == nil {
			err = k.consumer.Subscribe(k.Topic, nil)
		}
	})
	return err
}
```