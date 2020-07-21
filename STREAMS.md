# Streams

To keep dependencies less and allow users to choose what they really need,
instead of adding different stream implementations directly, they are provided
as snippets here.


## Kafka

> Note: This implementation is based on librdkafka and adds a CGo dependency.

```go
package stream

import (
	"context"
	"sync"
	"time"

	"github.com/spy16/fusion"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

// Kafka implements a kafka backed stream using librdkafka Go wrapper. 
// This implementation uses manual offset commit to ensure message is
// handled by readFn at-least once.
// Refer https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
type Kafka struct {
	// Config is the librdkafka config map.
	Consumer *kafka.Consumer
}

// Read reads a message from Kafka, calls readFn with it and commits if it
// succeeds.
func (k *Kafka) Read(ctx context.Context, readFn fusion.ReadFn) error {
	kafkaMsg, err := k.Consumer.ReadMessage(k.ReadTimeout)
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
		_, _ = k.Consumer.CommitMessage(kafkaMsg)
	}
	return nil
}

// Close closes the underlying consumer.
func (k *Kafka) Close() error { return k.consumer.Close() }
```