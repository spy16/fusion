package stream

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/spy16/fusion"
)

// Kafka implements fusion stream using the Kafka system as the backend with
// support for consumer groups. This implementation uses manual commit based
// on the Ack function to ensure at-least once delivery. Downstream consumers
// must take care of idempotency.
type Kafka struct {
	Workers  int           `json:"workers"`
	Topic    string        `json:"topic"`
	Brokers  []string      `json:"brokers"`
	GroupID  string        `json:"group_id"`
	MinBytes int           `json:"min_bytes"`
	MaxBytes int           `json:"max_bytes"`
	MaxWait  time.Duration `json:"max_wait"`

	log fusion.Log
}

// Out validates the Kafka config, creates a kafka consumer connection to the
// cluster and returns a fusion message channel where it streams the messages
// from Kafka.
func (ks Kafka) Out(ctx context.Context) (<-chan fusion.Msg, error) {
	ks.log = fusion.LogFrom(ctx)
	conf := kafka.ReaderConfig{
		Brokers:  ks.Brokers,
		Topic:    ks.Topic,
		GroupID:  ks.GroupID,
		MinBytes: ks.MinBytes,
		MaxBytes: ks.MaxBytes,
		MaxWait:  ks.MaxWait,
	}
	if err := conf.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	out := make(chan fusion.Msg)
	kafkaReader := kafka.NewReader(conf)
	stats := kafkaReader.Stats()

	ks.log(map[string]interface{}{
		"level":       "info",
		"message":     fmt.Sprintf("reader initialised to '%s'", stats.Topic),
		"current_lag": kafkaReader.Lag(),
	})

	go func() {
		defer close(out)

		wg := &sync.WaitGroup{}
		for i := 0; i < ks.Workers; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				ks.streamKafka(ctx, kafkaReader, out)
				ks.log(map[string]interface{}{
					"level":   "info",
					"message": fmt.Sprintf("worker %d exited", id),
				})
			}(i)
		}
		wg.Wait()
		ks.log(map[string]interface{}{
			"level":   "info",
			"message": "all workers exited, closing stream",
		})
	}()

	return out, nil
}

func (ks Kafka) streamKafka(ctx context.Context, kr *kafka.Reader, out chan<- fusion.Msg) {
	for ctx.Err() == nil {
		msg, err := kr.FetchMessage(ctx)
		if err != nil {
			ks.log(map[string]interface{}{
				"level":   "error",
				"message": fmt.Sprintf("reading from kafka failed: %v", err),
			})
			continue
		}

		fuMsg := fusion.Msg{
			Key:     msg.Key,
			Val:     msg.Value,
			Attribs: map[string]string{"topic": kr.Stats().Topic},
			Ack: func(err error) {
				if err != nil {
					ks.log(map[string]interface{}{
						"level":   "warn",
						"message": fmt.Sprintf("got error for message, will not commit: %v", err),
					})
					// do not acknowledge. rely on auto-commit false.
					return
				}
				_ = kr.CommitMessages(ctx, msg)
			},
		}

		select {
		case <-ctx.Done():
			// exit without acknowledgement.
			return
		case out <- fuMsg:
		}
	}
}
