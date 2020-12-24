package main

import (
	"context"
	"fmt"
	"sync"

	"github.com/segmentio/kafka-go"

	"github.com/spy16/fusion"
)

type KafkaStream struct {
	Brokers []string      `json:"brokers"`
	Topic   string        `json:"topic"`
	GroupID string        `json:"group_id"`
	Workers int           `json:"workers"`
	Logger  fusion.Logger `json:"-"`
}

func (ks KafkaStream) Out(ctx context.Context) (<-chan fusion.Msg, error) {
	conf := kafka.ReaderConfig{
		Brokers: ks.Brokers,
		Topic:   ks.Topic,
		GroupID: ks.GroupID,
	}
	if err := conf.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	out := make(chan fusion.Msg)
	kafkaReader := kafka.NewReader(conf)

	go func() {
		defer close(out)

		wg := &sync.WaitGroup{}
		for i := 0; i < ks.Workers; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				ks.streamKafka(ctx, kafkaReader, out)
				ks.Logger.Infof("worker %d exited", id)
			}(i)
		}
		wg.Wait()
		ks.Logger.Infof("all workers exited, closing stream")
	}()

	return out, nil
}

func (ks KafkaStream) streamKafka(ctx context.Context, kr *kafka.Reader, out chan<- fusion.Msg) {
	for ctx.Err() == nil {
		ks.Logger.Infof("fetching message...")
		msg, err := kr.FetchMessage(ctx)
		if err != nil {
			ks.Logger.Errorf("reading from kafka failed: %v", err)
			continue
		}

		ks.Logger.Infof("message received.")
		fuMsg := fusion.Msg{
			Key:     msg.Key,
			Val:     msg.Value,
			Attribs: map[string]string{"topic": kr.Stats().Topic},
			Ack: func(err error) {
				if err != nil {
					ks.Logger.Warnf("got error for message, will not commit: %v", err)
					// do not acknowledge. rely on auto-commit false.
					return
				}
				_ = kr.CommitMessages(ctx, msg)
			},
		}

		select {
		case <-ctx.Done():
			return
		case out <- fuMsg:
		}
	}
}
