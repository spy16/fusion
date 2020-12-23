// +build kafka

package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"time"

	"github.com/spy16/fusion"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

// KafkaConnect connects to the Kafka cluster with given configs and subscribes
// to the topic.
func KafkaConnect(topic string, configMap kafka.ConfigMap) (*KafkaStream, error) {
	cons, err := kafka.NewConsumer(sanitiseConfig(configMap))
	if err != nil {
		return nil, err
	}
	if err := cons.Subscribe(topic, nil); err != nil {
		_ = cons.Close()
		return nil, err
	}

	return &KafkaStream{
		topic:    topic,
		grpID:    configMap["group.id"].(string),
		consumer: cons,
	}, nil
}

type KafkaStream struct {
	topic    string
	grpID    string
	consumer *kafka.Consumer
}

func (k KafkaStream) Out(ctx context.Context) (<-chan fusion.Msg, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	out := make(chan fusion.Msg)
	go k.stream(ctx, out)

	return out, nil
}

func (k *KafkaStream) stream(ctx context.Context, out chan<- fusion.Msg) {
	defer close(out)

	for ctx.Err() == nil {
		km, err := k.consumer.ReadMessage(3 * time.Second)
		if err != nil {
			ke, ok := err.(kafka.Error)
			if !ok || ke.Code() != kafka.ErrTimedOut {
				log.Printf("read failed: %v", err)
			}
			continue
		}

		fuMsg := fusion.Msg{
			Key:     km.Key,
			Val:     km.Value,
			Attribs: map[string]string{"topic": k.topic},
			Ack: func(err error) {
				if err != nil {
					// do not acknowledge. rely on auto-commit false.
					return
				}
				_, _ = k.consumer.CommitMessage(km)
			},
		}

		select {
		case <-ctx.Done():
			return
		case out <- fuMsg:
		}
	}
}

func sanitiseConfig(configMap kafka.ConfigMap) *kafka.ConfigMap {
	gID, _ := configMap["group.id"].(string)
	gID = strings.TrimSpace(gID)
	if gID == "" {
		gID = fmt.Sprintf("kio_streamer_%d", rand.Int())
	}
	configMap["group.id"] = gID
	configMap["enable.auto.commit"] = false
	return &configMap
}
