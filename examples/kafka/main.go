package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"

	"github.com/spy16/fusion"
	"github.com/spy16/fusion/stream"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	go callOnInterrupt(cancel)

	lg := logrus.New()
	lg.SetLevel(logrus.DebugLevel)

	actor := fusion.New(fusion.Options{
		Workers:      1,
		MaxRetries:   3,
		Backoff:      fusion.ExpBackoff(2, 1*time.Millisecond, 1*time.Second),
		Logger:       lg,
		PollInterval: 100 * time.Millisecond,
		Processor: func(ctx context.Context, msg fusion.Message) error {
			if rand.Float64() > 0.6 {
				logrus.Infof("simulating failure")
				return errors.New("failed")
			}
			fmt.Printf("value='%s'\n", string(msg.Val))
			return nil
		},
		OnFailure: func(msg fusion.Message, err error) {
			logrus.Printf("failed msg=%v, err=%v", msg, err)
		},
		Stream: &stream.Kafka{
			Topic: "fusion",
			Config: map[string]kafka.ConfigValue{
				"bootstrap.servers": "localhost:9092",
				"group.id":          "fusion_streamer",
			},
		},
	})

	if err := actor.Run(ctx); err != nil {
		logrus.Fatalf("actor exited: %v", err)
	} else {
		logrus.Info("actor shutdown gracefully")
	}
}

func callOnInterrupt(cancel context.CancelFunc) {
	sigCh := make(chan os.Signal)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	<-sigCh
	cancel()
}
