package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/spy16/fusion"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	go callOnInterrupt(cancel)

	actor := fusion.New(fusion.Options{
		Workers:    1,
		MaxRetries: 3,
		Backoff:    fusion.ExpBackoff(2, 1*time.Millisecond, 1*time.Second),
		Logger:     logrus.New(),
		Processor: func(ctx context.Context, msg fusion.Message) error {
			fmt.Printf("value='%s'\n", string(msg.Val))
			time.Sleep(1 * time.Second)
			return nil
		},
		OnFailure: func(msg fusion.Message, err error) error {
			logrus.Printf("failed msg=%v, err=%v", msg, err)
			return nil
		},
		Stream: &fusion.LineStream{From: os.Stdin, Offset: 2},
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
