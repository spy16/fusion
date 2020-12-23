package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/spy16/fusion"
)

// some thing

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	go callOnInterrupt(cancel)

	counter := int64(0)

	fu := fusion.Runner{
		Stream: &fusion.LineStream{From: os.Stdin},
		Proc: &fusion.Fn{
			Workers: 5,
			Func: func(ctx context.Context, msg fusion.Msg) error {
				atomic.AddInt64(&counter, 1)
				return nil
			},
		},
	}

	if err := fu.Run(ctx); err != nil {
		log.Fatalf("fusion exited with err: %v", err)
	}
	log.Printf("count=%d", counter)
}

func callOnInterrupt(cancel context.CancelFunc) {
	sigCh := make(chan os.Signal)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	<-sigCh
	cancel()
}
