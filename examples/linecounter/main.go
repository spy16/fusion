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

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	go callOnInterrupt(cancel)

	ls := &fusion.LineStream{From: os.Stdin}

	counter := int64(0)
	proc := func(ctx context.Context, msg fusion.Msg) error {
		atomic.AddInt64(&counter, 1)
		return nil
	}

	fu, err := fusion.New(ls, fusion.Options{Processor: fusion.Proc(proc)})
	if err != nil {
		panic(err)
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
