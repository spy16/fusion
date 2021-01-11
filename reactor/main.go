package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spy16/fusion"

	"github.com/spy16/fusion/reactor/stream"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	go callOnInterrupt(cancel)

	config := flag.String("config", "reactor.json", "Configuration file")
	flag.Parse()
	if *config == "" {
		fatalExit("-config must be specified")
	}

	cfg := readConf(*config)

	fu := fusion.Runner{
		Stream:    cfg.Kafka,
		DrainTime: 5 * time.Second,
		Log:       jsonLog,
		Proc: &fusion.Fn{
			Workers: 10,
			Func: func(ctx context.Context, msg fusion.Msg) error {
				if cfg.Proto == nil {
					_, err := fmt.Fprintln(os.Stdout, string(msg.Val))
					return err
				}

				protoMsg, err := cfg.Proto.Unmarshal(msg.Val)
				if err != nil {
					jsonLog(map[string]interface{}{
						"level":   "error",
						"message": err.Error(),
					})
					return err
				}
				_ = json.NewEncoder(os.Stdout).Encode(protoMsg)
				return nil
			},
		},
	}

	if err := fu.Run(ctx); err != nil {
		fatalExit("fusion runner exited with error: %v", err)
	}

	jsonLog(map[string]interface{}{
		"level":   "info",
		"message": "fusion runner exited successfully",
	})
}

func readConf(configFile string) Config {
	f, err := os.Open(configFile)
	if err != nil {
		fatalExit("failed to open config file: %v", err)
	}
	defer func() { _ = f.Close() }()

	var cfg Config
	if err := json.NewDecoder(f).Decode(&cfg); err != nil {
		fatalExit("failed to read configs from '%s': %v", configFile, err)
	}
	return cfg
}

type Config struct {
	Topic string       `json:"topic"`
	Kafka stream.Kafka `json:"kafka"`
	Proto *ProtoBuf    `json:"proto"`
}

func fatalExit(msg string, args ...interface{}) {
	jsonLog(map[string]interface{}{
		"level":   "fatal",
		"message": fmt.Sprintf(msg, args...),
	})
	os.Exit(1)
}

func callOnInterrupt(cancel context.CancelFunc) {
	sigCh := make(chan os.Signal)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	<-sigCh
	cancel()
}

func jsonLog(fields map[string]interface{}) {
	_ = json.NewEncoder(os.Stderr).Encode(fields)
}
