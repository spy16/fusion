package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/jhump/protoreflect/dynamic"
	"github.com/spy16/fusion"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func main() {
	config := flag.String("config", "reactor.json", "Configuration file")
	flag.Parse()
	if *config == "" {
		fatalExit("-config must be specified")
	}

	cfg := readConf(*config)

	desc, err := Parse(cfg.MessageType, cfg.ProtoFiles, cfg.ProtoImportDirs)
	if err != nil {
		fatalExit("failed to parse proto: %v", err)
	}

	ks, err := KafkaConnect(cfg.Topic, cfg.Kafka)
	if err != nil {
		fatalExit("failed to connect to kafka: %v", ks)
	}

	fu := fusion.Runner{
		Proc: &fusion.Fn{
			Workers: 10,
			Func: func(ctx context.Context, msg fusion.Msg) error {
				pMsg := dynamic.NewMessage(desc)
				if err := pMsg.Unmarshal(msg.Val); err != nil {
					return fmt.Errorf("%w: %v", fusion.Fail, err)
				}
				_ = json.NewEncoder(os.Stdout).Encode(pMsg)
				return nil
			},
		},
		Stream:    ks,
		DrainTime: 5 * time.Second,
		Logger:    nil,
	}

	if err := fu.Run(context.Background()); err != nil {
		fatalExit("fusion runner exited with error: %v", err)
	}

	log.Printf("fusion runner exited successfully")
}

func readConf(configFile string) Config {
	f, err := os.Open(configFile)
	if err != nil {
		fatalExit("failed to open config file: %v", configFile, err)
	}
	defer func() { _ = f.Close() }()

	var cfg Config
	if err := json.NewDecoder(f).Decode(&cfg); err != nil {
		fatalExit("failed to read configs from '%s': %v", err)
	}
	return cfg
}

type Config struct {
	// kafka stream configs
	Topic string          `json:"topic"`
	Kafka kafka.ConfigMap `json:"kafka"`

	// protobuf configs
	MessageType     string   `json:"message_type"`
	ProtoFiles      []string `json:"proto_files"`
	ProtoImportDirs []string `json:"proto_import_dirs"`
}

func fatalExit(msg string, args ...interface{}) {
	fmt.Printf(strings.TrimSpace(msg)+"\n", args...)
	os.Exit(1)
}
