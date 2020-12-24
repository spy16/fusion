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

	"github.com/jhump/protoreflect/dynamic"
	"github.com/sirupsen/logrus"

	"github.com/spy16/fusion"
)

var logger = logrus.New()

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	go callOnInterrupt(cancel)

	config := flag.String("config", "reactor.json", "Configuration file")
	flag.Parse()
	if *config == "" {
		fatalExit("-config must be specified")
	}

	cfg := readConf(*config)
	lvl, err := logrus.ParseLevel(cfg.LogLevel)
	if err != nil {
		logger.Warnf("invalid log level '%s', using 'warn'", cfg.LogLevel)
		lvl = logrus.WarnLevel
	}
	logger.SetLevel(lvl)

	cfg.Kafka.Logger = logger.WithFields(map[string]interface{}{"component": "KafkaStream"})

	desc, err := Parse(cfg.MessageType, cfg.ProtoFiles, cfg.ProtoImportDirs)
	if err != nil {
		fatalExit("failed to parse proto: %v", err)
	}

	fu := fusion.Runner{
		Stream:    cfg.Kafka,
		DrainTime: 5 * time.Second,
		Logger:    logger.WithFields(map[string]interface{}{"component": "fusion.Runner"}),
		Proc: &fusion.Fn{
			Workers: 10,
			Logger:  logger.WithFields(map[string]interface{}{"component": "fusion.Fn"}),
			Func: func(ctx context.Context, msg fusion.Msg) error {
				pMsg := dynamic.NewMessage(desc)
				if err := pMsg.Unmarshal(msg.Val); err != nil {
					return fmt.Errorf("%w: %v", fusion.Fail, err)
				}
				_ = json.NewEncoder(os.Stdout).Encode(pMsg)
				return nil
			},
		},
	}

	if err := fu.Run(ctx); err != nil {
		fatalExit("fusion runner exited with error: %v", err)
	}

	logger.Infof("fusion runner exited successfully")
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
	LogLevel string `json:"log_level"`

	// kafka stream configs
	Topic string      `json:"topic"`
	Kafka KafkaStream `json:"kafka"`

	// protobuf configs
	MessageType     string   `json:"message_type"`
	ProtoFiles      []string `json:"proto_files"`
	ProtoImportDirs []string `json:"proto_import_dirs"`
}

func fatalExit(msg string, args ...interface{}) {
	logger.Fatalf(msg, args...)
}

func callOnInterrupt(cancel context.CancelFunc) {
	sigCh := make(chan os.Signal)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	<-sigCh
	cancel()
}
