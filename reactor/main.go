package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
)

func main() {
	var (
		config = flag.String("config", "reactor.json", "Configuration file")
	)
	flag.Parse()

	if *config == "" {
		fatalExit("-config must be specified")
	}
	cfg := readConf(*config)

	fmt.Println(cfg)
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

type Config struct{}

func fatalExit(msg string, args ...interface{}) {
	fmt.Printf(strings.TrimSpace(msg)+"\n", args...)
	os.Exit(1)
}
