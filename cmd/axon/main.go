package main

import (
	"fmt"

	_ "github.com/lib/pq"
	"github.com/sirupsen/logrus"

	warppipe "github.com/perangel/warp-pipe"
)

func main() {
	logger := logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{})

	cfg, err := warppipe.NewAxonConfigFromEnv()
	if err != nil {
		logger.WithError(err).Fatal("failed to process environment config")
	}

	axon := warppipe.Axon{Config: cfg, Logger: logger}
	err = axon.Run()
	if err != nil {
		fmt.Printf("ERROR: %s\n", err.Error())
	}
}
