package config

import (
	"errors"

	"github.com/kelseyhightower/envconfig"
)

// Config stores configuration for the application.
type Config struct {
	DBConfig
	DBSchema        string   `envconfig:"DB_SCHEMA" default:"public"`
	ReplicationMode string   `envconfig:"REPLICATION_MODE" default:"lr"`
	IgnoreTables    []string `envconfig:"IGNORE_TABLES"`
	LogLevel        string   `envconfig:"LOG_LEVEL" default:"info"`
}

// NewConfigFromEnv initializes and returns a new Config with values read from the environment.
func NewConfigFromEnv() (*Config, error) {
	var c Config
	err := envconfig.Process("wp", &c)
	if err != nil {
		return nil, errors.New("failed to parse configuration from environment")
	}
	return &c, nil
}
