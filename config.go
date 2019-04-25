package warppipe

import (
	"errors"
	"fmt"

	"github.com/kelseyhightower/envconfig"
	"github.com/sirupsen/logrus"
)

// DBConfig is a struct that stores database connection settings.
type DBConfig struct {
	Host     string `envconfig:"DB_HOST"`
	Port     int    `envconfig:"DB_PORT"`
	User     string `envconfig:"DB_USER"`
	Password string `envconfig:"DB_PASS"`
	Database string `envconfig:"DB_NAME"`
}

// Config is a struct that stores warp pipe configuration settings.
type Config struct {
	// Database configuration settings.
	Database DBConfig
	// ReplicationMode defines the type of listener that will be used to replicate
	// database changes. May be one of `lr` or `queue`.
	ReplicationMode string `envconfig:"REPLICATION_MODE" default:"lr"`
	// (LR mode) ReplicationSlotName specifies the name to be used.
	ReplicationSlotName string `envconfig:"REPLICATION_SLOT_NAME"`
	// If set, only include changesets from the following tables.
	WhitelistTables []string `envconfig:"WHITELIST_TABLES"`
	// If set, tables will be ignored when replicating changesets.
	// Any tables listed that are also in the whitelist will be ignored.
	IgnoreTables []string `envconfig:"IGNORE_TABLES"`
	// Logging level
	LogLevel string `envconfig:"LOG_LEVEL" default:"info"`
}

// NewConfigFromEnv returns a new Config initialized with values read from the environment.
func NewConfigFromEnv() (*Config, error) {
	var c Config
	err := envconfig.Process("wp", &c)
	if err != nil {
		return nil, errors.New("unable to parse configuration from environment")
	}
	return &c, nil
}

func ParseLogLevel(level string) (logrus.Level, error) {
	lvl, err := logrus.ParseLevel(level)
	if err != nil {
		return 0, fmt.Errorf("Error: '%s' is not a valid log level. Must be one of: 'trace', 'debug', 'info', 'warn', 'error', 'fatal'", level)
	}
	return lvl, err
}
