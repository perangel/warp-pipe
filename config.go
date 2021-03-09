package warppipe

import (
	"errors"
	"fmt"

	"github.com/kelseyhightower/envconfig"
	"github.com/sirupsen/logrus"
)

// DBConfig represents the database configuration settings.
type DBConfig struct {
	Host     string `envconfig:"DB_HOST"`
	Port     int    `envconfig:"DB_PORT"`
	User     string `envconfig:"DB_USER"`
	Password string `envconfig:"DB_PASS"`
	Database string `envconfig:"DB_NAME"`
	Schema   string `envconfig:"DB_SCHEMA"`
}

// Config represents the warp pipe configuration settings.
type Config struct {
	// Database connection settings.
	Database DBConfig

	// If defined, warppipe will only emit changes for the specified tables.
	WhitelistTables []string `envconfig:"WHITELIST_TABLES"`

	// If set, warppipe will suppress changes for any specified tables.
	// Note: This setting takes precedent over the whitelisted tables.
	IgnoreTables []string `envconfig:"IGNORE_TABLES"`

	// Replication mode may be either `lr` (logical replication) or `audit`.
	ReplicationMode string `envconfig:"REPLICATION_MODE" default:"lr"`

	// Specifies the replication slot name to be used. (LR mode only)
	ReplicationSlotName string `envconfig:"REPLICATION_SLOT_NAME"`

	// Start replication from the specified logical sequence number. (LR mode only)
	StartFromLSN uint64 `envconfig:"START_FROM_LSN"`

	// Start replication after the changeset count offset. (Audit mode only)
	StartFromOffset int64 `envconfig:"START_FROM_OFFSET"`

	// Start replication from the specified changeset timestamp. (Audit mode only)
	StartFromTimestamp int64 `envconfig:"START_FROM_TIMESTAMP"`

	// Sets the log level
	LogLevel string `envconfig:"LOG_LEVEL" default:"info"`
}

// NewConfigFromEnv returns a new Config initialized with values read from the environment.
func NewConfigFromEnv() (*Config, error) {
	var c Config
	err := envconfig.Process("wp", &c)
	if err != nil {
		return nil, errors.New("unable to parse configuration from environment")
	}

	var dbCfg DBConfig
	err = envconfig.Process("wp", &dbCfg)
	if err != nil {
		return nil, errors.New("unable to parse database configuration from environment")
	}

	c.Database = dbCfg

	return &c, nil
}

// ParseLogLevel parses a log level string and returns a logrus.Level.
func ParseLogLevel(level string) (logrus.Level, error) {
	lvl, err := logrus.ParseLevel(level)
	if err != nil {
		return 0, fmt.Errorf("Error: '%s' is not a valid log level. Must be one of: 'trace', 'debug', 'info', 'warn', 'error', 'fatal'", level)
	}
	return lvl, err
}
