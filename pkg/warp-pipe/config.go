package warppipe

import (
	"github.com/kelseyhightower/envconfig"
	log "github.com/sirupsen/logrus"
)

// ListenerType is a constant for a listener type.
type ListenerType string

const (
	// ListenerTypeNotify is the type name for a listener.NotifyListener
	ListenerTypeNotify ListenerType = "notify"
	// ListenerTypeLogicalReplication is the type name for a listener.LogicalReplicationListener
	ListenerTypeLogicalReplication ListenerType = "logical_replication"
)

// Config stores configuration for a WarpPipe.
type Config struct {
	// DB config
	DBHost string `envconfig:"DB_HOST"`
	DBPort uint16 `envconfig:"DB_PORT" default:"5432"`
	DBUser string `envconfig:"DB_USER"`
	DBPass string `envconfig:"DB_PASS"`
	DBName string `envconfig:"DB_NAME"`

	// Listener Config
	ListenerType           ListenerType `envconfig:"listener_type"`
	ListenerTableWhitelist []string     `envconfig:"listener_table_whitelist"`
	ListenerReplSlotName   string       `envconfig:"listener_replication_slot_name"`
}

// NewConfigFromEnv initializes and returns a new Config with values read from the environment.
func NewConfigFromEnv() *Config {
	var c Config
	err := envconfig.Process("wp", &c)
	if err != nil {
		log.WithError(err).Fatal("Failed to parse configuration from environment.")
		return nil
	}

	return &c
}
