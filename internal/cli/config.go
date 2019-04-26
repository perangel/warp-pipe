package cli

import (
	"fmt"

	warppipe "github.com/perangel/warp-pipe"
)

func parseConfig() (*warppipe.Config, error) {
	config, err := warppipe.NewConfigFromEnv()
	if err != nil {
		return nil, err
	}

	if dbHost != "" {
		config.Database.Host = dbHost
	}

	if dbPort != 0 {
		config.Database.Port = dbPort
	}

	if dbUser != "" {
		config.Database.User = dbUser
	}

	if dbPass != "" {
		config.Database.Password = dbPass
	}

	if dbName != "" {
		config.Database.Database = dbName
	}

	if replicationMode != "" {
		config.ReplicationMode = replicationMode
	}

	if ignoreTables != nil {
		config.IgnoreTables = ignoreTables
	}

	if logLevel != "" {
		config.LogLevel = logLevel
	}

	return config, err
}

func parseReplicationMode(mode string) (warppipe.Listener, error) {
	switch mode {
	case replicationModeLR:
		return warppipe.NewLogicalReplicationListener(), nil
	case replicationModeAudit:
		return warppipe.NewNotifyListener(), nil
	default:
		return nil, fmt.Errorf("'%s' is not a valid value for `--replication-mode`. Must be either `lr` or `audit`", mode)
	}
}
