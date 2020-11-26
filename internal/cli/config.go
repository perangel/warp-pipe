package cli

import (
	"fmt"
	"time"

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

	if whitelistTables != nil {
		config.WhitelistTables = whitelistTables
	}

	if ignoreTables != nil {
		config.IgnoreTables = ignoreTables
	}

	if replicationMode != "" {
		config.ReplicationMode = replicationMode
	}

	if replicationSlotName != "" {
		config.ReplicationSlotName = replicationSlotName
	}

	config.StartFromID = startFromID
	config.StartFromTimestamp = startFromTimestamp

	if logLevel != "" {
		config.LogLevel = logLevel
	}

	return config, err
}

func initListener(config *warppipe.Config) (warppipe.Listener, error) {
	switch config.ReplicationMode {
	case replicationModeLR:
		var opts []warppipe.LROption

		if config.ReplicationSlotName != "" {
			opts = append(opts, warppipe.ReplSlotName(config.ReplicationSlotName))
		}

		return warppipe.NewLogicalReplicationListener(opts...), nil
	case replicationModeAudit:
		var opts []warppipe.NotifyOption

		if config.StartFromID != -1 {
			opts = append(opts, warppipe.StartFromID(config.StartFromID))
		} else if config.StartFromTimestamp != -1 {
			t := time.Unix(config.StartFromTimestamp, 0)
			opts = append(opts, warppipe.StartFromTimestamp(t))
		}

		return warppipe.NewNotifyListener(opts...), nil
	default:
		return nil, fmt.Errorf("'%s' is not a valid value for `--replication-mode`. Must be either `lr` or `audit`", config.ReplicationMode)
	}
}
