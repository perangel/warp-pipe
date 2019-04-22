package cli

import (
	"fmt"
	"strings"

	"github.com/perangel/warp-pipe/pkg/config"
	warppipe "github.com/perangel/warp-pipe/pkg/warp-pipe"
)

func parseConfig() (*config.Config, error) {
	config, err := config.NewConfigFromEnv()
	if err != nil {
		return nil, err
	}

	if dbHost != "" {
		config.DBHost = dbHost
	}

	if dbPort != 0 {
		config.DBPort = uint16(dbPort)
	}

	if dbUser != "" {
		config.DBUser = dbUser
	}

	if dbPass != "" {
		config.DBPass = dbPass
	}

	if dbName != "" {
		config.DBName = dbName
	}

	if dbSchema != "" {
		config.DBSchema = dbSchema
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

func parseLogLevel(level string) (warppipe.LogLevel, error) {
	var lvl warppipe.LogLevel
	var err error

	switch strings.ToLower(level) {
	case "trace":
		lvl = warppipe.LogLevelTrace
	case "debug":
		lvl = warppipe.LogLevelDebug
	case "info":
		lvl = warppipe.LogLevelInfo
	case "warn":
		lvl = warppipe.LogLevelWarn
	case "error":
		lvl = warppipe.LogLevelError
	case "fatal":
		lvl = warppipe.LogLevelFatal
	default:
		lvl = 0
		err = fmt.Errorf("'%s' is not a valid log level for `--log-level`. Must be one of: 'trace', 'debug', 'info', 'warn', 'error', 'fatal'", level)
	}
	return lvl, err
}

func parseReplicationMode(mode string) (warppipe.ReplMode, error) {
	switch mode {
	case replicationModeLR:
		return warppipe.LRMode, nil
	case replicationModeQueue:
		return warppipe.QueueMode, nil
	default:
		return "", fmt.Errorf("'%s' is not a valid value for `--replication-mode`. Must be either `lr` or `queue`", mode)
	}
}
