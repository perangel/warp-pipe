package warppipe

import (
	"github.com/jackc/pgx"
	"github.com/perangel/warp-pipe/pkg/listener"
	log "github.com/sirupsen/logrus"
)

// WarpPipe is a deamon that listens for database changes and transmit them
// somewhere else.
type WarpPipe struct {
	logger   *log.Entry
	listener listener.Listener
}

// NewWarpPipe initializes and returns a new WarpPipe.
func NewWarpPipe(config *Config) *WarpPipe {
	dbConfig := &pgx.ConnConfig{
		Host:     config.DBHost,
		Port:     config.DBPort,
		User:     config.DBUser,
		Password: config.DBPass,
		Database: config.DBName,
	}

	var lstn listener.Listener
	switch config.ListenerType {
	case ListenerTypeNotify:
		lstn = listener.NewNotifyListener(dbConfig)
	case ListenerTypeLogicalReplication:
		lstn = listener.NewLogicalReplicationListener(dbConfig)
	}

	return &WarpPipe{
		listener: lstn,
		logger: log.WithFields(log.Fields{
			"component": "warp_pipe",
		}),
	}
}
