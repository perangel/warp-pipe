package warppipe

import (
	"github.com/jackc/pgx"
	"github.com/perangel/warp-pipe/pkg/listener"
	"github.com/perangel/warp-pipe/pkg/model"
	log "github.com/sirupsen/logrus"
)

// WarpPipe is a deamon that listens for database changes and transmits them
// somewhere else.
type WarpPipe struct {
	logger   *log.Entry
	listener listener.Listener
	errCh    chan error
	dbConfig *pgx.ConnConfig
}

// NewWarpPipe initializes and returns a new WarpPipe.
func NewWarpPipe(config *Config) *WarpPipe {
	var lstn listener.Listener
	switch config.ListenerType {
	case ListenerTypeNotify:
		lstn = listener.NewNotifyListener()
	case ListenerTypeLogicalReplication:
		lstn = listener.NewLogicalReplicationListener()
	}

	dbConfig := &pgx.ConnConfig{
		Host:     config.DBHost,
		Port:     config.DBPort,
		User:     config.DBUser,
		Password: config.DBPass,
		Database: config.DBName,
	}

	return &WarpPipe{
		dbConfig: dbConfig,
		listener: lstn,
		logger: log.WithFields(log.Fields{
			"component": "warp_pipe",
		}),
	}
}

// ListenForChanges starts the listener and returns two channels, one for changesets
// and another for errors.
func (w *WarpPipe) ListenForChanges() (chan *model.Changeset, chan error) {
	w.errCh = make(chan error)
	err := w.listener.Listen(w.dbConfig)
	if err != nil {
		w.errCh <- err
	}

	return w.listener.Changes(), w.errCh
}

// Close will close the listener and try to gracefully shutdown the WarpPipe.
func (w *WarpPipe) Close() error {
	return w.listener.Close()
}
