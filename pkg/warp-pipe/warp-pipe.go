package warppipe

import (
	"context"

	"github.com/jackc/pgx"
	"github.com/perangel/warp-pipe/pkg/listener"
	"github.com/perangel/warp-pipe/pkg/model"
	log "github.com/sirupsen/logrus"
)

// WarpPipe is a deamon that listens for database changes and transmits them
// somewhere else.
type WarpPipe struct {
	dbConfig  *pgx.ConnConfig
	listener  listener.Listener
	changesCh chan *model.Changeset
	errCh     chan error
	logger    *log.Entry
}

func initListener(lstType ListenerType) listener.Listener {
	switch lstType {
	case ListenerTypeNotify:
		return listener.NewNotifyListener()
	case ListenerTypeLogicalReplication:
		return listener.NewLogicalReplicationListener()
	}
	return nil
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

	return &WarpPipe{
		dbConfig: dbConfig,
		listener: initListener(config.ListenerType),
		logger:   log.WithFields(log.Fields{"component": "warp_pipe"}),
	}
}

// Open starts the WarpPipe listener and returns two channels, one for changesets
// and another for errors.
func (w *WarpPipe) Open() error {
	w.errCh = make(chan error)
	w.changesCh = make(chan *model.Changeset)

	err := w.listener.Dial(w.dbConfig)
	return err
}

func (w *WarpPipe) ListenForChanges(ctx context.Context) (<-chan *model.Changeset, <-chan error) {
	return w.listener.ListenForChanges(ctx)
}

// Close will close the listener and try to gracefully shutdown the WarpPipe.
func (w *WarpPipe) Close() error {
	return w.listener.Close()
}
