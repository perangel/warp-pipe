package warppipe

import (
	"context"

	"github.com/jackc/pgx"
	"github.com/perangel/warp-pipe/internal/db"
	"github.com/perangel/warp-pipe/internal/listener"
	"github.com/perangel/warp-pipe/pkg/model"
	log "github.com/sirupsen/logrus"
)

// WarpPipe is a daemon that listens for database changes and transmits them
// somewhere else.
type WarpPipe struct {
	dbConfig     *pgx.ConnConfig
	listener     listener.Listener
	replSlotName string
	dbSchema     string
	ignoreTables []string
	changesCh    chan *model.Changeset
	errCh        chan error
	logger       *log.Logger
}

// NewWarpPipe initializes and returns a new WarpPipe.
func NewWarpPipe(connCfg *db.ConnConfig, opts ...Option) *WarpPipe {
	w := &WarpPipe{
		dbConfig: &pgx.ConnConfig{
			Host:     connCfg.DBHost,
			Port:     connCfg.DBPort,
			User:     connCfg.DBUser,
			Password: connCfg.DBPass,
			Database: connCfg.DBName,
		},
		logger: log.New(),
	}

	// apply options
	for _, opt := range opts {
		opt(w)
	}

	// default listener if not set by opts
	if w.listener == nil {
		w.listener = listener.NewLogicalReplicationListener()
	}

	// default schema to `public` if not set by opts
	if w.dbSchema == "" {
		w.dbSchema = "public"
	}

	return w
}

// Open starts the listener's connection to the database.
func (w *WarpPipe) Open() error {
	err := w.listener.Dial(w.dbConfig)
	return err
}

// ListenForChanges starts the listener listening for database changesets.
// It returns two channels, on for Changesets, another for errors.
func (w *WarpPipe) ListenForChanges(ctx context.Context) (<-chan *model.Changeset, <-chan error) {
	return w.listener.ListenForChanges(ctx)
}

// Close will close the listener and try to gracefully shutdown the WarpPipe.
func (w *WarpPipe) Close() error {
	err := w.shutdown()
	if err != nil {
		w.logger.WithError(err).Warn("unable to gracefully shutdown warp pipe")
		return err
	}
	return nil
}

func (w *WarpPipe) shutdown() error {
	// TODO: implement any state preservation
	return w.listener.Close()
}
