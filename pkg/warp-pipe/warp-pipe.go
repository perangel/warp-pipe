package warppipe

import (
	"context"
	"strings"

	"github.com/jackc/pgx"
	"github.com/perangel/warp-pipe/internal/listener"
	"github.com/perangel/warp-pipe/pkg/config"
	"github.com/perangel/warp-pipe/pkg/model"
	"github.com/perangel/warp-pipe/pkg/pipeline"
	log "github.com/sirupsen/logrus"
)

// WarpPipe is a daemon that listens for database changes and transmits them
// somewhere else.
type WarpPipe struct {
	dbConfig        *pgx.ConnConfig
	listener        listener.Listener
	replSlotName    string
	dbSchema        string
	ignoreTables    []string
	whitelistTables []string
	changesCh       chan *model.Changeset
	errCh           chan error
	logger          *log.Logger
}

// NewWarpPipe initializes and returns a new WarpPipe.
func NewWarpPipe(dbConfig *config.DBConfig, opts ...Option) *WarpPipe {
	w := &WarpPipe{
		dbConfig: &pgx.ConnConfig{
			Host:     dbConfig.DBHost,
			Port:     dbConfig.DBPort,
			User:     dbConfig.DBUser,
			Password: dbConfig.DBPass,
			Database: dbConfig.DBName,
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
	P := pipeline.NewPipeline()

	if w.whitelistTables != nil {
		P.AddStage("whitelist_tables", func(change *model.Changeset) (*model.Changeset, error) {
			for _, table := range w.whitelistTables {
				parts := strings.Split(table, ".")
				// <schema>.<table>
				if len(parts) == 2 {
					if parts[0] == change.Schema {
						if parts[1] == "*" {
							return change, nil
						} else if parts[1] == change.Table {
							return change, nil
						}
					}
					// <table>
				} else {
					if parts[0] == change.Table {
						return change, nil
					}
				}
			}

			return nil, nil
		})
	}

	if w.ignoreTables != nil {
		P.AddStage("ignore_tables", func(change *model.Changeset) (*model.Changeset, error) {
			for _, table := range w.ignoreTables {
				parts := strings.Split(table, ".")
				// <schema>.<table>
				if len(parts) == 2 {
					if parts[0] == change.Schema {
						if parts[1] == "*" {
							return nil, nil
						} else if parts[1] == change.Table {
							return nil, nil
						}
					}
					// <table>
				} else {
					if parts[0] == change.Table {
						return nil, nil
					}
				}
			}
			return change, nil
		})

	}

	// listen for changes
	changeCh, errCh := w.listener.ListenForChanges(ctx)
	w.errCh = errCh

	// starts a pipeline
	outCh, _ := P.Start(ctx, changeCh)
	w.changesCh = outCh

	return w.changesCh, w.errCh
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
