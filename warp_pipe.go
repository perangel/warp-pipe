package warppipe

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
)

// Option is a WarpPipe option function
type Option func(*WarpPipe)

// IgnoreTables is an option for setting the tables that WarpPipe should ignore.
// It accepts entries in either of the following formats:
//     <schema>.<table>
//     <schema>.*
//     <table>
// Any tables in this list will negate any whitelisted tables set via WhitelistTables().
func IgnoreTables(tables []string) Option {
	return func(w *WarpPipe) {
		w.ignoreTables = tables
	}
}

// WhitelistTables is an option for setting a list of tables we want to listen for change from.
// It accepts entries in either of the following formats:
//     <schema>.<table>
//     <schema>.*
//     <table>
// Any tables set via IgnoreTables() will be excluded.
func WhitelistTables(tables []string) Option {
	return func(w *WarpPipe) {
		w.whitelistTables = tables
	}
}

// LogLevel is an option for setting the logging level.
func LogLevel(level string) Option {
	return func(w *WarpPipe) {
		lvl, err := logrus.ParseLevel(level)
		if err != nil {
			w.logger.WithError(err).
				Warnf("'%s' is not a valid log level, defaulting to 'info'", level)
			lvl = logrus.InfoLevel
		}
		w.logger.Level = lvl
	}
}

// WarpPipe is a daemon that listens for database changes and transmits them
// somewhere else.
type WarpPipe struct {
	connConfig      *pgx.ConnConfig
	conn            *pgx.Conn
	listener        Listener
	ignoreTables    []string
	whitelistTables []string
	changesCh       <-chan *Changeset
	errCh           chan error
	logger          *log.Logger
}

// NewWarpPipe initializes and returns a new WarpPipe.
func NewWarpPipe(connConfig *pgx.ConnConfig, listener Listener, opts ...Option) (*WarpPipe, error) {
	conn, err := pgx.Connect(*connConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to the source database: %w", err)
	}

	w := &WarpPipe{
		connConfig: connConfig,
		conn:       conn,
		listener:   listener,
		logger:     log.New(),
	}

	for _, opt := range opts {
		opt(w)
	}

	return w, nil
}

// Open dials the listener's connection to the database.
func (w *WarpPipe) Open() error {
	return w.listener.Dial(w.connConfig)
}

// ListenForChanges starts the listener listening for database changesets.
// It returns two channels, on for Changesets, another for errors.
func (w *WarpPipe) ListenForChanges(ctx context.Context) (<-chan *Changeset, <-chan error) {
	P := NewPipeline()

	if w.whitelistTables != nil {
		P.AddStage("whitelist_tables", func(change *Changeset) (*Changeset, error) {
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
		P.AddStage("ignore_tables", func(change *Changeset) (*Changeset, error) {
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

// IsLatestChangeSet returns true if the id argument matches that of the last record in the changeset table.
// TODO: This feature only supports the notify listener. It needs to support others.
func (w *WarpPipe) IsLatestChangeSet(id int64) (bool, error) {
	switch w.listener.(type) {
	case *NotifyListener:
		rows, err := w.conn.Query("SELECT id FROM warp_pipe.changesets ORDER BY id DESC LIMIT 1")
		if err != nil {
			return false, fmt.Errorf("failed to query latest changeset record: %w", err)
		}
		for rows.Next() {
			latestID := int64(0)
			err := rows.Scan(&latestID)
			if err != nil {
				return false, fmt.Errorf("failed to scan latest changeset record: %w", err)
			}
			if latestID == id {
				return true, nil
			}
		}
	default:
		return false, fmt.Errorf("unsupported listener. unable to determine if change is latest")
	}
	return false, nil
}

func (w *WarpPipe) shutdown() error {
	// TODO: implement any state preservation
	return w.listener.Close()
}
