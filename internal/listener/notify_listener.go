package listener

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx"
	"github.com/perangel/warp-pipe/internal/store"
	"github.com/perangel/warp-pipe/pkg/model"
	log "github.com/sirupsen/logrus"
)

// NotifyListener is a listener that uses Postgres' LISTEN/NOTIFY pattern for
// subscribing for subscribing to changeset enqueued in a changesets table.
// For more details see `pkg/schema/changesets`.
type NotifyListener struct {
	conn   *pgx.Conn
	logger *log.Entry

	store                  store.EventStore
	lastProcessedTimestamp *time.Time
	changesetsCh           chan *model.Changeset
	errCh                  chan error
}

// NewNotifyListener returne a new NotifyListener.
func NewNotifyListener() *NotifyListener {
	return &NotifyListener{
		logger:       log.WithFields(log.Fields{"component": "listener"}),
		changesetsCh: make(chan *model.Changeset),
		errCh:        make(chan error),
	}
}

// Dial connects to the source database.
func (l *NotifyListener) Dial(connConfig *pgx.ConnConfig) error {
	conn, err := pgx.Connect(*connConfig)
	if err != nil {
		log.WithError(err).Error("Failed to connect to database.")
		return err
	}

	l.conn = conn
	return nil
}

// ListenForChanges returns a channel that emits database changesets.
func (l *NotifyListener) ListenForChanges(ctx context.Context) (chan *model.Changeset, chan error) {
	l.logger.Info("Starting notify listener for `warp_pipe_new_changeset`")
	err := l.conn.Listen("warp_pipe_new_changeset")
	if err != nil {
		l.logger.WithError(err).Fatal("failed to listen on notify channel")
	}

	l.store = store.NewChangesetStore(l.conn)

	// loop - listen for notifications
	go func() {
		for {
			msg, err := l.conn.WaitForNotification(ctx)
			if err != nil {
				if ctx.Err() != nil {
					log.Info("shutting down...")
					return
				}
				if err != nil {
					log.WithError(err).Error("encountered an error while waiting for notifications")
					l.errCh <- err
				}
			}

			l.processMessage(msg.Payload)
		}
	}()

	return l.changesetsCh, l.errCh
}

func (l *NotifyListener) processMessage(msg string) {
	// TODO: finish implementation
	fmt.Println(msg)
}

// Close closes the database connection.
func (l *NotifyListener) Close() error {
	if err := l.conn.Close(); err != nil {
		log.WithError(err).Error("Error when closing database connection.")
		return err
	}

	return nil
}
