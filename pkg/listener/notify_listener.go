package listener

import (
	"github.com/jackc/pgx"
	"github.com/perangel/warp-pipe/pkg/model"
	log "github.com/sirupsen/logrus"
)

// NotifyListener is a listener that uses Postgres' LISTEN/NOTIFY pattern for
// subscribing for subscribing to changeset enqueued in a changesets table.
// For more details see `pkg/schema/changesets`.
type NotifyListener struct {
	conn   *pgx.Conn
	logger *log.Entry

	changesetsCh chan *model.Changeset
}

// NewNotifyListener returne a new NotifyListener.
func NewNotifyListener(connConfig *pgx.ConnConfig) Listener {
	return &NotifyListener{
		logger:       log.WithFields(log.Fields{"component": "listener"}),
		changesetsCh: make(chan *model.Changeset),
	}
}

// Listen connects to the database and emits changes via a channel.
func (l *NotifyListener) Listen(connConfig *pgx.ConnConfig) error {
	conn, err := pgx.Connect(*connConfig)
	if err != nil {
		log.WithError(err).Error("Failed to connect to database.")
		return err
	}

	l.conn = conn
	return nil
}

// Close closes the database connection.
func (l *NotifyListener) Close() error {
	if err := l.conn.Close(); err != nil {
		log.WithError(err).Error("Error when closing database connection.")
		return err
	}

	return nil
}

// Changes returns a channel that emits database changesets.
func (l *NotifyListener) Changes() chan *model.Changeset {
	return l.changesetsCh
}
