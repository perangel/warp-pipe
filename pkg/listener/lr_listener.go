package listener

import (
	"github.com/jackc/pgx"
	"github.com/perangel/warp-pipe/pkg/model"
	log "github.com/sirupsen/logrus"
)

// LogicalReplicationListener is a Listener that uses logical replication slots
// to listen for changesets.
type LogicalReplicationListener struct {
	conn     *pgx.Conn
	replConn *pgx.ReplicationConn
	logger   *log.Entry

	changesetsCh chan *model.Changeset
}

// NewLogicalReplicationListener returns a new LogicalReplicationListener.
func NewLogicalReplicationListener(connConfig *pgx.ConnConfig) Listener {
	return &LogicalReplicationListener{
		logger:       log.WithFields(log.Fields{"component": "listener"}),
		changesetsCh: make(chan *model.Changeset),
	}
}

// Listen connects to the database and emits changes via a channel.
func (l *LogicalReplicationListener) Listen(connConfig *pgx.ConnConfig) error {
	conn, err := pgx.Connect(*connConfig)
	if err != nil {
		log.WithError(err).Error("Failed to connect to database.")
		return err
	}
	l.conn = conn

	replConn, err := pgx.ReplicationConnect(*connConfig)
	if err != nil {
		log.WithError(err).Error("Failed to connect to database.")
		return err
	}
	l.replConn = replConn

	return nil
}

// Changes returns a channel that emits database changesets.
func (l *LogicalReplicationListener) Changes() chan *model.Changeset {
	return l.changesetsCh
}

// Close closes the database connection.
func (l *LogicalReplicationListener) Close() error {
	if err := l.replConn.Close(); err != nil {
		log.WithError(err).Error("Error when closing database replication connection.")
		return err
	}

	if err := l.conn.Close(); err != nil {
		log.WithError(err).Error("Error when closing database connection.")
		return err
	}

	return nil
}
