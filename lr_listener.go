package warppipe

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx"
	log "github.com/sirupsen/logrus"
)

const (
	replicationSlotNamePrefix = "wp_"
	replicationOutputPlugin   = "wal2json"
)

var (
	// ErrReplicationSlotExists is an error returned when trying to create an replication slot
	// that already exists.
	ErrReplicationSlotExists = errors.New("replication slot with same name already exists")

	// ErrAllReplicationSlotsInUse is an error returned when there are no more available
	// replication slots.
	ErrAllReplicationSlotsInUse = errors.New("all replication slots are currently in use")
)

var (
	defaultWal2jsonArgs = []string{
		"\"include-lsn\" 'on'",
		"\"pretty-print\" 'off'",
		"\"include-timestamp\" 'on'",
		"\"filter-tables\" 'warp_pipe.*'",
	}
)

// LogicalReplicationListener is a Listener that uses logical replication slots
// to listen for changesets.
type LogicalReplicationListener struct {
	conn                         *pgx.Conn
	replConn                     *pgx.ReplicationConn
	replSlotName                 string
	replLSN                      uint64
	wal2jsonArgs                 []string
	connHeartbeatIntervalSeconds int
	changesetsCh                 chan *Changeset
	errCh                        chan error
	logger                       *log.Entry
}

// NewLogicalReplicationListener returns a new LogicalReplicationListener.
func NewLogicalReplicationListener(opts ...LROption) *LogicalReplicationListener {
	l := &LogicalReplicationListener{
		logger:       log.WithFields(log.Fields{"component": "listener"}),
		wal2jsonArgs: defaultWal2jsonArgs,
	}

	for _, opt := range opts {
		opt(l)
	}

	if l.connHeartbeatIntervalSeconds == 0 {
		l.connHeartbeatIntervalSeconds = 10
	}

	return l
}

// Dial connects to the source database.
func (l *LogicalReplicationListener) Dial(connConfig *pgx.ConnConfig) error {
	conn, err := pgx.Connect(*connConfig)
	if err != nil {
		l.logger.WithError(err).Error("failed to connect to database")
		return err
	}
	l.conn = conn

	replConn, err := pgx.ReplicationConnect(*connConfig)
	if err != nil {
		l.logger.WithError(err).Error("failed to connect to database")
		return err
	}
	l.replConn = replConn

	err = l.initReplicationSlot()
	if err != nil {
		l.logger.WithError(err).Fatal("failed to initialize replication slot")
	}

	return nil
}

// ListenForChanges returns a channel that emits database changesets.
func (l *LogicalReplicationListener) ListenForChanges(ctx context.Context) (chan *Changeset, chan error) {
	l.logger.Infof("Starting replication for slot '%s' from LSN %d (%s)",
		l.replSlotName,
		l.replLSN,
		pgx.FormatLSN(l.replLSN),
	)

	err := l.replConn.StartReplication(l.replSlotName, l.replLSN, -1, l.wal2jsonArgs...)
	if err != nil {
		l.logger.WithError(err).Fatal("failed to start replication")
	}

	go l.startHeartBeat(ctx)

	l.changesetsCh = make(chan *Changeset)
	l.errCh = make(chan error)

	// loop - listen for messages
	go func() {
		for {
			if !l.replConn.IsAlive() {
				log.WithField("conn_err", l.replConn.CauseOfDeath()).Error(
					"replication connection is down",
				)
			}

			msg, err := l.replConn.WaitForReplicationMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					log.Info("shutting down...")
					return
				}
				log.WithError(err).Error("encountered an error while waiting for replication message")
				l.errCh <- err
			}

			if msg != nil && msg.WalMessage != nil {
				l.processMessage(msg)
			} else {
				continue
			}

			if msg.ServerHeartbeat != nil {
				l.logger.WithField("heartbeat", msg.ServerHeartbeat).Info("received server heartbeat")
				if msg.ServerHeartbeat.ReplyRequested == 1 {
					l.sendStandbyStatus()
				}
			}
		}
	}()

	return l.changesetsCh, l.errCh
}

// Close closes the database connection.
func (l *LogicalReplicationListener) Close() error {
	if err := l.replConn.Close(); err != nil {
		l.logger.WithError(err).Error("error when closing database replication connection.")
		return err
	}

	if err := l.conn.Close(); err != nil {
		l.logger.WithError(err).Error("error when closing database connection.")
		return err
	}

	return nil
}

func (l *LogicalReplicationListener) initReplicationSlot() error {
	// If no replication slot name is specified, auto-generate one
	if l.replSlotName == "" {
		l.replSlotName = fmt.Sprintf("%s%d", replicationSlotNamePrefix, time.Now().Unix())
	}

	// Clear any prior auto-generate replication slots (e.g. wp_XXXX)
	l.clearReplicationSlots()

	// Create the replication slot
	err := l.createReplicationSlot(l.replSlotName)
	if err != nil {
		switch err {
		case ErrReplicationSlotExists:
			if strings.HasPrefix(l.replSlotName, replicationSlotNamePrefix) {
				return err
			}
			return nil
		}
		return err
	}

	return nil
}

func (l *LogicalReplicationListener) clearReplicationSlots() error {
	rows, err := l.conn.Query("SELECT slot_name FROM pg_replication_slots")
	if err != nil {
		l.logger.WithError(err).Error("failed to read replication slots.")
		return err
	}

	for rows.Next() {
		var slotName string
		rows.Scan(&slotName)

		if !strings.HasPrefix(slotName, replicationSlotNamePrefix) {
			continue
		}

		l.logger.Infof("Deleting replication slot %s", slotName)
		err = l.replConn.DropReplicationSlot(slotName)
		if err != nil {
			log.WithError(err).Error("failed to delete replication slot", slotName)
		}
	}

	return nil
}

func (l *LogicalReplicationListener) createReplicationSlot(slotName string) error {
	consistentPoint, _, err := l.replConn.CreateReplicationSlotEx(slotName, replicationOutputPlugin)
	if err != nil {
		if pgErr, ok := err.(pgx.PgError); ok {
			switch pgErr.Code {
			// all replication slots in use
			case "53400":
				return ErrAllReplicationSlotsInUse
			// slot already exists
			case "42710":
				return ErrReplicationSlotExists
			default:
				return err
			}
		}
		return err
	}

	if l.replLSN == 0 {
		lsn, err := pgx.ParseLSN(consistentPoint)
		if err != nil {
			l.logger.WithError(err).Error("failed to parse LSN from consistent point")
			return err
		}
		l.replLSN = lsn
	}

	return nil
}

func (l *LogicalReplicationListener) startHeartBeat(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.Tick(time.Duration(l.connHeartbeatIntervalSeconds) * time.Second):
			l.logger.Info("sending heartbeat")
			l.sendStandbyStatus()
		}
	}
}

func (l *LogicalReplicationListener) processMessage(msg *pgx.ReplicationMessage) {
	walMsgRaw := msg.WalMessage.WalData
	var w2jmsg Wal2JSONMessage
	err := json.Unmarshal(walMsgRaw, &w2jmsg)
	if err != nil {
		l.logger.WithError(err).Error("failed to parse wal2json message")
		l.errCh <- fmt.Errorf("failed to parse wal2json: %v", err)
	}

	for _, change := range w2jmsg.Changes {
		cs := &Changeset{
			Kind:   ParseChangesetKind(change.Kind),
			Schema: change.Schema,
			Table:  change.Table,
		}

		newColValues := make([]*ChangesetColumn, len(change.ColumnValues))
		for i, name := range change.ColumnNames {
			newColValues[i] = &ChangesetColumn{
				Column: name,
				Value:  change.ColumnValues[i],
				Type:   change.ColumnTypes[i],
			}
		}
		cs.NewValues = newColValues

		if change.OldKeys != nil {
			oldColValues := make([]*ChangesetColumn, len(change.OldKeys.KeyValues))
			for i, name := range change.OldKeys.KeyNames {
				oldColValues[i] = &ChangesetColumn{
					Column: name,
					Value:  change.OldKeys.KeyValues[i],
					Type:   change.OldKeys.KeyTypes[i],
				}
			}
			cs.OldValues = oldColValues
		}

		l.changesetsCh <- cs
	}

	lsn, err := pgx.ParseLSN(w2jmsg.NextLSN)
	if err != nil {
		l.logger.WithError(err).Error("failed to parse NextLSN from wal2json message")
		l.errCh <- err
	}
	l.replLSN = lsn
}

func (l *LogicalReplicationListener) sendStandbyStatus() {
	status, err := pgx.NewStandbyStatus(l.replLSN)
	if err != nil {
		l.logger.WithError(err).Error("failed to create StandbyStatus")
		l.errCh <- fmt.Errorf("heartbeat failed")
	}

	status.ReplyRequested = 0
	l.logger.Infof("sending StandbyStatus with LSN %d (%s)", l.replLSN, pgx.FormatLSN(l.replLSN))

	err = l.replConn.SendStandbyStatus(status)
	if err != nil {
		l.logger.WithError(err).Error("failed to send StandbyStatus")
		l.errCh <- fmt.Errorf("heartbeat failed")
	}
}
