package warppipe

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx"
	log "github.com/sirupsen/logrus"

	"github.com/perangel/warp-pipe/db"
)

const (
	replicationSlotNamePrefix = "wp_"
	replicationOutputPlugin   = "wal2json"
)

var (
	defaultWal2jsonArgs = []string{
		"\"include-lsn\" 'on'",
		"\"pretty-print\" 'off'",
		"\"include-timestamp\" 'on'",
		"\"filter-tables\" 'warp_pipe.*'",
	}
)

// LROption is a LogicalReplicationListener option function
type LROption func(*LogicalReplicationListener)

// ReplSlotName is an option for setting the replication slot name.
func ReplSlotName(name string) LROption {
	return func(l *LogicalReplicationListener) {
		l.replSlotName = name
	}
}

// StartFromLSN is an option for setting the logical sequence number to start from.
func StartFromLSN(lsn uint64) LROption {
	return func(l *LogicalReplicationListener) {
		l.replLSN = lsn
	}
}

// HeartbeatInterval is an option for setting the connection heartbeat interval.
func HeartbeatInterval(seconds int) LROption {
	return func(l *LogicalReplicationListener) {
		l.connHeartbeatIntervalSeconds = seconds
	}
}

// LRLogger is an option for setting the logger
func LRLogger(logger *log.Logger) LROption {
	return func(l *LogicalReplicationListener) {
		l.logger = logger.WithFields(log.Fields{"component": "listener"})
	}
}

// LogicalReplicationListener is a Listener that uses logical replication slots
// to listen for changesets.
type LogicalReplicationListener struct {
	conn                         *pgx.Conn
	replConn                     *pgx.ReplicationConn
	replSlotName                 string
	replLSN                      uint64
	replSnapshot                 string
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

	if l.replSlotName == "" {
		l.replSlotName = fmt.Sprintf("%s%d", replicationSlotNamePrefix, time.Now().Unix())
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

	err = l.clearReplicationSlots()
	if err != nil {
		l.logger.WithError(err).Error("failed to clear replication slots")
		return err
	}

	consistentPoint, snapshot, err := l.replConn.CreateReplicationSlotEx(l.replSlotName, replicationOutputPlugin)
	if err != nil {
		l.logger.WithError(err).Errorf("failed to create replicaiton slot %s", l.replSlotName)
		return err
	}

	lsn, err := pgx.ParseLSN(consistentPoint)
	if err != nil {
		l.logger.WithError(err).Error("failed to parse LSN from consistent point")
		return err
	}

	if l.replLSN == 0 {
		l.replLSN = lsn
	}
	l.replSnapshot = snapshot

	return nil
}

// ListenForChanges returns a channel that emits database changesets.
func (l *LogicalReplicationListener) ListenForChanges(ctx context.Context) (chan *Changeset, chan error) {
	l.logger.Infof("Starting replication for slot '%s' from LSN %s",
		l.replSlotName,
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
	var w2jmsg db.Wal2JSONMessage
	err := json.Unmarshal(walMsgRaw, &w2jmsg)
	if err != nil {
		l.logger.WithError(err).Error("failed to parse wal2json message")
		l.errCh <- fmt.Errorf("failed to parse wal2json: %v", err)
	}

	for _, change := range w2jmsg.Changes {
		cs := &Changeset{
			ID:     change.ID,
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
}

func (l *LogicalReplicationListener) clearReplicationSlots() error {
	rows, err := l.conn.Query("SELECT slot_name FROM pg_replication_slots")
	if err != nil {
		l.logger.WithError(err).Error("Failed to read replication slots.")
		return err
	}

	for rows.Next() {
		var slotName string
		rows.Scan(&slotName)

		if !strings.HasPrefix(slotName, replicationSlotNamePrefix) {
			continue
		}

		// TODO: Handle re-using the same replication slot

		l.logger.Infof("Deleting replication slot %s", slotName)
		err = l.replConn.DropReplicationSlot(slotName)
		if err != nil {
			log.WithError(err).Error("failed to delte replication slot", slotName)
		}
	}

	return nil
}

func (l *LogicalReplicationListener) sendStandbyStatus() {
	status, err := pgx.NewStandbyStatus(l.replLSN)
	if err != nil {
		l.logger.WithError(err).Error("failed to create StandbyStatus")
		l.errCh <- fmt.Errorf("heartbeat failed")
	}

	status.ReplyRequested = 0
	l.logger.Infof("sending StandbyStatus with LSN %s", pgx.FormatLSN(l.replLSN))

	err = l.replConn.SendStandbyStatus(status)
	if err != nil {
		l.logger.WithError(err).Error("failed to send StandbyStatus")
		l.errCh <- fmt.Errorf("heartbeat failed")
	}
}
