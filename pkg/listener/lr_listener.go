package listener

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx"
	"github.com/perangel/warp-pipe/pkg/model"
	log "github.com/sirupsen/logrus"
)

const (
	replicationSlotNamePrefix = "wp_"
	replicationOutputPlugin   = "wal2json"
)

var (
	wal2jsonArgs = []string{"\"include-lsn\" 'on'", "\"pretty-print\" 'off'"}
)

// LogicalReplicationListener is a Listener that uses logical replication slots
// to listen for changesets.
type LogicalReplicationListener struct {
	conn                         *pgx.Conn
	replConn                     *pgx.ReplicationConn
	replSlotName                 string
	replLSN                      uint64
	replSnapshot                 string
	connHeartbeatIntervalSeconds int
	changesetsCh                 chan *model.Changeset
	errCh                        chan error
	logger                       *log.Entry
}

// NewLogicalReplicationListener returns a new LogicalReplicationListener.
func NewLogicalReplicationListener() Listener {
	return &LogicalReplicationListener{
		logger:                       log.WithFields(log.Fields{"component": "listener"}),
		changesetsCh:                 make(chan *model.Changeset),
		errCh:                        make(chan error),
		connHeartbeatIntervalSeconds: 10, // TODO: make configurable
	}
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

	l.replSlotName = fmt.Sprintf("%s%d", replicationSlotNamePrefix, time.Now().Unix())
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

	l.replLSN = lsn
	l.replSnapshot = snapshot

	return nil
}

// ListenForChanges returns a channel that emits database changesets.
func (l *LogicalReplicationListener) ListenForChanges() (chan *model.Changeset, chan error) {
	l.logger.Infof("starting repliction for slot '%s' from LSN %s",
		l.replSlotName,
		pgx.FormatLSN(l.replLSN),
	)

	err := l.replConn.StartReplication(l.replSlotName, l.replLSN, -1, wal2jsonArgs...)
	if err != nil {
		l.logger.WithError(err).Error("failed to start replication")
		l.errCh <- err
		// TODO: implement
		//l.shutdown()
	}

	go l.startHeartBeat()

	// loop - listen for messages
	go func() {
		for {
			if !l.replConn.IsAlive() {
				log.WithField("conn_err", l.replConn.CauseOfDeath()).Error(
					"replication connection is down",
				)
			}

			ctx := context.Background()
			msg, err := l.replConn.WaitForReplicationMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					l.logger.Warn("context was canceled")
					// TODO: should we bail here?
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
		l.logger.WithError(err).Error("Error when closing database replication connection.")
		return err
	}

	if err := l.conn.Close(); err != nil {
		l.logger.WithError(err).Error("Error when closing database connection.")
		return err
	}

	return nil
}

func (l *LogicalReplicationListener) startHeartBeat() {
	for {
		select {
		case <-time.Tick(time.Duration(l.connHeartbeatIntervalSeconds) * time.Second):
			l.logger.Info("connection heartbeat")
		}
	}
}

func (l *LogicalReplicationListener) processMessage(msg *pgx.ReplicationMessage) {
	walMsgRaw := msg.WalMessage.WalData
	var w2jmsg model.Wal2JSONMessage
	err := json.Unmarshal(walMsgRaw, &w2jmsg)
	if err != nil {
		l.logger.WithError(err).Error("failed to parse wal2json message")
		l.errCh <- fmt.Errorf("failed to parse wal2json: %v", err)
	}

	for _, change := range w2jmsg.Changes {
		cs := &model.Changeset{
			Kind:   change.Kind.AsChangesetKind(),
			Schema: change.Schema,
			Table:  change.Table,
		}

		newColValues := make(map[string]*model.ChangesetColumn, len(change.ColumnValues))
		for i, name := range change.ColumnNames {
			newColValues[name] = &model.ChangesetColumn{
				Type:  change.ColumnTypes[i],
				Value: change.ColumnValues[i],
			}
		}
		cs.NewValues = newColValues

		if change.OldKeys != nil {
			oldColValues := make(map[string]*model.ChangesetColumn, len(change.OldKeys.KeyValues))
			for i, name := range change.OldKeys.KeyNames {
				oldColValues[name] = &model.ChangesetColumn{
					Type:  change.OldKeys.KeyTypes[i],
					Value: change.OldKeys.KeyValues[i],
				}
			}
			cs.OldValues = oldColValues
		}

		//l.logger.Infof("received changeset: %+v", (cs))
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

		l.logger.Infof("Deleting replicaiton slot %s", slotName)
		err = l.replConn.DropReplicationSlot(slotName)
		if err != nil {
			log.WithError(err).Error("Failed to delte replication slot", slotName)
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
