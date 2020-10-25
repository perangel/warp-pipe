package warppipe

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx"
	"github.com/perangel/warp-pipe/internal/store"
	log "github.com/sirupsen/logrus"
)

// NotifyOption is a NotifyListener option function
type NotifyOption func(*NotifyListener)

// StartFromID is an option for setting the startFromID
func StartFromID(changesetID int64) NotifyOption {
	return func(l *NotifyListener) {
		l.startFromID = &changesetID
	}
}

// StartFromTimestamp is an option for setting the startFromTimestamp
func StartFromTimestamp(t time.Time) NotifyOption {
	return func(l *NotifyListener) {
		l.startFromTimestamp = &t
	}
}

// NotifyListener is a listener that uses Postgres' LISTEN/NOTIFY pattern for
// subscribing for subscribing to changeset enqueued in a changesets table.
// For more details see `pkg/schema/changesets`.
type NotifyListener struct {
	conn                   *pgx.Conn
	logger                 *log.Entry
	store                  store.EventStore
	startFromID            *int64
	startFromTimestamp     *time.Time
	lastProcessedTimestamp *time.Time
	changesetsCh           chan *Changeset
	errCh                  chan error
}

// NewNotifyListener returns a new NotifyListener.
func NewNotifyListener(opts ...NotifyOption) *NotifyListener {
	l := &NotifyListener{
		logger:       log.WithFields(log.Fields{"component": "listener"}),
		changesetsCh: make(chan *Changeset),
		errCh:        make(chan error),
	}

	for _, opt := range opts {
		opt(l)
	}

	return l
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
func (l *NotifyListener) ListenForChanges(ctx context.Context) (chan *Changeset, chan error) {
	l.logger.Info("Starting notify listener for `warp_pipe_new_changeset`")
	err := l.conn.Listen("warp_pipe_new_changeset")
	if err != nil {
		l.logger.WithError(err).Fatal("failed to listen on notify channel")
	}

	l.store = store.NewChangesetStore(l.conn)

	// loop - listen for notifications
	go func() {
		if l.startFromID != nil {
			eventCh := make(chan *store.Event)
			doneCh := make(chan bool)
			errCh := make(chan error)

			go l.store.GetSinceID(ctx, *l.startFromID, eventCh, doneCh, errCh)

		processIDLoop:
			for {
				select {
				case c := <-eventCh:
					l.processChangeset(c)
				case err := <-errCh:
					log.WithError(err).Fatal("encountered an error while reading changesets")
					l.errCh <- err
				case <-doneCh:
					close(errCh)
					close(eventCh)
					break processIDLoop
				}
			}
		} else if l.startFromTimestamp != nil {
			eventCh := make(chan *store.Event)
			doneCh := make(chan bool)
			errCh := make(chan error)

			go l.store.GetSinceTimestamp(ctx, *l.startFromTimestamp, eventCh, doneCh, errCh)

		processTimestampLoop:
			for {
				select {
				case c := <-eventCh:
					l.processChangeset(c)
				case err := <-errCh:
					log.WithError(err).Fatal("encountered an error while reading changesets")
					l.errCh <- err
				case <-doneCh:
					close(errCh)
					close(eventCh)
					break processTimestampLoop
				}
			}
		}

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

			l.processMessage(msg)
		}
	}()

	return l.changesetsCh, l.errCh
}

func (l *NotifyListener) processMessage(msg *pgx.Notification) {
	// payload is <event_id>_<timestamp>
	parts := strings.Split(msg.Payload, "_")
	eventID, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		log.WithError(err).WithField("changeset_id", parts[0]).
			Error("failed to parse changeset ID from notification payload")
		l.errCh <- err
	}

	event, err := l.store.GetByID(context.Background(), eventID)
	if err != nil {
		log.WithError(err).WithField("changeset_id", parts[0]).Error("failed to get changeset from store")
		l.errCh <- err
	}

	l.processChangeset(event)
}

func (l *NotifyListener) processChangeset(event *store.Event) {
	cs := &Changeset{
		Kind:      ParseChangesetKind(event.Action),
		Schema:    event.SchemaName,
		Table:     event.TableName,
		Timestamp: event.Timestamp,
	}

	if event.NewValues != nil {
		var newValues map[string]interface{}
		err := json.Unmarshal(event.NewValues, &newValues)
		if err != nil {
			l.errCh <- err
		}

		for k, v := range newValues {
			col := &ChangesetColumn{
				Column: k,
				Value:  v,
			}
			cs.NewValues = append(cs.NewValues, col)
		}
	}

	if event.OldValues != nil {
		var oldValues map[string]interface{}
		err := json.Unmarshal(event.OldValues, &oldValues)
		if err != nil {
			l.errCh <- err
		}

		for k, v := range oldValues {
			col := &ChangesetColumn{
				Column: k,
				Value:  v,
			}
			cs.OldValues = append(cs.OldValues, col)
		}
	}

	l.lastProcessedTimestamp = &event.Timestamp
	l.changesetsCh <- cs
}

// Close closes the database connection.
func (l *NotifyListener) Close() error {
	if err := l.conn.Close(); err != nil {
		log.WithError(err).Error("Error when closing database connection.")
		return err
	}

	return nil
}
