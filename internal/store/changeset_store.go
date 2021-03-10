package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx"
)

var (
	errEventNotFound = errors.New("event not found")
)

const (
	paginationDefaultLimit = 500
)

// Event represents an entry in the events store.
type Event struct {
	ID         int64
	Timestamp  time.Time
	Action     string
	SchemaName string
	TableName  string
	OID        int64
	NewValues  []byte
	OldValues  []byte
}

// String implements the Stringer interface to prettyprint the struct
func (e *Event) String() string {
	newValues := strings.Replace(string(e.NewValues), "\"", "'", -1)
	newValues = strings.Replace(newValues, "\n", "", -1)
	oldValues := strings.Replace(string(e.OldValues), "\"", "'", -1)
	oldValues = strings.Replace(oldValues, "\n", "", -1)
	return fmt.Sprintf("ID: %d, Timestamp: %s, Action: %s, TableName: %s, NewValues: %s, OldValues: %s",
		e.ID, e.Timestamp, e.Action, e.TableName, newValues, oldValues)
}

// EventStore is the interface for providing access to events storage.
type EventStore interface {
	GetByID(ctx context.Context, eventID int64) (*Event, error)
	GetFromOffset(ctx context.Context, offset int64, eventCh chan *Event, doneCh chan bool, errCh chan error)
	GetSinceTimestamp(ctx context.Context, since time.Time, eventCh chan *Event, doneCh chan bool, errCh chan error)
	DeleteBeforeID(ctx context.Context, eventID int64) error
	DeleteBeforeTimestamp(ctx context.Context, since time.Time) error
}

// ChangesetStore is an EventStore for changesets.
type ChangesetStore struct {
	conn *pgx.Conn
}

// NewChangesetStore initializes a new ChangesetStore.
func NewChangesetStore(conn *pgx.Conn) *ChangesetStore {
	return &ChangesetStore{conn: conn}
}

func (s *ChangesetStore) scanRow(rows *pgx.Rows) (*Event, error) {
	var evt Event
	err := rows.Scan(
		&evt.ID,
		&evt.Timestamp,
		&evt.Action,
		&evt.SchemaName,
		&evt.TableName,
		&evt.OID,
		&evt.NewValues,
		&evt.OldValues,
	)

	return &evt, err
}

func (s *ChangesetStore) get(id int64) (*Event, error) {
	events, err := s.query("SELECT * FROM warp_pipe.changesets WHERE id = $1", id)
	if err != nil {
		return nil, err
	}

	if len(events) == 0 {
		return nil, errEventNotFound
	}

	return events[0], nil
}

func (s *ChangesetStore) query(sql string, args ...interface{}) ([]*Event, error) {
	rows, err := s.conn.Query(sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []*Event
	for rows.Next() {
		evt, err := s.scanRow(rows)
		if err != nil {
			return nil, err
		}

		events = append(events, evt)
	}

	if rows.Err() != nil {
		return nil, err
	}

	return events, nil
}

func (s *ChangesetStore) exec(sql string, args ...interface{}) error {
	return nil
}

// GetByID gets an event by ID.
func (s *ChangesetStore) GetByID(ctx context.Context, eventID int64) (*Event, error) {
	return s.get(eventID)
}

// GetFromOffset returns all events starting from a specific offset
func (s *ChangesetStore) GetFromOffset(ctx context.Context, offset int64, eventCh chan *Event, doneCh chan bool, errCh chan error) {
	sql := `
		SELECT
			id,
			ts,
			action,
			schema_name,
			table_name,
			relid,
			new_values,
			old_values
		FROM warp_pipe.changesets
			ORDER BY id
			LIMIT %d
			OFFSET $1`

	for {
		evts, err := s.query(fmt.Sprintf(sql, paginationDefaultLimit), offset)
		if err != nil {
			errCh <- err
			return
		}

		for _, event := range evts {
			eventCh <- event
		}

		if len(evts) < paginationDefaultLimit {
			doneCh <- true
			return
		}

		offset += int64(len(evts))
	}
}

// GetSinceTimestamp returns all events after a given timestamp.
func (s *ChangesetStore) GetSinceTimestamp(ctx context.Context, since time.Time, eventCh chan *Event, doneCh chan bool, errCh chan error) {
	sql := `
		SELECT
			id,
			ts,
			action,
			schema_name,
			table_name,
			relid,
			new_values,
			old_values
		FROM warp_pipe.changesets
			WHERE ts >= $1
			ORDER BY ts
			LIMIT %d
			OFFSET %d`

	offset := 0
	for {
		evts, err := s.query(fmt.Sprintf(sql, paginationDefaultLimit, offset), since)
		if err != nil {
			errCh <- err
			return
		}

		for _, event := range evts {
			eventCh <- event
		}

		if len(evts) < paginationDefaultLimit {
			doneCh <- true
			return
		}

		offset += len(evts)
	}
}

// DeleteBeforeID deletes all events before a given ID.
func (s *ChangesetStore) DeleteBeforeID(ctx context.Context, eventID int64) error {
	return s.exec(`
		DELETE FROM warp_pipe.changesets
			WHERE id < $1`,
		eventID,
	)
}

// DeleteBeforeTimestamp deletes all events before a given timestamp.
func (s *ChangesetStore) DeleteBeforeTimestamp(ctx context.Context, ts time.Time) error {
	return s.exec(`
		DELETE FROM warp_pipe.changesets
			WHERE ts < $1`,
		ts,
	)
}
