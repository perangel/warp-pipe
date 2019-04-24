package store

import (
	"context"
	"time"

	"github.com/jackc/pgx"
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

// EventStore is the interface for providing access to events storage.
type EventStore interface {
	GetSinceID(ctx context.Context, eventID int64, limit int) ([]*Event, error)
	GetSinceTimestamp(ctx context.Context, since time.Time, limit int) ([]*Event, error)
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

// GetSinceID returns all events after a given ID.
func (s *ChangesetStore) GetSinceID(ctx context.Context, eventID int64, limit int) ([]*Event, error) {
	return s.query(`
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
			WHERE id > $1
			ORDER BY id 
			LIMIT $2`,
		eventID,
		limit,
	)
}

// GetSinceTimestamp returns all events after a given timestamp.
func (s *ChangesetStore) GetSinceTimestamp(ctx context.Context, since time.Time, limit int) ([]*Event, error) {
	return s.query(`
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
			WHERE ts > $1
			ORDER BY ts
			LIMIT $2`,
		since,
		limit,
	)
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
