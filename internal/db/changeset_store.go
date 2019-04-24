package db

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
	DropBeforeID(ctx context.Context, eventID int64) error
	DropBeforeTimestamp(ctx context.Context, since time.Time) error
}

// ChangesetStore is an EventStore for changesets.
type ChangesetStore struct {
	conn *pgx.Conn
}

// NewChangesetStore initializes a new ChangesetStore.
func NewChangesetStore(conn *pgx.Conn) *ChangesetStore {
	return &ChangesetStore{conn: conn}
}

// GetSinceTimestamp returns all events after a give timestamp.
func (s *ChangesetStore) GetSinceTimestamp(ctx context.Context, since time.Time, limit int) ([]*Event, error) {
	rows, err := s.conn.Query(`
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
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []*Event
	for rows.Next() {
		var evt Event
		err = rows.Scan(
			&evt.ID,
			&evt.Timestamp,
			&evt.Action,
			&evt.SchemaName,
			&evt.TableName,
			&evt.OID,
			&evt.NewValues,
			&evt.OldValues,
		)
		if err != nil {
			return nil, err
		}

		events = append(events, &evt)
	}

	if rows.Err() != nil {
		return nil, err
	}

	return events, nil
}
