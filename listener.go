package warppipe

import (
	"context"

	"github.com/jackc/pgx"
)

// Listener is an interface for implementing a changeset listener.
type Listener interface {
	Dial(*pgx.ConnConfig) error
	ListenForChanges(context.Context) (chan *Changeset, chan error)
	Close() error
}

// PersistentListener is an interface for implementing a changeset listener that
// can persist its state.
type PersistentListener interface {
	Listener
	CommitState(context.Context) error
}
