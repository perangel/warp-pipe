package listener

import (
	"context"

	"github.com/jackc/pgx"
	"github.com/perangel/warp-pipe/pkg/model"
)

// Listener is an interface for implementing a replication listener
type Listener interface {
	Dial(*pgx.ConnConfig) error
	ListenForChanges(context.Context) (chan *model.Changeset, chan error)
	Close() error
}
