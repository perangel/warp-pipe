package listener

import (
	"github.com/jackc/pgx"
	"github.com/perangel/warp-pipe/pkg/model"
)

// Listener is an interface for implementing a replication listener
type Listener interface {
	Dial(config *pgx.ConnConfig) error
	ListenForChanges() (chan *model.Changeset, chan error)
	Close() error
}
