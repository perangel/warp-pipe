package listener

import (
	"github.com/jackc/pgx"
	"github.com/perangel/warp-pipe/pkg/model"
)

// Listener is an interface for implementing a replication listener
type Listener interface {
	Listen(config *pgx.ConnConfig) error
	Changes() chan *model.Changeset
	Close() error
}
