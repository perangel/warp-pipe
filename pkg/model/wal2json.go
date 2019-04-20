package model

// Wal2JSONChangeKind is the type for wal2json change kind.
type Wal2JSONChangeKind string

// AsChangesetKind returns the corresponding ChangesetKind that maps to
// a Wal2JSONChange kind.
func (k *Wal2JSONChangeKind) AsChangesetKind() ChangesetKind {
	switch *k {
	case "insert":
		return ChangesetKindInsert
	case "update":
		return ChangesetKindUpdate
	case "delete":
		return ChangesetKindDelete
	default:
		// TODO: should this error?
		return ""
	}
}

// Wal2JSONMessage represents a wal2json message object.
type Wal2JSONMessage struct {
	Changes []*Wal2JSONChange `json:"change"`
	NextLSN string            `json:"nextlsn"`
}

// Wal2JSONChange represents a changeset within a Wal2JSONMessage.
type Wal2JSONChange struct {
	Kind         Wal2JSONChangeKind `json:"kind"`
	Schema       string             `json:"schema"`
	Table        string             `json:"table"`
	ColumnNames  []string           `json:"columnnames"`
	ColumnTypes  []string           `json:"columntypes"`
	ColumnValues []interface{}      `json:"columnvalues"`
	OldKeys      *Wal2JSONOldKeys   `json:"oldkeys"`
}

// Wal2JSONOldKeys represents the `oldkeys` object in a Wal2JSON change
type Wal2JSONOldKeys struct {
	KeyNames  []string      `json:"keynames"`
	KeyTypes  []string      `json:"keytypes"`
	KeyValues []interface{} `json:"keyvalues"`
}
