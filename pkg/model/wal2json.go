package model

// Wal2JSONMessage represents a wal2json message object.
type Wal2JSONMessage struct {
	Changes []*Wal2JSONChange `json:"changes"`
}

// Wal2JSONChange represents a changeset within a Wal2JSONMessage.
type Wal2JSONChange struct {
	Kind         string           `json:"kind"`
	Schema       string           `json:"schema"`
	Table        string           `json:"table"`
	ColumnNames  []string         `json:"columnnames"`
	ColumnTypes  []string         `json:"columntypes"`
	ColumnValues []string         `json:"columnvalues"`
	OldKeys      *Wal2JSONOldKeys `json:"oldkeys"`
}

// Wal2JSONOldKeys represents the `oldkeys` object in a Wal2JSON change
type Wal2JSONOldKeys struct {
	KeyNames  []string `json:"keynames"`
	KeyTypes  []string `json:"keytypes"`
	KeyValues []string `json:"keyvalues"`
}
