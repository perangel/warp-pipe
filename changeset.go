package warppipe

import (
	"strings"
	"time"

	"fmt"
)

// ChangesetKind is the type for changeset kinds
type ChangesetKind string

// ChangesetKind constants
const (
	ChangesetKindInsert ChangesetKind = "insert"
	ChangesetKindUpdate ChangesetKind = "update"
	ChangesetKindDelete ChangesetKind = "delete"
)

// ParseChangesetKind parses a changeset kind from a string.
func ParseChangesetKind(kind string) ChangesetKind {
	switch strings.ToLower(kind) {
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

// Changeset represents a changeset for a record on a Postgres table.
type Changeset struct {
	ID        int64              `json:"id"`
	Kind      ChangesetKind      `json:"kind"`
	Schema    string             `json:"schema"`
	Table     string             `json:"table"`
	Timestamp time.Time          `json:"timestamp"`
	NewValues []*ChangesetColumn `json:"new_values"`
	OldValues []*ChangesetColumn `json:"old_values"`
}

func (c *Changeset) getColumnValue(values []*ChangesetColumn, column string) (interface{}, bool) {
	for _, v := range values {
		if v.Column == column {
			return v.Value, true
		}
	}

	return nil, false
}

// String implements Stringer to create a useful string representation of a Changeset.
func (c *Changeset) String() string {
	// While c.newValues is an ordered array, the original data source is a JSON
	// object used as a hashmap, therefore the data is stored as a map in Go
	// which means the field order in the array is randomized.
	return fmt.Sprintf("{id: %d, timestamp: %s, kind: %s, schema: %s, table: %s}", c.ID, c.Timestamp, c.Kind, c.Schema, c.Table)
}

// GetNewColumnValue returns the current value for a column and a bool denoting
// whether a new value is present in the changeset.
func (c *Changeset) GetNewColumnValue(column string) (interface{}, bool) {
	return c.getColumnValue(c.NewValues, column)
}

// GetPreviousColumnValue returns the previous value for a column and a bool denoting
// whether an old value is present in the changeset.
func (c *Changeset) GetPreviousColumnValue(column string) (interface{}, bool) {
	return c.getColumnValue(c.OldValues, column)
}

// ChangesetColumn represents a type and value for a column in a changeset.
type ChangesetColumn struct {
	Column string      `json:"column"`
	Value  interface{} `json:"value"`
	Type   string      `json:"type"`
}
