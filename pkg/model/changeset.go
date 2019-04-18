package model

// ChangesetKind is the type for changeset kinds
type ChangesetKind string

// ChangsetKind constants
const (
	ChangesetKindInsert ChangesetKind = "insert"
	ChangesetKindUpdate ChangesetKind = "update"
	ChangesetKindDelete ChangesetKind = "delete"
)

// Changeset represents a changeset for a record on a Postgres table.
type Changeset struct {
	Kind      ChangesetKind               `json:"kind"`
	Schema    string                      `json:"schema"`
	Table     string                      `json:"table"`
	NewValues map[string]*ChangesetColumn `json:"new_values"`
	OldValues map[string]*ChangesetColumn `json:"old_values"`
}

// ChangesetColumn represents a type and value for a column in a changset.
type ChangesetColumn struct {
	Name  string      `json:"name"`
	Type  string      `json:"type"`
	Value interface{} `json:"value"`
}
