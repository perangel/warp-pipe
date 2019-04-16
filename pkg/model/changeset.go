package model

import "time"

// Changeset represents a changeset for a record on a Postgres table.
type Changeset struct {
	Table     string    `json:"table"`
	Columns   []string  `json:"columns"`
	Values    []string  `json:"values"`
	LSN       *string   `json:"lsn"`
	Timestamp time.Time `json:"ts"`
}
