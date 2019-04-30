package warppipe

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPipeline(t *testing.T) {
	p := NewPipeline()

	p.AddStage("remove_pii", func(change *Changeset) (*Changeset, error) {
		var filtered []*ChangesetColumn
		for _, val := range change.NewValues {
			if val.Column == "first_name" {
				continue
			}
			filtered = append(filtered, val)
		}

		change.NewValues = filtered
		return change, nil
	})

	p.AddStage("uppercase_tablename", func(change *Changeset) (*Changeset, error) {
		change.Table = strings.ToUpper(change.Table)
		return change, nil
	})

	p.AddStage("filter_test_users", func(change *Changeset) (*Changeset, error) {
		for _, val := range change.NewValues {
			if val.Column == "is_test" && val.Value == "TRUE" {
				return nil, nil
			}
		}
		return change, nil
	})

	sourceCh := make(chan *Changeset)
	ctx, cancel := context.WithCancel(context.Background())
	outCh, errCh := p.Start(ctx, sourceCh)

	changesetWithPii := &Changeset{
		Table: "users",
		NewValues: []*ChangesetColumn{
			{
				Column: "first_name",
				Type:   "string",
				Value:  "Bob",
			},
			{
				Column: "is_test",
				Type:   "boolean",
				Value:  "FALSE",
			},
		},
	}

	changesetForTestUser := &Changeset{
		Table: "users",
		NewValues: []*ChangesetColumn{
			{
				Column: "first_name",
				Type:   "string",
				Value:  "Alice",
			},
			{
				Column: "is_test",
				Type:   "boolean",
				Value:  "TRUE",
			},
		},
	}

	// NOTE: only add 1 to the waitgroup since the test user will be dropped
	for _, change := range []*Changeset{changesetWithPii, changesetForTestUser} {
		sourceCh <- change
	}

	var results []*Changeset
	go func() {
		for {
			select {
			case change := <-outCh:
				results = append(results, change)
			case err := <-errCh:
				cancel()
				t.Error(err)
			}
		}
	}()

	_ = <-time.After(300 * time.Millisecond)
	assert.Equal(t, 1, len(results))
	assert.Equal(t, 1, len(results[0].NewValues))
	assert.Equal(t, "USERS", results[0].Table)
}
