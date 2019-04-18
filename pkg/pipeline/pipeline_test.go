package pipeline

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/perangel/warp-pipe/pkg/model"
	"github.com/stretchr/testify/assert"
)

func TestPipeline(t *testing.T) {
	p := NewPipeline()

	p.AddStage("remove_pii", func(change *model.Changeset) (*model.Changeset, error) {
		if _, ok := change.NewValues["first_name"]; ok {
			delete(change.NewValues, "first_name")
		}
		return change, nil
	})

	p.AddStage("uppercase_tablename", func(change *model.Changeset) (*model.Changeset, error) {
		change.Table = strings.ToUpper(change.Table)
		return change, nil
	})

	p.AddStage("filter_test_users", func(change *model.Changeset) (*model.Changeset, error) {
		if isTest, ok := change.NewValues["is_test"]; ok {
			if isTest.Value == "TRUE" {
				return nil, nil
			}
		}
		return change, nil
	})

	sourceCh := make(chan *model.Changeset)
	ctx, cancel := context.WithCancel(context.Background())
	outCh, errCh := p.Start(ctx, sourceCh)

	changesetWithPii := &model.Changeset{
		Table: "users",
		NewValues: map[string]*model.ChangesetColumn{
			"first_name": &model.ChangesetColumn{
				Name:  "first_name",
				Type:  "string",
				Value: "Bob",
			},
		},
	}

	changesetForTestUser := &model.Changeset{
		Table: "users",
		NewValues: map[string]*model.ChangesetColumn{
			"first_name": &model.ChangesetColumn{
				Name:  "first_name",
				Type:  "string",
				Value: "Alice",
			},
			"is_test": &model.ChangesetColumn{
				Name:  "is_test",
				Type:  "boolean",
				Value: "TRUE",
			},
		},
	}

	// NOTE: only add 1 to the waitgroup since the test user will be dropped
	for _, change := range []*model.Changeset{changesetWithPii, changesetForTestUser} {
		sourceCh <- change
	}

	var results []*model.Changeset
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
	assert.NotContains(t, results[0].NewValues, "first_name")
	assert.Equal(t, "USERS", results[0].Table)
}
