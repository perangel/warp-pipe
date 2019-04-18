package pipeline

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/perangel/warp-pipe/pkg/model"
	"github.com/stretchr/testify/assert"
)

func TestPipeline(t *testing.T) {
	p := NewPipeline()

	p.AddStage("remove_pii", func(ctx context.Context, inCh <-chan *model.Changeset, errCh chan error) <-chan *model.Changeset {
		outCh := make(chan *model.Changeset)
		go func() {
			defer close(outCh)
			for {
				select {
				case change := <-inCh:
					if _, ok := change.NewValues["first_name"]; ok {
						delete(change.NewValues, "first_name")
					}
					outCh <- change
				case <-ctx.Done():
					return
				}
			}
		}()
		return outCh
	})

	p.AddStage("uppercase_tablename", func(ctx context.Context, inCh <-chan *model.Changeset, errCh chan error) <-chan *model.Changeset {
		outCh := make(chan *model.Changeset)
		go func() {
			defer close(outCh)
			for {
				select {
				case change := <-inCh:
					change.Table = strings.ToUpper(change.Table)
					outCh <- change
				case <-ctx.Done():
					return
				}
			}
		}()
		return outCh

	})

	var wg sync.WaitGroup

	sourceCh := make(chan *model.Changeset)
	outCh, errCh := p.Start(context.Background(), sourceCh)
	go func() {
		for {
			select {
			case msg := <-outCh:
				assert.NotContains(t, msg.NewValues, "first_name")
				assert.Equal(t, "USERS", msg.Table)
				wg.Done()
			case err := <-errCh:
				t.Error(err)
				t.FailNow()
			}
		}
	}()

	changeSetWithPii := &model.Changeset{
		Table: "users",
		NewValues: map[string]*model.ChangesetColumn{
			"first_name": &model.ChangesetColumn{
				Name:  "first_name",
				Type:  "string",
				Value: "Bob",
			},
		},
	}

	wg.Add(1)
	sourceCh <- changeSetWithPii

	wg.Wait()
}
