package pipeline

import (
	"context"

	"github.com/perangel/warp-pipe/pkg/model"
)

type stageFn func(context.Context, <-chan *model.Changeset, chan error) <-chan *model.Changeset

func wrapStageFunc(sFun StageFunc) stageFn {
	f := func(ctx context.Context, inCh <-chan *model.Changeset, errCh chan error) <-chan *model.Changeset {
		outCh := make(chan *model.Changeset)
		go func() {
			defer close(outCh)
			for {
				select {
				case change := <-inCh:
					c, err := sFun(change)
					if err != nil {
						errCh <- err
					}

					if c == nil {
						continue
					}

					outCh <- c
				case <-ctx.Done():
					return
				}
			}
		}()
		return outCh
	}
	return f
}

// StageFunc is a function for processing changesets in a Pipline Stage.
// It accepts a single argument, a Changset, and returns one of:
//     (Changset, nil): If the stage was successful
//     (nil, nil): If the changeset should be dropped (useful for filtering)
//     (nil, error): If there was an error during the stage
type StageFunc func(*model.Changeset) (*model.Changeset, error)

// Stage is a pipeline stage.
type Stage struct {
	Name string
	Fn   stageFn
}

// Pipeline represents a sequence of stages for processing Changesets.
type Pipeline struct {
	Stages []*Stage
	outCh  <-chan *model.Changeset
	errCh  chan error
}

func NewPipeline() *Pipeline {
	return &Pipeline{
		Stages: []*Stage{},
		outCh:  make(chan *model.Changeset),
		errCh:  make(chan error),
	}
}

func (p *Pipeline) AddStage(name string, fn StageFunc) {
	p.Stages = append(p.Stages, &Stage{
		Name: name,
		Fn:   wrapStageFunc(fn),
	})
}

func (p *Pipeline) Start(ctx context.Context, sourceCh <-chan *model.Changeset) (<-chan *model.Changeset, <-chan error) {
	initStage := p.Stages[0]
	outCh := initStage.Fn(ctx, sourceCh, p.errCh)
	for _, stage := range p.Stages[1:] {
		outCh = stage.Fn(ctx, outCh, p.errCh)
	}
	p.outCh = outCh
	return p.outCh, p.errCh
}
