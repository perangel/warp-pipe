package pipeline

import (
	"context"

	"github.com/perangel/warp-pipe/pkg/model"
)

type StageFn func(context.Context, <-chan *model.Changeset, chan error) <-chan *model.Changeset

// Stage is a pipeline stage.
type Stage struct {
	Name string
	Fn   StageFn
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

func (p *Pipeline) AddStage(name string, fn StageFn) {
	p.Stages = append(p.Stages, &Stage{
		Name: name,
		Fn:   fn,
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
