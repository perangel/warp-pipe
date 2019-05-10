package warppipe

import (
	"context"
)

type stageFn func(context.Context, <-chan *Changeset, chan error) chan *Changeset

// makeStageFunc wraps a StageFunc and returns a stageFn.
func makeStageFunc(sFun StageFunc) stageFn {
	f := func(ctx context.Context, inCh <-chan *Changeset, errCh chan error) chan *Changeset {
		outCh := make(chan *Changeset)
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

// StageFunc is a function for processing changesets in a pipeline Stage.
// It accepts a single argument, a Changset, and returns one of:
//     (Changeset, nil): If the stage was successful
//     (nil, nil): If the changeset should be dropped (useful for filtering)
//     (nil, error): If there was an error during the stage
type StageFunc func(*Changeset) (*Changeset, error)

// Stage is a pipeline stage.
type Stage struct {
	Name string
	Fn   stageFn
}

// Pipeline represents a sequence of stages for processing Changesets.
type Pipeline struct {
	stages []*Stage
	outCh  chan *Changeset
	errCh  chan error
}

// NewPipeline returns a new Pipeline.
func NewPipeline() *Pipeline {
	return &Pipeline{
		stages: []*Stage{},
		outCh:  make(chan *Changeset),
		errCh:  make(chan error),
	}
}

// AddStage adds a new Stage to the pipeline
func (p *Pipeline) AddStage(name string, fn StageFunc) {
	p.stages = append(p.stages, &Stage{
		Name: name,
		Fn:   makeStageFunc(fn),
	})
}

// Start starts the pipeline, consuming off of a source chan that emits *Changeset.
func (p *Pipeline) Start(ctx context.Context, sourceCh chan *Changeset) (chan *Changeset, <-chan error) {
	if len(p.stages) > 0 {
		initStage := p.stages[0]
		outCh := initStage.Fn(ctx, sourceCh, p.errCh)
		for _, stage := range p.stages[1:] {
			outCh = stage.Fn(ctx, outCh, p.errCh)
		}
		p.outCh = outCh
	} else {
		p.outCh = sourceCh
	}

	return p.outCh, p.errCh
}
