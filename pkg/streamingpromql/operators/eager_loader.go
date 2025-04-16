// SPDX-License-Identifier: AGPL-3.0-only

package operators

import (
	"context"

	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

type EagerLoader struct {
	Inner                    types.InstantVectorOperator
	MemoryConsumptionTracker *limiting.MemoryConsumptionTracker

	nextSeries chan nextSeriesResult
	stop       chan struct{}
	stopped    chan struct{}

	seriesCount int

	started        bool
	closeRequested bool
}

var _ types.InstantVectorOperator = &EagerLoader{}

func NewEagerLoader(inner types.InstantVectorOperator, memoryConsumptionTracker *limiting.MemoryConsumptionTracker) *EagerLoader {
	return &EagerLoader{
		Inner:                    inner,
		MemoryConsumptionTracker: memoryConsumptionTracker,
	}
}

func (e *EagerLoader) SeriesMetadata(ctx context.Context) ([]types.SeriesMetadata, error) {
	series, err := e.Inner.SeriesMetadata(ctx)
	if err != nil {
		return nil, err
	}

	e.seriesCount = len(series)
	return series, nil
}

func (e *EagerLoader) Start(ctx context.Context) {
	if e.started {
		panic("Start() called multiple times")
		return
	}

	e.started = true
	e.nextSeries = make(chan nextSeriesResult)
	e.stop = make(chan struct{})
	e.stopped = make(chan struct{})

	go e.run(ctx)
}

func (e *EagerLoader) run(ctx context.Context) {
	defer func() {
		e.Inner.Close()
		close(e.stopped)
	}()

	for range e.seriesCount {
		d, err := e.Inner.NextSeries(ctx)

		select {
		case <-ctx.Done():
			types.PutInstantVectorSeriesData(d, e.MemoryConsumptionTracker)
			return
		case <-e.stop:
			types.PutInstantVectorSeriesData(d, e.MemoryConsumptionTracker)
			return
		case e.nextSeries <- nextSeriesResult{data: d, err: err}:
			// Nothing else to do.
		}

		if err != nil {
			return
		}
	}
}

func (e *EagerLoader) NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	select {
	case <-ctx.Done():
		return types.InstantVectorSeriesData{}, context.Cause(ctx)
	case res := <-e.nextSeries:
		return res.data, res.err
	}
}

func (e *EagerLoader) ExpressionPosition() posrange.PositionRange {
	return e.Inner.ExpressionPosition()
}

func (e *EagerLoader) Close() {
	if !e.started {
		e.Inner.Close()
		return
	}

	if !e.closeRequested {
		e.closeRequested = true
		close(e.stop)
	}

	<-e.stopped
}

type nextSeriesResult struct {
	data types.InstantVectorSeriesData
	err  error
}
