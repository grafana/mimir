// SPDX-License-Identifier: AGPL-3.0-only

package remoteexec

import (
	"context"

	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

type RangeVectorRemoteExec struct {
	RootPlan                 *planning.QueryPlan
	Node                     planning.Node
	TimeRange                types.QueryTimeRange
	RemoteExecutor           RemoteExecutor
	MemoryConsumptionTracker *limiter.MemoryConsumptionTracker
	Annotations              *annotations.Annotations
	QueryStats               *types.QueryStats

	resp                      RangeVectorRemoteExecutionResponse
	finalized                 bool
	stepsReadForCurrentSeries int
}

var _ types.RangeVectorOperator = &RangeVectorRemoteExec{}

func (r *RangeVectorRemoteExec) Prepare(ctx context.Context, params *types.PrepareParams) error {
	r.QueryStats = params.QueryStats

	var err error
	r.resp, err = r.RemoteExecutor.StartRangeVectorExecution(ctx, r.RootPlan, r.Node, r.TimeRange, r.MemoryConsumptionTracker, r.QueryStats.EnablePerStepStats)
	return err
}

func (r *RangeVectorRemoteExec) SeriesMetadata(ctx context.Context, _ types.Matchers) ([]types.SeriesMetadata, error) {
	return r.resp.GetSeriesMetadata(ctx)
}

func (r *RangeVectorRemoteExec) NextSeries(ctx context.Context) error {
	r.stepsReadForCurrentSeries = 0
	return r.resp.AdvanceToNextSeries(ctx)
}

func (r *RangeVectorRemoteExec) NextStepSamples(ctx context.Context) (*types.RangeVectorStepData, error) {
	if r.stepsReadForCurrentSeries >= r.TimeRange.StepCount {
		return nil, types.EOS
	}

	r.stepsReadForCurrentSeries++

	return r.resp.GetNextStepSamples(ctx)
}

func (r *RangeVectorRemoteExec) Finalize(ctx context.Context) error {
	if r.finalized {
		return nil
	}

	r.finalized = true

	return finalize(ctx, r.resp, r.Annotations, r.QueryStats)
}

func (r *RangeVectorRemoteExec) ExpressionPosition() posrange.PositionRange {
	return r.Node.ExpressionPosition()
}

func (r *RangeVectorRemoteExec) Close() {
	if r.resp != nil {
		r.resp.Close()
	}
}
