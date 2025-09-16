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

type InstantVectorRemoteExec struct {
	RootPlan                 *planning.QueryPlan
	Node                     planning.Node
	TimeRange                types.QueryTimeRange
	RemoteExecutor           RemoteExecutor
	MemoryConsumptionTracker *limiter.MemoryConsumptionTracker
	Annotations              *annotations.Annotations
	QueryStats               *types.QueryStats

	resp      InstantVectorRemoteExecutionResponse
	finalized bool
}

var _ types.InstantVectorOperator = &InstantVectorRemoteExec{}

func (r *InstantVectorRemoteExec) Prepare(ctx context.Context, params *types.PrepareParams) error {
	r.QueryStats = params.QueryStats

	var err error
	r.resp, err = r.RemoteExecutor.StartInstantVectorExecution(ctx, r.RootPlan, r.Node, r.TimeRange, r.MemoryConsumptionTracker, r.QueryStats.EnablePerStepStats)
	return err
}

func (r *InstantVectorRemoteExec) SeriesMetadata(ctx context.Context) ([]types.SeriesMetadata, error) {
	return r.resp.GetSeriesMetadata(ctx)
}

func (r *InstantVectorRemoteExec) NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	return r.resp.GetNextSeries(ctx)
}

func (r *InstantVectorRemoteExec) Finalize(ctx context.Context) error {
	if r.finalized {
		return nil
	}

	r.finalized = true

	return finalize(ctx, r.resp, r.Annotations, r.QueryStats)
}

func (r *InstantVectorRemoteExec) ExpressionPosition() posrange.PositionRange {
	return r.Node.ExpressionPosition()
}

func (r *InstantVectorRemoteExec) Close() {
	if r.resp != nil {
		r.resp.Close()
	}
}
