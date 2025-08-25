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
	r.resp, err = r.RemoteExecutor.StartInstantVectorExecution(ctx, r.RootPlan, r.Node, r.TimeRange)
	return err
}

func (r *InstantVectorRemoteExec) SeriesMetadata(ctx context.Context) ([]types.SeriesMetadata, error) {
	series, err := r.resp.GetSeriesMetadata(ctx)
	if err != nil {
		return nil, err
	}

	if err := accountForSeriesMetadataMemoryConsumption(series, r.MemoryConsumptionTracker); err != nil {
		return nil, err
	}

	return series, nil
}

func (r *InstantVectorRemoteExec) NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	d, err := r.resp.GetNextSeries(ctx)
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	if err := accountForFPointMemoryConsumption(d.Floats, r.MemoryConsumptionTracker); err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	if err := accountForHPointMemoryConsumption(d.Histograms, r.MemoryConsumptionTracker); err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	return d, nil
}

func (r *InstantVectorRemoteExec) Finalize(ctx context.Context) error {
	if r.finalized {
		return nil
	}

	r.finalized = true

	return finalise(ctx, r.resp, r.Annotations, r.QueryStats)
}

func (r *InstantVectorRemoteExec) ExpressionPosition() posrange.PositionRange {
	return r.Node.ExpressionPosition()
}

func (r *InstantVectorRemoteExec) Close() {
	if r.resp != nil {
		r.resp.Close()
	}
}
