// SPDX-License-Identifier: AGPL-3.0-only

package remoteexec

import (
	"context"

	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

type InstantVectorRemoteExec struct {
	Node               planning.Node
	TimeRange          types.QueryTimeRange
	GroupEvaluator     GroupEvaluator
	Annotations        *annotations.Annotations
	QueryStats         *types.QueryStats
	expressionPosition posrange.PositionRange

	resp      InstantVectorRemoteExecutionResponse
	finalized bool
}

var _ types.InstantVectorOperator = &InstantVectorRemoteExec{}

func (r *InstantVectorRemoteExec) Prepare(ctx context.Context, params *types.PrepareParams) error {
	var err error
	r.resp, err = r.GroupEvaluator.CreateInstantVectorExecution(ctx, r.Node, r.TimeRange)
	if err != nil {
		return err
	}

	return nil
}

func (r *InstantVectorRemoteExec) AfterPrepare(ctx context.Context) error {
	return r.resp.Start(ctx)
}

func (r *InstantVectorRemoteExec) SeriesMetadata(ctx context.Context, _ types.Matchers) ([]types.SeriesMetadata, error) {
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
	return r.expressionPosition
}

func (r *InstantVectorRemoteExec) Close() {
	if r.resp != nil {
		r.resp.Close()
	}

	r.finalized = true // Don't try to finalize from a closed stream.
}
