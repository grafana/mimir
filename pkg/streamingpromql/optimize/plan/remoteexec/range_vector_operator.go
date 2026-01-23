// SPDX-License-Identifier: AGPL-3.0-only

package remoteexec

import (
	"context"

	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

type RangeVectorRemoteExec struct {
	Node               planning.Node
	TimeRange          types.QueryTimeRange
	GroupEvaluator     GroupEvaluator
	Annotations        *annotations.Annotations
	QueryStats         *types.QueryStats
	expressionPosition posrange.PositionRange

	resp                      RangeVectorRemoteExecutionResponse
	finalized                 bool
	stepsReadForCurrentSeries int
}

var _ types.RangeVectorOperator = &RangeVectorRemoteExec{}

func (r *RangeVectorRemoteExec) Prepare(ctx context.Context, params *types.PrepareParams) error {
	var err error
	r.resp, err = r.GroupEvaluator.CreateRangeVectorExecution(ctx, r.Node, r.TimeRange)
	if err != nil {
		return err
	}

	return nil
}

func (r *RangeVectorRemoteExec) AfterPrepare(ctx context.Context) error {
	return r.resp.Start(ctx)
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
	return r.expressionPosition
}

func (r *RangeVectorRemoteExec) Close() {
	if r.resp != nil {
		r.resp.Close()
	}

	r.finalized = true // Don't try to finalize from a closed stream.
}
