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
	expressionPosition posrange.PositionRange

	resp                  InstantVectorRemoteExecutionResponse
	finishedReadingCalled bool
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

func (r *InstantVectorRemoteExec) FinishedReading(ctx context.Context) error {
	if r.finishedReadingCalled {
		return nil
	}

	r.finishedReadingCalled = true

	return finishedReading(ctx, r.resp)
}

func (r *InstantVectorRemoteExec) ExpressionPosition() posrange.PositionRange {
	return r.expressionPosition
}

func (r *InstantVectorRemoteExec) Stats(ctx context.Context) (*types.OperatorEvaluationStats, annotations.Annotations, error) {
	return r.resp.Stats(ctx)
}

func (r *InstantVectorRemoteExec) Close() {
	if r.resp != nil {
		r.resp.Close()
	}

	r.finishedReadingCalled = true // Don't try to call FinishedReading from a closed stream.
}
