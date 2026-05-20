// SPDX-License-Identifier: AGPL-3.0-only

package sharding

import (
	"context"
	"fmt"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/streamingpromql/operators/functions"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func init() {
	parser.Functions[AvgFunction.Name] = AvgFunction

	if err := functions.RegisterFunction(functions.FUNCTION_SHARDING_AVG, AvgFunction.Name, AvgFunction.ReturnType, shardedAvgFactory); err != nil {
		panic(err)
	}
}

func shardedAvgFactory(args []types.Operator, _ labels.Labels, _ *planning.OperatorParameters, _ posrange.PositionRange, _ types.QueryTimeRange) (types.Operator, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("expected exactly 1 argument for %s, but got %d", AvgFunction.Name, len(args))
	}

	inner, ok := args[0].(types.InstantVectorOperator)
	if !ok {
		return nil, fmt.Errorf("expected InstantVectorOperator for %s, but got %T", AvgFunction.Name, args[0])
	}

	return &ShardedAvg{Inner: inner}, nil
}

var AvgFunction = &parser.Function{
	Name:       "__sharded_avg__",
	ArgTypes:   []parser.ValueType{parser.ValueTypeVector},
	ReturnType: parser.ValueTypeVector,
}

// ShardedAvg is an operator that passes through all data from its inner operator unchanged,
// but halves all sample statistics reported by the inner operator.
//
// This corrects for the double-counting that occurs when both the sum and count legs
// of a sharded avg() expression process the same underlying data.
type ShardedAvg struct {
	Inner types.InstantVectorOperator
}

func (a *ShardedAvg) Stats(ctx context.Context) (*types.OperatorEvaluationStats, annotations.Annotations, error) {
	stats, annos, err := a.Inner.Stats(ctx)
	if err != nil {
		return nil, nil, err
	}

	stats.HalveCounts()

	return stats, annos, nil
}

func (a *ShardedAvg) Prepare(ctx context.Context, params *types.PrepareParams) error {
	return a.Inner.Prepare(ctx, params)
}

func (a *ShardedAvg) AfterPrepare(ctx context.Context) error {
	return a.Inner.AfterPrepare(ctx)
}

func (a *ShardedAvg) SeriesMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, error) {
	return a.Inner.SeriesMetadata(ctx, matchers)
}

func (a *ShardedAvg) NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	return a.Inner.NextSeries(ctx)
}

func (a *ShardedAvg) FinishedReading(ctx context.Context) error {
	return a.Inner.FinishedReading(ctx)
}

func (a *ShardedAvg) ExpressionPosition() posrange.PositionRange {
	return a.Inner.ExpressionPosition()
}

func (a *ShardedAvg) Close() {
	a.Inner.Close()
}

var _ types.InstantVectorOperator = &ShardedAvg{}
