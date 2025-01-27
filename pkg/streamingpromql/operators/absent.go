// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/functions.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors.

package operators

import (
	"context"
	"errors"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

// AbsentOperator is an operator that implements the absent() function.
type AbsentOperator struct {
	timeRange                types.QueryTimeRange
	innerExpr                parser.Expr
	inner                    types.InstantVectorOperator
	expressionPosition       posrange.PositionRange
	absentCount              int
	memoryConsumptionTracker *limiting.MemoryConsumptionTracker
}

var _ types.InstantVectorOperator = &AbsentOperator{}

// NewAbsent creates a new AbsentOperator.
func NewAbsent(inner types.InstantVectorOperator, innerExpr parser.Expr, timeRange types.QueryTimeRange, expressionPosition posrange.PositionRange, memoryConsumptionTracker *limiting.MemoryConsumptionTracker) *AbsentOperator {
	return &AbsentOperator{
		timeRange:                timeRange,
		inner:                    inner,
		innerExpr:                innerExpr,
		expressionPosition:       expressionPosition,
		memoryConsumptionTracker: memoryConsumptionTracker,
	}
}

func (s *AbsentOperator) SeriesMetadata(ctx context.Context) ([]types.SeriesMetadata, error) {
	innerMetadata, err := s.inner.SeriesMetadata(ctx)
	if err != nil {
		return nil, err
	}

	defer types.PutSeriesMetadataSlice(innerMetadata)

	if innerMetadata == nil {
		s.absentCount++
	}

	metadata := types.GetSeriesMetadataSlice(s.absentCount)
	for range s.absentCount {
		metadata = append(metadata, types.SeriesMetadata{
			Labels: createLabelsForAbsentFunction(s.innerExpr),
		})
	}
	return metadata, nil
}

func (s *AbsentOperator) NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	output := types.InstantVectorSeriesData{}
	var err error
	output.Floats, err = types.FPointSlicePool.Get(s.absentCount, s.memoryConsumptionTracker)
	if err != nil {
		return output, err
	}

	for range s.absentCount {
		innerResult, err := s.inner.NextSeries(ctx)
		types.PutInstantVectorSeriesData(innerResult, s.memoryConsumptionTracker)

		if err != nil && errors.Is(err, types.EOS) {
			output.Floats = append(output.Floats, promql.FPoint{T: s.timeRange.StartT + s.timeRange.IntervalMilliseconds, F: 1})
		} else {
			return types.InstantVectorSeriesData{}, nil
		}
	}
	return output, nil
}

func (s *AbsentOperator) ExpressionPosition() posrange.PositionRange {
	return s.expressionPosition
}

func (s *AbsentOperator) Close() {
	s.inner.Close()
}

// createLabelsForAbsentFunction returns the labels that are uniquely and exactly matched
// in a given expression. It is used in the absent functions.
// This function is copied from Prometheus
func createLabelsForAbsentFunction(expr parser.Expr) labels.Labels {
	b := labels.NewBuilder(labels.EmptyLabels())

	var lm []*labels.Matcher
	switch n := expr.(type) {
	case *parser.VectorSelector:
		lm = n.LabelMatchers
	case *parser.MatrixSelector:
		lm = n.VectorSelector.(*parser.VectorSelector).LabelMatchers
	default:
		return labels.EmptyLabels()
	}

	// The 'has' map implements backwards-compatibility for historic behaviour:
	// e.g. in `absent(x{job="a",job="b",foo="bar"})` then `job` is removed from the output.
	// Note this gives arguably wrong behaviour for `absent(x{job="a",job="a",foo="bar"})`.
	has := make(map[string]bool, len(lm))
	for _, ma := range lm {
		if ma.Name == labels.MetricName {
			continue
		}
		if ma.Type == labels.MatchEqual && !has[ma.Name] {
			b.Set(ma.Name, ma.Value)
			has[ma.Name] = true
		} else {
			b.Del(ma.Name)
		}
	}

	return b.Labels()
}
