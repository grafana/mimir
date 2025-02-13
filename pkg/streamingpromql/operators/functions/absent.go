// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/functions.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors.

package functions

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

// Absent is an operator that implements the absent() function.
type Absent struct {
	timeRange                types.QueryTimeRange
	innerExpr                parser.Expr
	inner                    types.InstantVectorOperator
	expressionPosition       posrange.PositionRange
	memoryConsumptionTracker *limiting.MemoryConsumptionTracker
}

var _ types.InstantVectorOperator = &Absent{}

// NewAbsent creates a new Absent.
func NewAbsent(inner types.InstantVectorOperator, innerExpr parser.Expr, timeRange types.QueryTimeRange, expressionPosition posrange.PositionRange, memoryConsumptionTracker *limiting.MemoryConsumptionTracker) *Absent {
	return &Absent{
		timeRange:                timeRange,
		inner:                    inner,
		innerExpr:                innerExpr,
		expressionPosition:       expressionPosition,
		memoryConsumptionTracker: memoryConsumptionTracker,
	}
}

func (a *Absent) SeriesMetadata(ctx context.Context) ([]types.SeriesMetadata, error) {
	innerMetadata, err := a.inner.SeriesMetadata(ctx)
	if err != nil {
		return nil, err
	}
	defer types.PutSeriesMetadataSlice(innerMetadata)

	metadata := types.GetSeriesMetadataSlice(1)
	metadata = append(metadata, types.SeriesMetadata{
		Labels: createLabelsForAbsentFunction(a.innerExpr),
	})

	return metadata, nil
}

func (a *Absent) NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	output := types.InstantVectorSeriesData{}

	var err error
	output.Floats, err = types.FPointSlicePool.Get(a.timeRange.StepCount, a.memoryConsumptionTracker)
	if err != nil {
		return output, err
	}

	series, err := a.inner.NextSeries(ctx)
	defer types.PutInstantVectorSeriesData(series, a.memoryConsumptionTracker)

	for step := range a.timeRange.StepCount {
		t := a.timeRange.IndexTime(int64(step))
		if err != nil && errors.Is(err, types.EOS) {
			output.Floats = append(output.Floats, promql.FPoint{T: t, F: 1})
		} else {
			found := false
			for _, s := range series.Floats {
				if t == s.T {
					found = true
					break
				}
			}
			for _, s := range series.Histograms {
				if t == s.T {
					found = true
					break
				}
			}
			if !found {
				output.Floats = append(output.Floats, promql.FPoint{T: t, F: 1})
			}
		}
	}
	return output, nil
}

func (s *Absent) ExpressionPosition() posrange.PositionRange {
	return s.expressionPosition
}

func (s *Absent) Close() {
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
