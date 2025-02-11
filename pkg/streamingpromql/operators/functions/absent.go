// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/functions.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors.

package functions

import (
	"context"
	"errors"
	"fmt"

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

func (s *Absent) SeriesMetadata(ctx context.Context) ([]types.SeriesMetadata, error) {
	innerMetadata, err := s.inner.SeriesMetadata(ctx)
	fmt.Printf("Absent SeriesMetadata: %v exp: %v\n", innerMetadata, s.expressionPosition)
	if err != nil {
		return nil, err
	}
	defer types.PutSeriesMetadataSlice(innerMetadata)

	metadata := types.GetSeriesMetadataSlice(1)
	if len(innerMetadata) == 0 {
		metadata = append(metadata, types.SeriesMetadata{
			Labels: createLabelsForAbsentFunction(s.innerExpr),
		})
	} else {
		for range innerMetadata {
			metadata = append(metadata, types.SeriesMetadata{
				Labels: createLabelsForAbsentFunction(s.innerExpr),
			})
		}
	}

	return metadata, nil
}

func (s *Absent) NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	output := types.InstantVectorSeriesData{}
	var err error
	output.Floats, err = types.FPointSlicePool.Get(s.timeRange.StepCount, s.memoryConsumptionTracker)
	// defer types.FPointSlicePool.Put(output.Floats, s.memoryConsumptionTracker)

	if err != nil {
		return output, err
	}

	series, err := s.inner.NextSeries(ctx)
	fmt.Printf("Absent NextSeries: %v exp: %v\n", series, s.expressionPosition)
	defer types.PutInstantVectorSeriesData(series, s.memoryConsumptionTracker)

	if err != nil && errors.Is(err, types.EOS) {
		for step := range s.timeRange.StepCount {
			t := s.timeRange.IndexTime(int64(step))
			output.Floats = append(output.Floats, promql.FPoint{T: t, F: 1})
		}
	} else {
		for step := range s.timeRange.StepCount {
			t := s.timeRange.IndexTime(int64(step))
			hasSample := false

			for _, sample := range series.Floats {
				if sample.T == t {
					hasSample = true
					break
				}
			}

			if !hasSample {
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
