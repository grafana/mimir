// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/functions.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors.

package functions

import (
	"context"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

// Absent is an operator that implements the absent() function.
type Absent struct {
	TimeRange                types.QueryTimeRange
	Labels                   labels.Labels
	Inner                    types.InstantVectorOperator
	MemoryConsumptionTracker *limiting.MemoryConsumptionTracker

	expressionPosition posrange.PositionRange
	presence           []bool
	exhausted          bool
}

var _ types.InstantVectorOperator = &Absent{}

// NewAbsent creates a new Absent.
func NewAbsent(inner types.InstantVectorOperator, labels labels.Labels, timeRange types.QueryTimeRange, memoryConsumptionTracker *limiting.MemoryConsumptionTracker, expressionPosition posrange.PositionRange) *Absent {
	return &Absent{
		TimeRange:                timeRange,
		Inner:                    inner,
		Labels:                   labels,
		MemoryConsumptionTracker: memoryConsumptionTracker,
		expressionPosition:       expressionPosition,
	}
}

func (a *Absent) SeriesMetadata(ctx context.Context) ([]types.SeriesMetadata, error) {
	innerMetadata, err := a.Inner.SeriesMetadata(ctx)
	if err != nil {
		return nil, err
	}
	defer types.PutSeriesMetadataSlice(innerMetadata)

	a.presence, err = types.BoolSlicePool.Get(a.TimeRange.StepCount, a.MemoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	// Initialize presence slice
	a.presence = a.presence[:a.TimeRange.StepCount]

	metadata := types.GetSeriesMetadataSlice(1)
	metadata = append(metadata, types.SeriesMetadata{
		Labels: a.Labels,
	})

	for range innerMetadata {
		series, err := a.Inner.NextSeries(ctx)
		if err != nil {
			return nil, err
		}

		for _, s := range series.Floats {
			a.presence[a.TimeRange.PointIndex(s.T)] = true
		}
		for _, s := range series.Histograms {
			a.presence[a.TimeRange.PointIndex(s.T)] = true
		}
		types.PutInstantVectorSeriesData(series, a.MemoryConsumptionTracker)
	}
	return metadata, nil
}

func (a *Absent) NextSeries(_ context.Context) (types.InstantVectorSeriesData, error) {
	output := types.InstantVectorSeriesData{}
	if a.exhausted {
		return output, types.EOS
	}
	a.exhausted = true

	var err error
	for step := range a.TimeRange.StepCount {
		t := a.TimeRange.IndexTime(int64(step))
		if a.presence[step] {
			continue
		}

		if output.Floats == nil {
			output.Floats, err = types.FPointSlicePool.Get(a.TimeRange.StepCount, a.MemoryConsumptionTracker)
			if err != nil {
				return output, err
			}
		}
		output.Floats = append(output.Floats, promql.FPoint{T: t, F: 1})
	}
	return output, nil
}

func (a *Absent) ExpressionPosition() posrange.PositionRange {
	return a.expressionPosition
}

func (a *Absent) Close() {
	a.Inner.Close()
	types.BoolSlicePool.Put(a.presence, a.MemoryConsumptionTracker)
}

// CreateLabelsForAbsentFunction returns the labels that are uniquely and exactly matched
// in a given expression. It is used for the absent and absent_over_time functions.
// This function is copied from Prometheus.
func CreateLabelsForAbsentFunction(expr parser.Expr) labels.Labels {
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
