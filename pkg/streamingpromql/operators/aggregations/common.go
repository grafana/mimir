// SPDX-License-Identifier: AGPL-3.0-only

package aggregations

import (
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

// AggregationGroup accumulates series that have been grouped together and computes the output series data.
type AggregationGroup interface {
	// AccumulateSeries takes in a series as part of the group
	AccumulateSeries(data types.InstantVectorSeriesData, timeRange types.QueryTimeRange, memoryConsumptionTracker *limiting.MemoryConsumptionTracker, emitAnnotationFunc types.EmitAnnotationFunc) error
	// ComputeOutputSeries does any final calculations and returns the grouped series data
	ComputeOutputSeries(timeRange types.QueryTimeRange, memoryConsumptionTracker *limiting.MemoryConsumptionTracker) (types.InstantVectorSeriesData, bool, error)
}

type AggregationGroupFactory func() AggregationGroup

var AggregationGroupFactories = map[parser.ItemType]AggregationGroupFactory{
	parser.AVG:    func() AggregationGroup { return &AvgAggregationGroup{} },
	parser.COUNT:  func() AggregationGroup { return NewCountGroupAggregationGroup(true) },
	parser.GROUP:  func() AggregationGroup { return NewCountGroupAggregationGroup(false) },
	parser.MAX:    func() AggregationGroup { return NewMinMaxAggregationGroup(true) },
	parser.MIN:    func() AggregationGroup { return NewMinMaxAggregationGroup(false) },
	parser.STDDEV: func() AggregationGroup { return NewStddevStdvarAggregationGroup(true) },
	parser.STDVAR: func() AggregationGroup { return NewStddevStdvarAggregationGroup(false) },
	parser.SUM:    func() AggregationGroup { return &SumAggregationGroup{} },
}

// Sentinel value used to indicate a sample has seen an invalid combination of histograms and should be ignored.
//
// Invalid combinations include exponential and custom buckets, and histograms with incompatible custom buckets.
var invalidCombinationOfHistograms = &histogram.FloatHistogram{}
