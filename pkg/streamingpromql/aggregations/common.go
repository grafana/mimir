// SPDX-License-Identifier: AGPL-3.0-only

package aggregations

import (
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/grafana/mimir/pkg/streamingpromql/functions"
	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

// AggregationGroup accumulates series that have been grouped together and computes the output series data.
type AggregationGroup interface {
	// AccumulateSeries takes in a series as part of the group
	AccumulateSeries(data types.InstantVectorSeriesData, steps int, start int64, interval int64, memoryConsumptionTracker *limiting.MemoryConsumptionTracker, emitAnnotationFunc functions.EmitAnnotationFunc) error
	// ComputeOutputSeries does any final calculations and returns the grouped series data
	ComputeOutputSeries(start int64, interval int64, memoryConsumptionTracker *limiting.MemoryConsumptionTracker) (types.InstantVectorSeriesData, bool, error)
}

type AggregationGroupFactory func() AggregationGroup

var AggregationGroupFactories = map[parser.ItemType]AggregationGroupFactory{
	parser.MAX: func() AggregationGroup { return NewMinMaxAggregationGroup(true) },
	parser.MIN: func() AggregationGroup { return NewMinMaxAggregationGroup(false) },
	parser.SUM: func() AggregationGroup { return &SumAggregationGroup{} },
}

// Sentinel value used to indicate a sample has seen an invalid combination of histograms and should be ignored.
//
// Invalid combinations include exponential and custom buckets, and histograms with incompatible custom buckets.
var invalidCombinationOfHistograms = &histogram.FloatHistogram{}
