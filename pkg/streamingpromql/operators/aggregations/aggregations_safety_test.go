// SPDX-License-Identifier: AGPL-3.0-only

package aggregations

import (
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func TestAggregationGroupNativeHistogramSafety(t *testing.T) {
	// This test exists to ensure that AggregationGroup implementations correctly remove FloatHistogram instances they retain,
	// so that retained FloatHistogram instances are not mutated when the HPoint slice is later reused by something else.

	// These are the aggregations that retain the first FloatHistogram instance for an output timestamp.
	groups := map[string]AggregationGroup{
		"sum": &SumAggregationGroup{},
		"avg": &AvgAggregationGroup{},
	}

	for name, group := range groups {
		t.Run(name, func(t *testing.T) {
			memoryConsumptionTracker := limiting.NewMemoryConsumptionTracker(0, nil)
			timeRange := types.NewRangeQueryTimeRange(timestamp.Time(0), timestamp.Time(4), time.Millisecond)

			// First series: all histograms should be nil-ed out after returning, as they're all retained for use.
			histograms, err := types.HPointSlicePool.Get(4, memoryConsumptionTracker)
			require.NoError(t, err)

			h1 := &histogram.FloatHistogram{Sum: 1}
			h2 := &histogram.FloatHistogram{Sum: 2}
			h3 := &histogram.FloatHistogram{Sum: 3}
			h4 := &histogram.FloatHistogram{Sum: 4}
			histograms = append(histograms, promql.HPoint{T: 0, H: h1})
			histograms = append(histograms, promql.HPoint{T: 1, H: h2})
			histograms = append(histograms, promql.HPoint{T: 2, H: h3})
			histograms = append(histograms, promql.HPoint{T: 4, H: h4})
			series := types.InstantVectorSeriesData{Histograms: histograms}

			require.NoError(t, group.AccumulateSeries(series, timeRange, memoryConsumptionTracker, nil))
			require.Equal(t, []promql.HPoint{{T: 0, H: nil}, {T: 1, H: nil}, {T: 2, H: nil}, {T: 4, H: nil}}, series.Histograms, "all histograms retained should be nil-ed out after accumulating series")

			// Second series: all histograms that are not retained should be nil-ed out after returning.
			histograms, err = types.HPointSlicePool.Get(5, memoryConsumptionTracker)
			require.NoError(t, err)
			h5 := &histogram.FloatHistogram{Sum: 5}
			h6 := &histogram.FloatHistogram{Sum: 6}
			h7 := &histogram.FloatHistogram{Sum: 7}
			h8 := &histogram.FloatHistogram{Sum: 8}
			h9 := &histogram.FloatHistogram{Sum: 9}
			histograms = append(histograms, promql.HPoint{T: 0, H: h5})
			histograms = append(histograms, promql.HPoint{T: 1, H: h6})
			histograms = append(histograms, promql.HPoint{T: 2, H: h7})
			histograms = append(histograms, promql.HPoint{T: 3, H: h8})
			histograms = append(histograms, promql.HPoint{T: 4, H: h9})
			series = types.InstantVectorSeriesData{Histograms: histograms}

			require.NoError(t, group.AccumulateSeries(series, timeRange, memoryConsumptionTracker, nil))

			expected := []promql.HPoint{
				{T: 0, H: h5},  // h5 not retained (added to h1)
				{T: 1, H: h6},  // h6 not retained (added to h2)
				{T: 2, H: h7},  // h7 not retained (added to h3)
				{T: 3, H: nil}, // h8 is retained for this point
				{T: 4, H: h9},  // h9 not retained (added to h4)
			}

			require.Equal(t, expected, series.Histograms, "all histograms retained should be nil-ed out after accumulating series")
		})
	}
}
