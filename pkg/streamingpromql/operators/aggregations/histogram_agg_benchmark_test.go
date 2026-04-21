// SPDX-License-Identifier: AGPL-3.0-only

package aggregations

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/promql"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

// makeTestFloatHistogram creates a FloatHistogram with the given schema and number of positive buckets.
// All buckets are in a single contiguous span.
func makeTestFloatHistogram(schema int32, numBuckets int) *histogram.FloatHistogram {
	buckets := make([]float64, numBuckets)
	for i := range buckets {
		buckets[i] = float64(i + 1)
	}

	h := &histogram.FloatHistogram{
		Schema:        schema,
		Count:         100,
		Sum:           18.4,
		ZeroThreshold: 0.001,
		ZeroCount:     2,
		PositiveSpans: []histogram.Span{
			{Offset: 0, Length: uint32(numBuckets)},
		},
		PositiveBuckets: buckets,
		NegativeSpans: []histogram.Span{
			{Offset: 0, Length: uint32(numBuckets)},
		},
		NegativeBuckets: make([]float64, numBuckets),
	}
	copy(h.NegativeBuckets, buckets)
	return h
}

func noopEmitAnnotation(_ types.AnnotationGenerator) {}

func runHistogramAggregationBenchmark(b *testing.B, newGroup func() AggregationGroup) {
	b.Helper()
	for _, numBuckets := range []int{4, 32, 128} {
		for _, numSeries := range []int{10, 100} {
			b.Run(fmt.Sprintf("buckets=%d/series=%d", numBuckets, numSeries), func(b *testing.B) {
				memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(context.Background(), 0, nil, "")
				timeRange := types.NewRangeQueryTimeRange(time.Unix(0, 0), time.Unix(60, 0), time.Minute) // 2 steps

				// Pre-build series data slices (one per series).
				allSeriesData := make([]types.InstantVectorSeriesData, numSeries)
				for s := range numSeries {
					h := makeTestFloatHistogram(0, numBuckets)
					hpoints, _ := types.HPointSlicePool.Get(timeRange.StepCount, memoryConsumptionTracker)
					for step := range timeRange.StepCount {
						hpoints = append(hpoints, promql.HPoint{
							T: timeRange.IndexTime(int64(step)),
							H: h.Copy(),
						})
					}
					allSeriesData[s] = types.InstantVectorSeriesData{Histograms: hpoints}
				}

				b.ResetTimer()
				b.ReportAllocs()

				for b.Loop() {
					g := newGroup()

					for s := range numSeries {
						// Rebuild series data each iteration since AccumulateSeries may mutate it.
						data := types.InstantVectorSeriesData{
							Histograms: make([]promql.HPoint, len(allSeriesData[s].Histograms)),
						}
						for i, hp := range allSeriesData[s].Histograms {
							data.Histograms[i] = promql.HPoint{T: hp.T, H: hp.H.Copy()}
						}

						_ = g.AccumulateSeries(data, timeRange, memoryConsumptionTracker, noopEmitAnnotation, 0, true)
					}

					g.Close(memoryConsumptionTracker)
				}
			})
		}
	}
}

func BenchmarkHistogramAggregationSum(b *testing.B) {
	runHistogramAggregationBenchmark(b, func() AggregationGroup { return &SumAggregationGroup{} })
}

func BenchmarkHistogramAggregationAvg(b *testing.B) {
	runHistogramAggregationBenchmark(b, func() AggregationGroup { return &AvgAggregationGroup{} })
}
