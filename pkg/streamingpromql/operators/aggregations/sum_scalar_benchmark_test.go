// SPDX-License-Identifier: AGPL-3.0-only

package aggregations

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

func BenchmarkSumAggregationDenseScalar(b *testing.B) {
	runSumAggregationBenchmarks(b, func(b *testing.B, fixture sumBenchmarkFixture) {
		requireSumBenchmarkContiguous(b, fixture)
		inputs := makeSumDenseBenchmarkInputs(fixture)
		g, tracker := newSumScalarBenchmarkState(b, fixture)

		for b.Loop() {
			clear(g.floatSums)
			clear(g.floatCompensatingValues)
			clear(g.floatPresent)

			for _, input := range inputs {
				end := input.first + len(input.values)
				accumulateFloat64sScalar(input.values, g.floatSums[input.first:end], g.floatCompensatingValues[input.first:end])
				present := g.floatPresent[input.first:end]
				for i := range present {
					present[i] = true
				}
			}
		}

		validateSumBenchmarkState(b, g, fixture)
		reportSumBenchmarkMetrics(b, fixture, tracker)
		var packedBytes uint64
		for _, input := range inputs {
			packedBytes += uint64(cap(input.values)) * types.Float64Size
		}
		b.ReportMetric(float64(packedBytes), "packed-input-B")
	})
}

func BenchmarkSumAggregationPackFloats(b *testing.B) {
	runSumAggregationBenchmarks(b, func(b *testing.B, fixture sumBenchmarkFixture) {
		tracker := limiter.NewUnlimitedMemoryConsumptionTracker(b.Context())
		b.Cleanup(func() { require.Zero(b, tracker.CurrentEstimatedMemoryConsumptionBytes()) })
		scratch := newSumBenchmarkScratch(b, fixture, tracker)

		for b.Loop() {
			for _, data := range fixture.series {
				packFloat64s(scratch, data.Floats)
			}
		}

		last := fixture.series[len(fixture.series)-1].Floats
		for i, p := range last {
			require.Equal(b, p.F, scratch[i])
		}
		reportSumBenchmarkMetrics(b, fixture, tracker)
	})
}

func BenchmarkSumAggregationPackAndDenseScalar(b *testing.B) {
	runSumAggregationBenchmarks(b, func(b *testing.B, fixture sumBenchmarkFixture) {
		requireSumBenchmarkContiguous(b, fixture)
		g, tracker := newSumScalarBenchmarkState(b, fixture)
		scratch := newSumBenchmarkScratch(b, fixture, tracker)

		for b.Loop() {
			clear(g.floatSums)
			clear(g.floatCompensatingValues)
			clear(g.floatPresent)

			for _, data := range fixture.series {
				values := scratch[:len(data.Floats)]
				packFloat64s(values, data.Floats)
				first := fixture.timeRange.PointIndex(data.Floats[0].T)
				end := first + int64(len(values))
				accumulateFloat64sScalar(values, g.floatSums[first:end], g.floatCompensatingValues[first:end])
				present := g.floatPresent[first:end]
				for i := range present {
					present[i] = true
				}
			}
		}

		validateSumBenchmarkState(b, g, fixture)
		reportSumBenchmarkMetrics(b, fixture, tracker)
	})
}

type sumDenseBenchmarkInput struct {
	first  int
	values []float64
}

func makeSumDenseBenchmarkInputs(fixture sumBenchmarkFixture) []sumDenseBenchmarkInput {
	inputs := make([]sumDenseBenchmarkInput, len(fixture.series))
	for i, data := range fixture.series {
		inputs[i].first = int(fixture.timeRange.PointIndex(data.Floats[0].T))
		inputs[i].values = make([]float64, len(data.Floats))
		packFloat64s(inputs[i].values, data.Floats)
	}
	return inputs
}

func requireSumBenchmarkContiguous(t testing.TB, fixture sumBenchmarkFixture) {
	t.Helper()
	for _, data := range fixture.series {
		points := data.Floats
		if len(points) == 0 || points[len(points)-1].T-points[0].T != int64(len(points)-1)*fixture.timeRange.IntervalMilliseconds {
			t.Skip("dense kernel requires contiguous input")
		}
	}
}

func newSumScalarBenchmarkState(b *testing.B, fixture sumBenchmarkFixture) (*SumAggregationGroup, *limiter.MemoryConsumptionTracker) {
	b.Helper()
	tracker := limiter.NewUnlimitedMemoryConsumptionTracker(b.Context())
	g := &SumAggregationGroup{}
	b.Cleanup(func() {
		g.Close(tracker)
		require.Zero(b, tracker.CurrentEstimatedMemoryConsumptionBytes())
	})
	require.NoError(b, g.accumulateFloats(fixture.series[0], fixture.timeRange, tracker))
	return g, tracker
}

func newSumBenchmarkScratch(b *testing.B, fixture sumBenchmarkFixture, tracker *limiter.MemoryConsumptionTracker) []float64 {
	b.Helper()
	var size int
	for _, data := range fixture.series {
		size = max(size, len(data.Floats))
	}
	scratch, err := types.Float64SlicePool.Get(size, tracker)
	require.NoError(b, err)
	scratch = scratch[:size]
	b.Cleanup(func() { types.Float64SlicePool.Put(&scratch, tracker) })
	return scratch
}
