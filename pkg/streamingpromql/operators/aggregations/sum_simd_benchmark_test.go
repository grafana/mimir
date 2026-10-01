// SPDX-License-Identifier: AGPL-3.0-only

//go:build goexperiment.simd

package aggregations

import (
	"testing"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func BenchmarkSumAggregationDenseSIMD(b *testing.B) {
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
				accumulateFloat64sSIMD(input.values, g.floatSums[input.first:end], g.floatCompensatingValues[input.first:end])
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

func BenchmarkSumAggregationPackAndDenseSIMD(b *testing.B) {
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
				accumulateFloat64sSIMD(values, g.floatSums[first:end], g.floatCompensatingValues[first:end])
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

func BenchmarkSumAggregationKernelScalar(b *testing.B) {
	benchmarkSumAggregationKernel(b, accumulateFloat64sScalar)
}

func BenchmarkSumAggregationKernelSIMD(b *testing.B) {
	benchmarkSumAggregationKernel(b, accumulateFloat64sSIMD)
}

func benchmarkSumAggregationKernel(b *testing.B, accumulate func([]float64, []float64, []float64)) {
	runSumAggregationBenchmarks(b, func(b *testing.B, fixture sumBenchmarkFixture) {
		requireSumBenchmarkContiguous(b, fixture)
		inputs := makeSumDenseBenchmarkInputs(fixture)
		g, tracker := newSumScalarBenchmarkState(b, fixture)

		for b.Loop() {
			clear(g.floatSums)
			clear(g.floatCompensatingValues)
			for _, input := range inputs {
				end := input.first + len(input.values)
				accumulate(input.values, g.floatSums[input.first:end], g.floatCompensatingValues[input.first:end])
			}
		}

		// Presence is excluded from kernel timing but is needed by the fixture oracle.
		clear(g.floatPresent)
		for _, input := range inputs {
			for i := input.first; i < input.first+len(input.values); i++ {
				g.floatPresent[i] = true
			}
		}
		validateSumBenchmarkState(b, g, fixture)
		reportSumBenchmarkMetrics(b, fixture, tracker)
	})
}
