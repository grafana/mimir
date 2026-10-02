// SPDX-License-Identifier: AGPL-3.0-only

//go:build goexperiment.simd

package aggregations

import (
	"fmt"
	"math"
	"math/rand/v2"
	"simd"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/floats"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

func TestAccumulateFloat64sScalar(t *testing.T) {
	testAccumulateFloat64s(t, accumulateFloat64sScalar, 2)
}

func testAccumulateFloat64s(t *testing.T, accumulate func([]float64, []float64, []float64), offset int, lengths ...int) {
	t.Helper()
	testAccumulateFloat64sWithT(t, func(_ *testing.T, values, sums, compensations []float64) {
		accumulate(values, sums, compensations)
	}, offset, lengths...)
}

func TestAccumulateFloat64sScalarRandom(t *testing.T) {
	testAccumulateFloat64sRandom(t, accumulateFloat64sScalar)
}

func testAccumulateFloat64sRandom(t *testing.T, accumulate func([]float64, []float64, []float64)) {
	rng := rand.New(rand.NewPCG(0, 1))
	const steps = 129
	sums, compensations := make([]float64, steps), make([]float64, steps)
	for step := range steps {
		sums[step] = rng.Float64() - 0.5
		compensations[step] = rng.Float64() - 0.5
	}
	wantSums, wantCompensations := slices.Clone(sums), slices.Clone(compensations)
	values := make([]float64, steps)
	// Keep every update finite so NaNs cannot hide later state mismatches.
	for range 256 {
		for step := range steps {
			values[step] = math.Ldexp(rng.Float64()-0.5, rng.IntN(2001)-1000)
			wantSums[step], wantCompensations[step] = floats.KahanSumInc(values[step], wantSums[step], wantCompensations[step])
		}
		accumulate(values, sums, compensations)
		assertFloat64State(t, wantSums, sums)
		assertFloat64State(t, wantCompensations, compensations)
	}
}

func TestAccumulateFloat64sSIMD(t *testing.T) {
	var zero simd.Float64s
	width := zero.Len()
	t.Logf("vector bits=%d, float64 lanes=%d, emulated=%t", simd.VectorBitSize(), width, simd.Emulated())
	lengths := []int{0, 1, 2, 3, 7, 8, 9, 31, 32, 33, width - 1, width, width + 1, 2*width - 1, 2 * width, 2*width + 1}
	slices.Sort(lengths)
	lengths = slices.Compact(lengths)
	for _, offset := range []int{1, width} {
		t.Run(fmt.Sprintf("offset=%d", offset), func(t *testing.T) {
			testAccumulateFloat64s(t, accumulateFloat64sSIMD, offset, lengths...)
		})
	}
}

func TestAccumulateFloat64sSIMDRandom(t *testing.T) {
	testAccumulateFloat64sRandom(t, accumulateFloat64sSIMD)
}

func BenchmarkSumAggregationDenseScalar(b *testing.B) {
	runSumAggregationBenchmarks(b, func(b *testing.B, fixture sumBenchmarkFixture) {
		requireSumBenchmarkContiguous(b, fixture)
		inputs := makeSumDenseBenchmarkInputs(fixture)
		g, tracker := newSumBenchmarkState(b, fixture)

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

func BenchmarkSumAggregationDenseSIMD(b *testing.B) {
	runSumAggregationBenchmarks(b, func(b *testing.B, fixture sumBenchmarkFixture) {
		requireSumBenchmarkContiguous(b, fixture)
		inputs := makeSumDenseBenchmarkInputs(fixture)
		g, tracker := newSumBenchmarkState(b, fixture)

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
		g, tracker := newSumBenchmarkState(b, fixture)

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

type sumDenseBenchmarkInput struct {
	first  int
	values []float64
}

func makeSumDenseBenchmarkInputs(fixture sumBenchmarkFixture) []sumDenseBenchmarkInput {
	inputs := make([]sumDenseBenchmarkInput, len(fixture.series))
	for i, data := range fixture.series {
		inputs[i].first = int(fixture.timeRange.PointIndex(data.Floats[0].T))
		inputs[i].values = make([]float64, len(data.Floats))
		for j, point := range data.Floats {
			inputs[i].values[j] = point.F
		}
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

func newSumBenchmarkState(b *testing.B, fixture sumBenchmarkFixture) (*SumAggregationGroup, *limiter.MemoryConsumptionTracker) {
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
