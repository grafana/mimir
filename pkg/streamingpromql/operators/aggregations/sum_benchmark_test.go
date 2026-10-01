// SPDX-License-Identifier: AGPL-3.0-only

package aggregations

import (
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

func BenchmarkSumAggregationAccumulateFloats(b *testing.B) {
	runSumAggregationBenchmarks(b, func(b *testing.B, fixture sumBenchmarkFixture) {
		tracker := limiter.NewUnlimitedMemoryConsumptionTracker(b.Context())
		g := &SumAggregationGroup{}
		b.Cleanup(func() {
			g.Close(tracker)
			require.Zero(b, tracker.CurrentEstimatedMemoryConsumptionBytes())
		})
		require.NoError(b, g.accumulateFloats(fixture.series[0], fixture.timeRange, tracker))

		for b.Loop() {
			// Keep summation depth fixed without per-iteration timer manipulation.
			clear(g.floatSums)
			clear(g.floatCompensatingValues)
			clear(g.floatPresent)

			for _, data := range fixture.series {
				if err := g.accumulateFloats(data, fixture.timeRange, tracker); err != nil {
					b.Fatal(err)
				}
			}
		}

		validateSumBenchmarkState(b, g, fixture)
		reportSumBenchmarkMetrics(b, fixture, tracker)
	})
}

func BenchmarkSumAggregationGroup(b *testing.B) {
	runSumAggregationBenchmarks(b, func(b *testing.B, fixture sumBenchmarkFixture) {
		tracker := limiter.NewUnlimitedMemoryConsumptionTracker(b.Context())
		factory := AggregationGroupFactories[parser.SUM]
		var checksum float64

		for b.Loop() {
			g := factory.Create()
			if err := tracker.IncreaseMemoryConsumption(factory.StructSize(), limiter.AggregationGroup); err != nil {
				b.Fatal(err)
			}

			for i, data := range fixture.series {
				if err := g.AccumulateSeries(data, fixture.timeRange, tracker, nil, uint(len(fixture.series)-i), false); err != nil {
					b.Fatal(err)
				}
			}

			output, mixed, err := g.ComputeOutputSeries(types.ScalarData{}, fixture.timeRange, tracker)
			if err != nil {
				b.Fatal(err)
			}
			if mixed || len(output.Floats) != len(fixture.expected) || len(output.Histograms) != 0 {
				b.Fatal("unexpected sum output shape")
			}
			checksum = output.Floats[0].F + output.Floats[len(output.Floats)-1].F

			tracker.DecreaseMemoryConsumption(factory.StructSize(), limiter.AggregationGroup)
			g.Close(tracker)
			types.PutInstantVectorSeriesData(output, tracker)
		}

		require.Equal(b, fixture.expected[0].F+fixture.expected[len(fixture.expected)-1].F, checksum)
		require.Zero(b, tracker.CurrentEstimatedMemoryConsumptionBytes())
		reportSumBenchmarkMetrics(b, fixture, tracker)
	})
}

func runSumAggregationBenchmarks(b *testing.B, run func(*testing.B, sumBenchmarkFixture)) {
	// Tests enable pool mangling, which would add work absent from production queries.
	mangling := types.EnableManglingReturnedSlices.Swap(false)
	b.Cleanup(func() { types.EnableManglingReturnedSlices.Store(mangling) })

	for _, c := range sumBenchmarkCases() {
		b.Run(c.name(), func(b *testing.B) {
			// Borrow fixtures across iterations; exclude decoding, input allocation, and input pool returns.
			fixture := newSumBenchmarkFixture(c)
			validateSumBenchmarkFixture(b, fixture)
			b.ReportAllocs()
			run(b, fixture)
		})
	}
}

func reportSumBenchmarkMetrics(b *testing.B, fixture sumBenchmarkFixture, tracker *limiter.MemoryConsumptionTracker) {
	b.ReportMetric(float64(fixture.samples), "samples/op")
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(fixture.samples), "ns/sample")
	b.ReportMetric(float64(fixture.inputBytes), "input-B")
	// Fixture storage is excluded: this is the additional tracked memory used by the group.
	b.ReportMetric(float64(tracker.PeakEstimatedMemoryConsumptionBytes()), "tracked-peak-B")
}

type sumBenchmarkCase struct {
	steps  int
	series int
	layout string
	values string
}

func (c sumBenchmarkCase) name() string {
	return fmt.Sprintf("steps=%d/series=%d/layout=%s/values=%s", c.steps, c.series, c.layout, c.values)
}

func sumBenchmarkCases() []sumBenchmarkCase {
	var cases []sumBenchmarkCase
	for _, steps := range []int{1, 8, 128, 1000} {
		for _, series := range []int{1, 16, 256} {
			cases = append(cases, sumBenchmarkCase{steps, series, "dense", "positive"})
		}
	}
	for _, series := range []int{16, 256} {
		cases = append(cases, sumBenchmarkCase{10000, series, "dense", "positive"})
	}
	for _, steps := range []int{128, 1000} {
		for _, series := range []int{16, 256} {
			for _, layout := range []string{"partial", "gaps"} {
				cases = append(cases, sumBenchmarkCase{steps, series, layout, "positive"})
			}
		}
	}
	for _, steps := range []int{8, 128, 1000} {
		for _, series := range []int{16, 256} {
			cases = append(cases, sumBenchmarkCase{steps, series, "dense", "mixed_magnitude"})
		}
	}
	return cases
}

type sumBenchmarkFixture struct {
	timeRange  types.QueryTimeRange
	series     []types.InstantVectorSeriesData
	expected   []promql.FPoint
	samples    int
	inputBytes uint64
}

func newSumBenchmarkFixture(c sumBenchmarkCase) sumBenchmarkFixture {
	start := time.Unix(0, 0)
	interval := 15 * time.Second
	timeRange := types.NewRangeQueryTimeRange(start, start.Add(time.Duration(c.steps-1)*interval), interval)
	if c.steps == 1 {
		timeRange = types.NewInstantQueryTimeRange(start)
	}
	fixture := sumBenchmarkFixture{
		timeRange: timeRange,
		series:    make([]types.InstantVectorSeriesData, c.series),
	}

	// Integer eighths give an independent, exact oracle for these finite workloads.
	sumUnits := make([]int64, c.steps)
	present := make([]bool, c.steps)
	for seriesIdx := range c.series {
		first, end := 0, c.steps
		if c.layout == "partial" {
			offset := seriesIdx % max(1, c.steps/16)
			first = c.steps/4 + offset
			end = c.steps - c.steps/4 - offset
		}
		points := make([]promql.FPoint, 0, end-first)
		for step := first; step < end; step++ {
			switch c.layout {
			case "dense", "partial":
			case "gaps":
				if step%8 == 7 || step%4 == seriesIdx%3 {
					continue
				}
			default:
				panic("unknown benchmark layout")
			}

			units := int64(1 + (step*17+seriesIdx*13)%63)
			switch c.values {
			case "positive":
			case "mixed_magnitude":
				// These terms lose the fractional inputs without compensation.
				switch (seriesIdx + step) % 4 {
				case 0:
					units = 1 << 57
				case 2:
					units = -(1 << 57)
				}
			default:
				panic("unknown benchmark values")
			}
			points = append(points, promql.FPoint{T: timeRange.IndexTime(int64(step)), F: float64(units) / 8})
			sumUnits[step] += units
			present[step] = true
		}
		fixture.series[seriesIdx].Floats = points
		fixture.samples += len(points)
		fixture.inputBytes += uint64(cap(points)) * types.FPointSize
	}

	for step, p := range present {
		if p {
			fixture.expected = append(fixture.expected, promql.FPoint{T: timeRange.IndexTime(int64(step)), F: float64(sumUnits[step]) / 8})
		}
	}
	return fixture
}

func validateSumBenchmarkState(t testing.TB, g *SumAggregationGroup, fixture sumBenchmarkFixture) {
	t.Helper()
	expectedIdx := 0
	for step, present := range g.floatPresent {
		wantPresent := expectedIdx < len(fixture.expected) && fixture.expected[expectedIdx].T == fixture.timeRange.IndexTime(int64(step))
		require.Equal(t, wantPresent, present, "step %d", step)
		if present {
			require.Equal(t, fixture.expected[expectedIdx].F, g.floatSums[step]+g.floatCompensatingValues[step], "step %d", step)
			expectedIdx++
		} else {
			require.Zero(t, g.floatSums[step])
			require.Zero(t, g.floatCompensatingValues[step])
		}
	}
	require.Equal(t, len(fixture.expected), expectedIdx)
}

func validateSumBenchmarkFixture(t testing.TB, fixture sumBenchmarkFixture) {
	t.Helper()
	tracker := limiter.NewUnlimitedMemoryConsumptionTracker(t.Context())
	g := &SumAggregationGroup{}
	for i, data := range fixture.series {
		require.NoError(t, g.AccumulateSeries(data, fixture.timeRange, tracker, nil, uint(len(fixture.series)-i), false))
	}
	validateSumBenchmarkState(t, g, fixture)
	output, mixed, err := g.ComputeOutputSeries(types.ScalarData{}, fixture.timeRange, tracker)
	require.NoError(t, err)
	require.False(t, mixed)
	require.Equal(t, fixture.expected, output.Floats)
	require.Empty(t, output.Histograms)
	g.Close(tracker)
	types.PutInstantVectorSeriesData(output, tracker)
	require.Zero(t, tracker.CurrentEstimatedMemoryConsumptionBytes())
}

func TestSumAggregationBenchmarkFixtures(t *testing.T) {
	for _, c := range sumBenchmarkCases() {
		t.Run(c.name(), func(t *testing.T) {
			fixture := newSumBenchmarkFixture(c)
			require.Equal(t, c.steps, fixture.timeRange.StepCount)
			require.Len(t, fixture.series, c.series)
			require.Positive(t, fixture.samples)
			validateSumBenchmarkFixture(t, fixture)
		})
	}
}
