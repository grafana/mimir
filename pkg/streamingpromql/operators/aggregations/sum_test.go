// SPDX-License-Identifier: AGPL-3.0-only

package aggregations

import (
	"fmt"
	"math"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/floats"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

func testAccumulateFloat64sWithT(t *testing.T, accumulate func(*testing.T, []float64, []float64, []float64), offset int, lengths ...int) {
	if len(lengths) == 0 {
		lengths = []int{0, 1, 2, 3, 7, 8, 9, 31, 32, 33}
	}
	inf := math.Inf(1)
	negativeZero := math.Copysign(0, -1)
	testCases := []struct {
		name         string
		sum          float64
		compensation float64
		terms        []float64
	}{
		{name: "ordinary", terms: []float64{3.14159, 2.71828, 1, -3}},
		{name: "cancellation", terms: []float64{1e16, 1, 1, -1e16}},
		{name: "larger_input", terms: []float64{1, 1e100, 1, -1e100}},
		{name: "zeros", terms: []float64{0, negativeZero, negativeZero, 0}},
		{name: "subnormals", terms: []float64{math.SmallestNonzeroFloat64, -math.SmallestNonzeroFloat64, 2 * math.SmallestNonzeroFloat64}},
		{name: "positive_infinity", terms: []float64{1e16, 1, inf, 2, inf}},
		{name: "negative_infinity", terms: []float64{-1e16, 1, -inf, 2, -inf}},
		{name: "opposite_infinities", terms: []float64{inf, -inf, 1}},
		{name: "positive_overflow", terms: []float64{math.MaxFloat64, math.MaxFloat64, -math.MaxFloat64}},
		{name: "negative_overflow", terms: []float64{-math.MaxFloat64, -math.MaxFloat64, math.MaxFloat64}},
		{name: "nan", terms: []float64{math.NaN(), 1, inf}},
		{name: "signaling_nan", terms: []float64{math.Float64frombits(0x7ff0000000000001), 1}},
		{name: "seeded_negative_zero", sum: negativeZero, compensation: negativeZero, terms: []float64{negativeZero, 0}},
		{name: "seeded_compensation", sum: 1e16, compensation: 1, terms: []float64{1, -1e16}},
		{name: "seeded_infinite_sum", sum: inf, compensation: math.NaN(), terms: []float64{1, inf}},
		{name: "seeded_infinite_compensation", sum: 1, compensation: inf, terms: []float64{-1, inf}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			for _, steps := range lengths {
				t.Run(fmt.Sprintf("steps=%d", steps), func(t *testing.T) {
					const guard = -123.5
					sumStorage := slices.Repeat([]float64{guard}, steps+offset+2)
					compensationStorage := slices.Clone(sumStorage)
					sums := sumStorage[offset : offset+steps]
					compensations := compensationStorage[offset : offset+steps]
					for i := range steps {
						sums[i], compensations[i] = tc.sum, tc.compensation
					}
					wantSums, wantCompensations := slices.Clone(sums), slices.Clone(compensations)

					accumulate(t, nil, sums, compensations)
					assertFloat64State(t, wantSums, sums)
					assertFloat64State(t, wantCompensations, compensations)

					valueStorage := slices.Repeat([]float64{guard}, steps+offset+2)
					values := valueStorage[offset : offset+steps]
					for series := range tc.terms {
						for step := range steps {
							// Adjacent steps exercise different magnitude and non-finite branches.
							values[step] = tc.terms[(series+step)%len(tc.terms)]
							wantSums[step], wantCompensations[step] = floats.KahanSumInc(values[step], wantSums[step], wantCompensations[step])
						}
						before := slices.Clone(values)
						accumulate(t, values, sums, compensations)
						assertFloat64State(t, wantSums, sums)
						assertFloat64State(t, wantCompensations, compensations)
						for i := range values {
							require.Equal(t, math.Float64bits(before[i]), math.Float64bits(values[i]), "input at step %d", i)
						}
					}

					for _, storage := range [][]float64{valueStorage, sumStorage, compensationStorage} {
						require.Equal(t, slices.Repeat([]float64{guard}, offset), storage[:offset])
						require.Equal(t, []float64{guard, guard}, storage[offset+steps:])
					}
				})
			}
		})
	}
}

func assertFloat64State(t testing.TB, want, got []float64) {
	t.Helper()
	require.Len(t, got, len(want))
	for i, expected := range want {
		if math.IsNaN(expected) {
			require.True(t, math.IsNaN(got[i]), "step %d: expected NaN, got %g", i, got[i])
		} else {
			require.Equal(t, math.Float64bits(expected), math.Float64bits(got[i]), "step %d: expected %g, got %g", i, expected, got[i])
		}
	}
}

func TestSumAggregationGroupContiguousArithmetic(t *testing.T) {
	tracker := limiter.NewUnlimitedMemoryConsumptionTracker(t.Context())
	testAccumulateFloat64sWithT(t, func(t *testing.T, values, sums, compensations []float64) {
		tr := sumIntegrationTimeRange(len(sums))
		points := make([]promql.FPoint, len(values))
		for i, value := range values {
			points[i] = promql.FPoint{T: tr.IndexTime(int64(i)), F: value}
		}
		before := slices.Clone(points)
		g := &SumAggregationGroup{floatSums: sums, floatCompensatingValues: compensations, floatPresent: make([]bool, len(sums))}
		require.NoError(t, g.accumulateFloats(types.InstantVectorSeriesData{Floats: points}, tr, tracker))
		assertSumInputUnchanged(t, before, points)
		require.Zero(t, tracker.CurrentEstimatedMemoryConsumptionBytes())
	}, 1, 0, 1, 7, 8, 9, 31, 32, 33, 63, 64, 65, 127, 128, 129, 255, 256, 257)
}

func TestSumAggregationGroupStateAfterEachSeries(t *testing.T) {
	for _, c := range sumBenchmarkCases() {
		t.Run(c.name(), func(t *testing.T) {
			fixture := newSumBenchmarkFixture(c)
			tracker := limiter.NewUnlimitedMemoryConsumptionTracker(t.Context())
			g := &SumAggregationGroup{}
			t.Cleanup(func() {
				g.Close(tracker)
				require.Zero(t, tracker.CurrentEstimatedMemoryConsumptionBytes())
			})
			sums, compensations, present := make([]float64, c.steps), make([]float64, c.steps), make([]bool, c.steps)
			for i, data := range fixture.series {
				before := slices.Clone(data.Floats)
				for _, point := range data.Floats {
					step := fixture.timeRange.PointIndex(point.T)
					sums[step], compensations[step] = floats.KahanSumInc(point.F, sums[step], compensations[step])
					present[step] = true
				}
				require.NoError(t, g.AccumulateSeries(data, fixture.timeRange, tracker, nil, uint(len(fixture.series)-i), false))
				assertFloat64State(t, sums, g.floatSums)
				assertFloat64State(t, compensations, g.floatCompensatingValues)
				require.Equal(t, present, g.floatPresent)
				assertSumInputUnchanged(t, before, data.Floats)
				stateBytes := uint64(cap(g.floatSums)+cap(g.floatCompensatingValues))*types.Float64Size + uint64(cap(g.floatPresent))*types.BoolSize
				require.Equal(t, stateBytes, tracker.CurrentEstimatedMemoryConsumptionBytes(), "scratch must not survive accumulation")
			}
			validateSumBenchmarkState(t, g, fixture)
		})
	}
}

func TestSumAggregationGroupEmptyAndMixedHistograms(t *testing.T) {
	tr := sumIntegrationTimeRange(257)
	tracker := limiter.NewUnlimitedMemoryConsumptionTracker(t.Context())
	g := &SumAggregationGroup{}
	t.Cleanup(func() {
		g.Close(tracker)
		require.Zero(t, tracker.CurrentEstimatedMemoryConsumptionBytes())
	})
	require.NoError(t, g.AccumulateSeries(types.InstantVectorSeriesData{}, tr, tracker, nil, 3, false))
	require.Zero(t, tracker.CurrentEstimatedMemoryConsumptionBytes())

	h := &histogram.FloatHistogram{Count: 1, Sum: 2}
	histograms := types.InstantVectorSeriesData{Histograms: []promql.HPoint{{T: tr.IndexTime(0), H: h}, {T: tr.IndexTime(256), H: h.Copy()}}}
	require.NoError(t, g.AccumulateSeries(histograms, tr, tracker, nil, 2, false))
	require.Nil(t, g.floatSums)
	require.NotSame(t, h, g.histogramSums[0])

	points := make([]promql.FPoint, 129)
	for i := range points {
		points[i] = promql.FPoint{T: tr.IndexTime(int64(i)), F: 0}
	}
	points[1].F = math.NaN()
	require.NoError(t, g.AccumulateSeries(types.InstantVectorSeriesData{Floats: points}, tr, tracker, nil, 1, false))
	output, mixed, err := g.ComputeOutputSeries(types.ScalarData{}, tr, tracker)
	require.NoError(t, err)
	require.True(t, mixed)
	require.Len(t, output.Floats, 128)
	require.Equal(t, tr.IndexTime(1), output.Floats[0].T)
	require.True(t, math.IsNaN(output.Floats[0].F))
	require.Zero(t, output.Floats[1].F)
	require.Equal(t, []promql.HPoint{{T: tr.IndexTime(256), H: h}}, output.Histograms)
	require.Equal(t, &histogram.FloatHistogram{Count: 1, Sum: 2}, h)
	types.PutInstantVectorSeriesData(output, tracker)

	a, err := NewAggregator(parser.SUM, nil, false, tracker, tr, posrange.PositionRange{})
	require.NoError(t, err)
	metadata, err := a.ComputeGroups([]types.SeriesMetadata{{Labels: labels.EmptyLabels()}, {Labels: labels.EmptyLabels()}})
	require.NoError(t, err)
	types.SeriesMetadataSlicePool.Put(&metadata, tracker)
	require.NoError(t, a.AccumulateNextInnerSeries(types.InstantVectorSeriesData{Floats: points}, false))
	require.NoError(t, a.AccumulateNextInnerSeries(histograms, false))
	output, err = a.ComputeNextOutputSeries()
	require.NoError(t, err)
	require.Len(t, a.Annotations, 1)
	for _, annotation := range a.Annotations {
		require.EqualError(t, annotation, annotations.NewMixedFloatsHistogramsAggWarning(posrange.PositionRange{}).Error())
	}
	types.PutInstantVectorSeriesData(output, tracker)
	a.FinishedReading()
}

func TestSumAggregationGroupMemoryFailures(t *testing.T) {
	tr := sumIntegrationTimeRange(129)
	data := newSumBenchmarkFixture(sumBenchmarkCase{129, 1, "full_range", "positive"}).series[0]
	// Pools round 129 steps to 256; limits must use bucket capacity.
	stateBytes := uint64(256) * (2*types.Float64Size + types.BoolSize)
	for _, limit := range []uint64{1, 256 * 8, 256 * 16, stateBytes - 1} {
		t.Run(strconv.FormatUint(limit, 10), func(t *testing.T) {
			tracker := limiter.NewMemoryConsumptionTracker(t.Context(), limit, prometheus.NewCounter(prometheus.CounterOpts{Name: "sum_memory_rejections"}), "")
			g := &SumAggregationGroup{}
			require.ErrorIs(t, g.AccumulateSeries(data, tr, tracker, nil, 1, false), limiter.NewMaxEstimatedMemoryConsumptionPerQueryLimitError(limit))
			g.Close(tracker)
			require.Zero(t, tracker.CurrentEstimatedMemoryConsumptionBytes())
		})
	}

	for _, pooledInput := range []bool{false, true} {
		t.Run("exact budget/pooled input="+strconv.FormatBool(pooledInput), func(t *testing.T) {
			limit := stateBytes
			if pooledInput {
				limit += 256 * types.FPointSize
			}
			tracker := limiter.NewMemoryConsumptionTracker(t.Context(), limit, prometheus.NewCounter(prometheus.CounterOpts{Name: "sum_memory_rejections"}), "")
			input := data
			if pooledInput {
				var err error
				input, err = data.Clone(tracker)
				require.NoError(t, err)
			}
			g := &SumAggregationGroup{}
			require.NoError(t, g.AccumulateSeries(input, tr, tracker, nil, 1, false))
			assertSumInputUnchanged(t, data.Floats, input.Floats)
			require.Equal(t, limit, tracker.CurrentEstimatedMemoryConsumptionBytes())
			require.Equal(t, limit, tracker.PeakEstimatedMemoryConsumptionBytes())
			g.Close(tracker)
			if pooledInput {
				types.PutInstantVectorSeriesData(input, tracker)
			}
			require.Zero(t, tracker.CurrentEstimatedMemoryConsumptionBytes())
		})
	}
}

func TestSumAggregationGroupContiguousInputReusesStateBudget(t *testing.T) {
	tr := sumIntegrationTimeRange(257)
	stateBytes := uint64(512) * (2*types.Float64Size + types.BoolSize)
	tracker := limiter.NewMemoryConsumptionTracker(t.Context(), stateBytes, prometheus.NewCounter(prometheus.CounterOpts{Name: "sum_memory_rejections"}), "")
	g := &SumAggregationGroup{}
	t.Cleanup(func() {
		g.Close(tracker)
		require.Zero(t, tracker.CurrentEstimatedMemoryConsumptionBytes())
	})
	points := make([]promql.FPoint, 128)
	for i := range points {
		points[i] = promql.FPoint{T: tr.IndexTime(int64(i)), F: float64(i)}
	}
	require.NoError(t, g.AccumulateSeries(types.InstantVectorSeriesData{Floats: points}, tr, tracker, nil, 2, false))
	sums, compensations, present := slices.Clone(g.floatSums), slices.Clone(g.floatCompensatingValues), slices.Clone(g.floatPresent)
	data := newSumBenchmarkFixture(sumBenchmarkCase{257, 1, "full_range", "positive"}).series[0]
	for _, point := range data.Floats {
		step := tr.PointIndex(point.T)
		sums[step], compensations[step] = floats.KahanSumInc(point.F, sums[step], compensations[step])
		present[step] = true
	}
	require.NoError(t, g.AccumulateSeries(data, tr, tracker, nil, 1, false))
	assertFloat64State(t, sums, g.floatSums)
	assertFloat64State(t, compensations, g.floatCompensatingValues)
	require.Equal(t, present, g.floatPresent)
	require.Equal(t, stateBytes, tracker.CurrentEstimatedMemoryConsumptionBytes())
}

func TestSumAggregatorGroups(t *testing.T) {
	for _, grouped := range []bool{false, true} {
		t.Run(strconv.FormatBool(grouped), func(t *testing.T) {
			tr := sumIntegrationTimeRange(129)
			tracker := limiter.NewUnlimitedMemoryConsumptionTracker(t.Context())
			var grouping []string
			if grouped {
				grouping = []string{"group"}
			}
			a, err := NewAggregator(parser.SUM, grouping, false, tracker, tr, posrange.PositionRange{})
			require.NoError(t, err)
			metadata := make([]types.SeriesMetadata, 6)
			for i := range metadata {
				group := "A"
				if i%2 == 1 {
					group = "B"
				}
				metadata[i].Labels = labels.FromStrings("group", group, "idx", strconv.Itoa(i))
			}
			outputMetadata, err := a.ComputeGroups(metadata)
			require.NoError(t, err)
			wantValues := []float64{7}
			if grouped {
				wantValues = []float64{1, 6}
				require.Equal(t, []types.SeriesMetadata{{Labels: labels.FromStrings("group", "A")}, {Labels: labels.FromStrings("group", "B")}}, outputMetadata)
			} else {
				require.Equal(t, []types.SeriesMetadata{{Labels: labels.EmptyLabels()}}, outputMetadata)
			}
			types.SeriesMetadataSlicePool.Put(&outputMetadata, tracker)
			for _, value := range []float64{1e16, 3, 1, 2, -1e16, 1} {
				points, err := types.FPointSlicePool.Get(tr.StepCount, tracker)
				require.NoError(t, err)
				for step := range tr.StepCount {
					points = append(points, promql.FPoint{T: tr.IndexTime(int64(step)), F: value})
				}
				require.NoError(t, a.AccumulateNextInnerSeries(types.InstantVectorSeriesData{Floats: points}, true))
			}
			for _, want := range wantValues {
				require.True(t, a.IsNextOutputSeriesComplete())
				output, err := a.ComputeNextOutputSeries()
				require.NoError(t, err)
				require.Len(t, output.Floats, tr.StepCount)
				for step, point := range output.Floats {
					require.Equal(t, tr.IndexTime(int64(step)), point.T)
					require.Equal(t, want, point.F)
				}
				types.PutInstantVectorSeriesData(output, tracker)
			}
			require.False(t, a.HasMoreOutputSeries())
			a.FinishedReading()
			require.Zero(t, tracker.CurrentEstimatedMemoryConsumptionBytes())
		})
	}
}

func sumIntegrationTimeRange(steps int) types.QueryTimeRange {
	start := time.Unix(0, 0)
	return types.NewRangeQueryTimeRange(start, start.Add(time.Duration(steps-1)*15*time.Second), 15*time.Second)
}

func assertSumInputUnchanged(t *testing.T, before, after []promql.FPoint) {
	t.Helper()
	require.Len(t, after, len(before))
	for i := range before {
		require.Equal(t, before[i].T, after[i].T)
		require.Equal(t, math.Float64bits(before[i].F), math.Float64bits(after[i].F))
	}
}

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
	steps    int
	series   int
	coverage string
	values   string
}

func (c sumBenchmarkCase) name() string {
	return fmt.Sprintf("steps=%d/series=%d/coverage=%s/values=%s", c.steps, c.series, c.coverage, c.values)
}

func sumBenchmarkCases() []sumBenchmarkCase {
	var cases []sumBenchmarkCase
	for _, steps := range []int{1, 8, 128, 1000} {
		for _, series := range []int{1, 16, 256} {
			cases = append(cases, sumBenchmarkCase{steps, series, "full_range", "positive"})
		}
	}
	for _, series := range []int{16, 256} {
		cases = append(cases, sumBenchmarkCase{10000, series, "full_range", "positive"})
	}
	for _, steps := range []int{128, 1000} {
		for _, series := range []int{16, 256} {
			for _, coverage := range []string{"partial_range", "gaps"} {
				cases = append(cases, sumBenchmarkCase{steps, series, coverage, "positive"})
			}
		}
	}
	for _, steps := range []int{8, 128, 1000} {
		for _, series := range []int{16, 256} {
			cases = append(cases, sumBenchmarkCase{steps, series, "full_range", "mixed_magnitude"})
		}
	}
	for _, steps := range []int{2, 4, 16, 32, 64, 127, 129, 1001} {
		for _, series := range []int{1, 16, 256} {
			cases = append(cases, sumBenchmarkCase{steps, series, "full_range", "positive"})
		}
	}
	for _, steps := range []int{128, 1000} {
		for _, series := range []int{16, 256} {
			for _, values := range []string{"mixed_sign", "metric_like"} {
				cases = append(cases, sumBenchmarkCase{steps, series, "full_range", values})
			}
		}
	}
	for _, steps := range []int{8, 32, 64} {
		for _, series := range []int{1, 16, 256} {
			for _, values := range []string{"mixed_sign", "metric_like"} {
				cases = append(cases, sumBenchmarkCase{steps, series, "full_range", values})
			}
		}
	}
	for _, steps := range []int{32, 64} {
		for _, series := range []int{16, 256} {
			cases = append(cases, sumBenchmarkCase{steps, series, "full_range", "mixed_magnitude"})
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
		if c.coverage == "partial_range" {
			offset := seriesIdx % max(1, c.steps/16)
			first = c.steps/4 + offset
			end = c.steps - c.steps/4 - offset
		}
		points := make([]promql.FPoint, 0, end-first)
		for step := first; step < end; step++ {
			switch c.coverage {
			case "full_range", "partial_range":
			case "gaps":
				if step%8 == 7 || step%4 == seriesIdx%3 {
					continue
				}
			default:
				panic("unknown benchmark coverage")
			}

			units := int64(1 + (step*17+seriesIdx*13)%63)
			switch c.values {
			case "positive":
			case "mixed_sign":
				if (seriesIdx+step)%2 == 0 {
					units = -units
				}
			case "metric_like":
				units = int64(10000 + 13*seriesIdx + 2*step)
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
