// SPDX-License-Identifier: AGPL-3.0-only

package functions

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/streamingpromql/operators"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

const benchNumSeries = 100
const benchNumSteps = 300

// syntheticRangeVectorOperator is a minimal RangeVectorOperator that returns
// pre-baked step data in a tight loop without any I/O.
type syntheticRangeVectorOperator struct {
	timeRange types.QueryTimeRange
	stepIndex int
	stepData  *types.RangeVectorStepData
	floatBuf  *types.FPointRingBuffer
	histBuf   *types.HPointRingBuffer
}

func newSyntheticRangeVectorOperator(tr types.QueryTimeRange, memTracker *limiter.MemoryConsumptionTracker, samplesPerStep int) *syntheticRangeVectorOperator {
	floatBuf := types.NewFPointRingBuffer(memTracker)
	histBuf := types.NewHPointRingBuffer(memTracker)

	// Pre-populate with samplesPerStep float points.
	for i := 0; i < samplesPerStep; i++ {
		_ = floatBuf.Append(promql.FPoint{T: tr.StartT + int64(i)*5000, F: float64(i + 1)})
	}

	o := &syntheticRangeVectorOperator{
		timeRange: tr,
		floatBuf:  floatBuf,
		histBuf:   histBuf,
	}
	o.stepData = &types.RangeVectorStepData{}
	o.stepData.Floats = floatBuf.ViewUntilSearchingBackwards(tr.EndT+1_000_000_000, nil)
	o.stepData.Histograms = histBuf.ViewUntilSearchingBackwards(tr.EndT+1_000_000_000, nil)
	return o
}

func (s *syntheticRangeVectorOperator) ExpressionPosition() posrange.PositionRange {
	return posrange.PositionRange{}
}
func (s *syntheticRangeVectorOperator) Close() {}
func (s *syntheticRangeVectorOperator) Prepare(_ context.Context, _ *types.PrepareParams) error {
	return nil
}
func (s *syntheticRangeVectorOperator) AfterPrepare(_ context.Context) error { return nil }
func (s *syntheticRangeVectorOperator) Finalize(_ context.Context) error     { return nil }
func (s *syntheticRangeVectorOperator) SeriesMetadata(_ context.Context, _ types.Matchers) ([]types.SeriesMetadata, error) {
	meta := make([]types.SeriesMetadata, benchNumSeries)
	for i := range meta {
		meta[i] = types.SeriesMetadata{
			Labels: labels.FromStrings("__name__", "some_metric_total"),
		}
	}
	return meta, nil
}
func (s *syntheticRangeVectorOperator) NextSeries(_ context.Context) error {
	s.stepIndex = 0
	return nil
}
func (s *syntheticRangeVectorOperator) NextStepSamples(_ context.Context) (*types.RangeVectorStepData, error) {
	if s.stepIndex >= s.timeRange.StepCount {
		return nil, types.EOS
	}
	t := s.timeRange.StartT + int64(s.stepIndex)*s.timeRange.IntervalMilliseconds
	s.stepData.StepT = t
	s.stepData.RangeStart = t - 5*60*1000
	s.stepData.RangeEnd = t
	s.stepData.Floats = s.floatBuf.ViewUntilSearchingBackwards(t, s.stepData.Floats)
	s.stepData.Histograms = s.histBuf.ViewUntilSearchingBackwards(t, s.stepData.Histograms)
	s.stepIndex++
	return s.stepData, nil
}

// makeBenchOperator constructs a FunctionOverRangeVector ready for NextSeries calls.
// It directly populates metricNames when NeedsSeriesNamesForAnnotations is set so we
// can skip the real SeriesMetadata flow (which has memory-tracker bookkeeping
// that would panic with bare labels).
func makeBenchOperator(
	inner types.RangeVectorOperator,
	memTracker *limiter.MemoryConsumptionTracker,
	fn FunctionOverRangeVectorDefinition,
	tr types.QueryTimeRange,
) *FunctionOverRangeVector {
	op := NewFunctionOverRangeVector(
		inner,
		nil,
		memTracker,
		fn,
		annotations.New(),
		posrange.PositionRange{},
		tr,
		false,
	)
	if fn.NeedsSeriesNamesForAnnotations {
		meta := make([]types.SeriesMetadata, benchNumSeries)
		for i := range meta {
			meta[i] = types.SeriesMetadata{Labels: labels.FromStrings("__name__", "some_metric_total")}
		}
		mn := &operators.MetricNames{}
		mn.CaptureMetricNames(meta)
		op.metricNames = mn
	}
	return op
}

// BenchmarkFunctionOverRangeVector_NextSeries benchmarks the hot path for
// FunctionOverRangeVector.NextSeries.
//
// We run two sample densities:
//   - "sparse" (2 samples/step): amplifies per-step outer-loop overhead
//   - "dense" (60 samples/step): represents ~5min range at 5s scrape interval
//
// Functions exercised: rate (counter-reset + validation), countOverTime (minimal
// per-step work), deriv (linear regression).
func BenchmarkFunctionOverRangeVector_NextSeries(b *testing.B) {
	startTime := time.Unix(0, 0)
	endTime := startTime.Add(time.Duration(benchNumSteps-1) * 15 * time.Second)
	tr := types.NewRangeQueryTimeRange(startTime, endTime, 15*time.Second)

	funcs := []struct {
		name string
		def  FunctionOverRangeVectorDefinition
	}{
		{"rate", Rate},
		{"countOverTime", CountOverTime},
		{"deriv", Deriv},
	}

	for _, samples := range []int{2, 60} {
		samples := samples
		for _, fn := range funcs {
			fn := fn
			b.Run(fn.name+"/"+map[int]string{2: "sparse", 60: "dense"}[samples], func(b *testing.B) {
				ctx := context.Background()
				memTracker := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")
				inner := newSyntheticRangeVectorOperator(tr, memTracker, samples)
				op := makeBenchOperator(inner, memTracker, fn.def, tr)

				b.ResetTimer()
				b.ReportAllocs()

				for i := 0; i < b.N; i++ {
					for s := 0; s < benchNumSeries; s++ {
						inner.stepIndex = 0

						data, err := op.NextSeries(ctx)
						if err != nil {
							b.Fatal(err)
						}
						if data.Floats != nil {
							types.FPointSlicePool.Put(&data.Floats, memTracker)
						}
						if data.Histograms != nil {
							types.HPointSlicePool.Put(&data.Histograms, memTracker)
						}
						op.currentSeriesIndex = 0
					}
				}
			})
		}
	}
}

// BenchmarkFunctionOverRangeVector_NextSeriesHistogram exercises the histogram
// code path in the inner loop.
func BenchmarkFunctionOverRangeVector_NextSeriesHistogram(b *testing.B) {
	const (
		numSeries      = 50
		numSteps       = 300
		samplesPerStep = 20
	)

	startTime := time.Unix(0, 0)
	endTime := startTime.Add(time.Duration(numSteps-1) * 15 * time.Second)
	tr := types.NewRangeQueryTimeRange(startTime, endTime, 15*time.Second)

	ctx := context.Background()
	memTracker := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")
	inner := newSyntheticHistogramRangeVectorOperator(tr, memTracker, samplesPerStep)
	op := makeBenchOperator(inner, memTracker, CountOverTime, tr)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for s := 0; s < numSeries; s++ {
			inner.stepIndex = 0
			data, err := op.NextSeries(ctx)
			if err != nil {
				b.Fatal(err)
			}
			if data.Floats != nil {
				types.FPointSlicePool.Put(&data.Floats, memTracker)
			}
			if data.Histograms != nil {
				types.HPointSlicePool.Put(&data.Histograms, memTracker)
			}
			op.currentSeriesIndex = 0
		}
	}
}

// syntheticHistogramRangeVectorOperator produces only histogram points.
type syntheticHistogramRangeVectorOperator struct {
	timeRange types.QueryTimeRange
	stepIndex int
	stepData  *types.RangeVectorStepData
	floatBuf  *types.FPointRingBuffer
	histBuf   *types.HPointRingBuffer
}

func newSyntheticHistogramRangeVectorOperator(tr types.QueryTimeRange, memTracker *limiter.MemoryConsumptionTracker, samplesPerStep int) *syntheticHistogramRangeVectorOperator {
	floatBuf := types.NewFPointRingBuffer(memTracker)
	histBuf := types.NewHPointRingBuffer(memTracker)

	h := &histogram.FloatHistogram{Count: 100, Sum: 500}
	for i := 0; i < samplesPerStep; i++ {
		_ = histBuf.Append(promql.HPoint{T: tr.StartT + int64(i)*5000, H: h})
	}

	o := &syntheticHistogramRangeVectorOperator{
		timeRange: tr,
		floatBuf:  floatBuf,
		histBuf:   histBuf,
	}
	o.stepData = &types.RangeVectorStepData{}
	o.stepData.Floats = floatBuf.ViewUntilSearchingBackwards(tr.EndT+1_000_000_000, nil)
	o.stepData.Histograms = histBuf.ViewUntilSearchingBackwards(tr.EndT+1_000_000_000, nil)
	return o
}

func (s *syntheticHistogramRangeVectorOperator) ExpressionPosition() posrange.PositionRange {
	return posrange.PositionRange{}
}
func (s *syntheticHistogramRangeVectorOperator) Close() {}
func (s *syntheticHistogramRangeVectorOperator) Prepare(_ context.Context, _ *types.PrepareParams) error {
	return nil
}
func (s *syntheticHistogramRangeVectorOperator) AfterPrepare(_ context.Context) error { return nil }
func (s *syntheticHistogramRangeVectorOperator) Finalize(_ context.Context) error     { return nil }
func (s *syntheticHistogramRangeVectorOperator) SeriesMetadata(_ context.Context, _ types.Matchers) ([]types.SeriesMetadata, error) {
	meta := make([]types.SeriesMetadata, benchNumSeries)
	for i := range meta {
		meta[i] = types.SeriesMetadata{Labels: labels.FromStrings("__name__", "some_metric")}
	}
	return meta, nil
}
func (s *syntheticHistogramRangeVectorOperator) NextSeries(_ context.Context) error {
	s.stepIndex = 0
	return nil
}
func (s *syntheticHistogramRangeVectorOperator) NextStepSamples(_ context.Context) (*types.RangeVectorStepData, error) {
	if s.stepIndex >= s.timeRange.StepCount {
		return nil, types.EOS
	}
	t := s.timeRange.StartT + int64(s.stepIndex)*s.timeRange.IntervalMilliseconds
	s.stepData.StepT = t
	s.stepData.RangeStart = t - 5*60*1000
	s.stepData.RangeEnd = t
	s.stepData.Floats = s.floatBuf.ViewUntilSearchingBackwards(t, s.stepData.Floats)
	s.stepData.Histograms = s.histBuf.ViewUntilSearchingBackwards(t, s.stepData.Histograms)
	s.stepIndex++
	return s.stepData, nil
}
