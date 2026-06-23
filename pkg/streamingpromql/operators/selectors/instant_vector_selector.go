// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package selectors

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/streamingpromql/operators"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

type InstantVectorSelector struct {
	Selector                                 *Selector
	MemoryConsumptionTracker                 *limiter.MemoryConsumptionTracker
	ReturnSampleTimestamps                   bool // true if this operator is wrapped directly in the timestamp() function and so should return the underlying sample timestamps.
	ReturnSampleTimestampsPreserveHistograms bool // Used for info() function to preserve histograms in info metrics while making the floats reflect timestamps.

	chunkIterator    chunkenc.Iterator
	memoizedIterator *storage.MemoizedSeriesIterator
	evaluationStats  *types.OperatorEvaluationStats

	// metricNames captures the metric name of each series in SeriesMetadata, so that the smoothed
	// modifier can emit per-series annotations once the series data has been examined in NextSeries
	// (the series labels are no longer available by then). Only populated when smoothed is used.
	metricNames        *operators.MetricNames
	currentSeriesIndex int
	annos              annotations.Annotations
}

var _ types.InstantVectorOperator = &InstantVectorSelector{}

func NewInstantVectorSelector(selector *Selector, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, returnSampleTimestamps, returnSampleTimestampsPreserveHistograms bool) *InstantVectorSelector {
	v := &InstantVectorSelector{
		Selector:                                 selector,
		MemoryConsumptionTracker:                 memoryConsumptionTracker,
		ReturnSampleTimestamps:                   returnSampleTimestamps,
		ReturnSampleTimestampsPreserveHistograms: returnSampleTimestampsPreserveHistograms,
	}

	if selector.Smoothed {
		// The smoothed modifier can emit per-series annotations, which require the metric name.
		v.metricNames = &operators.MetricNames{}
	}

	return v
}

func (v *InstantVectorSelector) ExpressionPosition() posrange.PositionRange {
	return v.Selector.ExpressionPosition
}

func (v *InstantVectorSelector) SeriesMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, error) {
	metadata, err := v.Selector.SeriesMetadata(ctx, matchers)
	if err != nil {
		return nil, err
	}

	// The smoothed modifier may need the metric name to emit annotations for a series once its
	// samples have been examined in NextSeries.
	if v.metricNames != nil {
		v.metricNames.CaptureMetricNames(metadata)
	}

	return metadata, nil
}

func (v *InstantVectorSelector) NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	if v.memoizedIterator == nil {
		v.memoizedIterator = storage.NewMemoizedEmptyIterator(v.Selector.LookbackDelta.Milliseconds() - 1) // -1 to exclude samples on the lower boundary of the range.
	}

	seriesIndex := v.currentSeriesIndex
	v.currentSeriesIndex++

	var matchesSubsets []bool
	var err error
	v.chunkIterator, matchesSubsets, err = v.Selector.Next(ctx, v.chunkIterator)
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	v.memoizedIterator.Reset(v.chunkIterator)

	data := types.InstantVectorSeriesData{}

	// Keep track of the last histogram we saw.
	// This is important for a few reasons:
	// - it allows us to avoid unnecessarily creating FloatHistograms when the same histogram is used at multiple points
	//   due to lookback
	// - it allows consuming operators that mutate histograms to avoid making copies of FloatHistograms where possible,
	//   as they can check if the same FloatHistogram instance is used for multiple points, and then only make a copy
	//   if the histogram is used for multiple points
	lastHistogramT := int64(math.MinInt64)
	var lastHistogram *histogram.FloatHistogram

	stepIndex := -1
	for stepT := v.Selector.TimeRange.StartT; stepT <= v.Selector.TimeRange.EndT; stepT += v.Selector.TimeRange.IntervalMilliseconds {
		stepIndex++
		var t int64
		var f float64
		var h *histogram.FloatHistogram
		// hInterpolated is true when h is a freshly-interpolated histogram (not a source sample),
		// so we know not to feed it into the lastHistogram reuse path on the next iteration.
		hInterpolated := false

		ts := stepT

		if v.Selector.Timestamp != nil {
			// Timestamp from @ modifier takes precedence over query evaluation timestamp.
			ts = *v.Selector.Timestamp
		}

		// Apply offset after adjusting for timestamp from @ modifier.
		ts = ts - v.Selector.Offset
		valueType := v.memoizedIterator.Seek(ts)

		switch valueType {
		case chunkenc.ValNone:
			if v.memoizedIterator.Err() != nil {
				return types.InstantVectorSeriesData{}, v.memoizedIterator.Err()
			}
		case chunkenc.ValFloat:
			t, f = v.memoizedIterator.At()
		case chunkenc.ValHistogram, chunkenc.ValFloatHistogram:
			if atT := v.memoizedIterator.AtT(); atT == lastHistogramT && lastHistogram != nil {
				// We're still looking at the last histogram we used, don't bother creating another FloatHistogram yet as we might not need it.
				// If we're going to return this histogram, we'll make a copy below.
				t, h = atT, lastHistogram
			} else {
				t, h = v.memoizedIterator.AtFloatHistogram()
			}

		default:
			return types.InstantVectorSeriesData{}, fmt.Errorf("streaming PromQL engine: unknown value type %s", valueType.String())
		}

		if valueType == chunkenc.ValNone || t > ts {
			var ok bool

			// Keep this a copy of this point for use with smoothed case below.
			right := promql.FPoint{T: t, F: f}
			// Capture the right-side histogram before PeekPrev overwrites h, so that smoothed
			// queries can interpolate between two histograms or detect a mixed prev/right pair.
			var rightH *histogram.FloatHistogram
			if valueType == chunkenc.ValHistogram || valueType == chunkenc.ValFloatHistogram {
				rightH = h
			}

			t, f, h, ok = v.memoizedIterator.PeekPrev()
			if !ok || t <= ts-v.Selector.LookbackDelta.Milliseconds() {
				continue
			}

			// Under the smoothed modifier, if the look-back/look-ahead window straddles a float and
			// a histogram we cannot interpolate. Mirror Prometheus's smoothSeries, which emits
			// MixedFloatsHistogramsWarning and produces no point for the step.
			//
			// Known limitation vs Prometheus: this compares only the two immediate neighbours of ts
			// (the nearest look-back sample and the nearest look-ahead sample). Prometheus's
			// smoothSeries scans the entire [ts-lookback, ts+lookback] window, so it also detects a
			// non-adjacent other-typed sample (e.g. a histogram at ts-2*step when both immediate
			// neighbours are floats). In that case MQE interpolates and returns a value where
			// Prometheus would warn and drop the point.
			if v.Selector.Smoothed && valueType != chunkenc.ValNone && right.T <= ts+v.Selector.LookbackDelta.Milliseconds() && ((h != nil) != (rightH != nil)) {
				v.annos.Add(annotations.NewMixedFloatsHistogramsWarning(v.metricNames.GetMetricNameForSeries(seriesIndex), v.ExpressionPosition()))
				continue
			}

			if h != nil {
				if t == lastHistogramT && lastHistogram != nil {
					// Reuse exactly the same FloatHistogram as last time, don't bother creating another FloatHistogram yet.
					// PeekPrev can return a new FloatHistogram instance with the same underlying bucket slices as a previous call
					// to AtFloatHistogram, so if we're going to return this histogram, we'll make a copy below.
					h = lastHistogram
				}
				// Under the smoothed modifier with two histograms surrounding ts within the
				// look-back/look-ahead window, interpolate the histogram at ts. If the
				// right-side histogram detected a counter reset against the left, model the
				// counter as restarting from zero (mirrors interpolateHistograms in
				// vendor/.../promql/functions.go).
				if v.Selector.Smoothed && rightH != nil && right.T <= ts+v.Selector.LookbackDelta.Milliseconds() {
					interpolated, err := interpolateHistogramAt(h, t, rightH, right.T, ts, func(op annotations.HistogramOperation) {
						v.annos.Add(annotations.NewMismatchedCustomBucketsHistogramsInfo(v.ExpressionPosition(), op))
					})
					if err != nil {
						// Exponential and custom-bucket histograms cannot be interpolated: emit
						// MixedExponentialCustomHistogramsWarning and drop the step, mirroring
						// Prometheus's smoothSeries / annosFromInterpolationError.
						if errors.Is(err, histogram.ErrHistogramsIncompatibleSchema) {
							v.annos.Add(annotations.NewMixedExponentialCustomHistogramsWarning(v.metricNames.GetMetricNameForSeries(seriesIndex), v.ExpressionPosition()))
							continue
						}
						return types.InstantVectorSeriesData{}, err
					}
					h = interpolated
					hInterpolated = true
				}
				// A mixed float/histogram window is handled above (skip + warn), so any remaining
				// right-side float here is outside the look-ahead window: fall back to the lookback
				// histogram, mirroring Prometheus's nearest-previous-sample behaviour.
			} else {
				// If this query uses the 'smoothed' modifier, we look back within the look-back delta
				// to find the most recent float value before the requested timestamp.
				// If both a previous and a next point are found, the value at the requested time
				// is computed as the linear interpolation between those two points.
				if v.Selector.Smoothed && valueType == chunkenc.ValFloat && right.T <= ts+v.Selector.LookbackDelta.Milliseconds() {
					f = f + (right.F-f)*float64(ts-t)/float64(right.T-t)
				}
			}
		}

		if value.IsStaleNaN(f) || (h != nil && value.IsStaleNaN(h.Sum)) {
			continue
		}

		if v.ReturnSampleTimestamps || (v.ReturnSampleTimestampsPreserveHistograms && h == nil) {
			f = float64(t) / 1000
			h = nil
		}

		// if (f, h) have been set by PeekPrev, we do not know if f is 0 because that's the actual value, or because
		// the previous value had a histogram.
		// PeekPrev will set the histogram to nil, or the value to 0 if the other type exists.
		// So check if histograms is nil first. If we don't have a histogram, then we should have a value and vice-versa.
		if h != nil {
			// Under the smoothed modifier, a histogram exact-match or lookback returns the
			// histogram as-is. Interpolation between two histograms within the look-back/look-
			// ahead window is handled at the range-vector / function level via extendedHistogramRate;
			// the instant-vector selector mirrors Prometheus's behaviour of picking the nearest
			// previous sample.

			// Only create the slice once we know the series is a histogram or not.
			// (It is possible to over-allocate in the case where we have both floats and histograms, but that won't be common).
			if len(data.Histograms) == 0 {
				remainingStepCount := v.Selector.TimeRange.StepCount - int(v.Selector.TimeRange.PointIndex(stepT)) // Only get a slice for the number of points remaining in the query range.

				var err error
				if data.Histograms, err = types.HPointSlicePool.Get(remainingStepCount, v.MemoryConsumptionTracker); err != nil {
					return types.InstantVectorSeriesData{}, err
				}
			}

			if t == lastHistogramT {
				// We're returning a histogram we've previously used, so make a copy of it now.
				h = h.Copy()
			}

			data.Histograms = append(data.Histograms, promql.HPoint{T: stepT, H: h})
			if hInterpolated {
				// Don't cache a smoothed-interpolated histogram under the source-sample timestamp
				// t; on the next step the PeekPrev reuse check would otherwise feed the previous
				// interpolation back in as the left endpoint and drift the result.
				lastHistogramT = math.MinInt64
				lastHistogram = nil
			} else {
				lastHistogramT = t
				lastHistogram = h
			}

			// For consistency with Prometheus' engine, we convert each histogram point to an equivalent number of float points.
			sampleCount := types.EquivalentFloatSampleCount(h)
			v.evaluationStats.TrackSampleForInstantVectorSelector(stepT, sampleCount, matchesSubsets)

		} else {
			// Only create the slice once we know the series is a histogram or not.
			if len(data.Floats) == 0 {
				remainingStepCount := v.Selector.TimeRange.StepCount - int(v.Selector.TimeRange.PointIndex(stepT)) // Only get a slice for the number of points remaining in the query range.

				var err error
				if data.Floats, err = types.FPointSlicePool.Get(remainingStepCount, v.MemoryConsumptionTracker); err != nil {
					return types.InstantVectorSeriesData{}, err
				}
			}
			v.evaluationStats.TrackSampleForInstantVectorSelector(stepT, 1, matchesSubsets)
			data.Floats = append(data.Floats, promql.FPoint{T: stepT, F: f})
		}
	}

	if v.memoizedIterator.Err() != nil {
		return types.InstantVectorSeriesData{}, v.memoizedIterator.Err()
	}

	return data, nil
}

func (v *InstantVectorSelector) Prepare(ctx context.Context, params *types.PrepareParams) error {
	var err error
	v.evaluationStats, err = types.NewOperatorEvaluationStats(ctx, v.Selector.TimeRange, v.MemoryConsumptionTracker, len(v.Selector.Subsets))
	if err != nil {
		return err
	}
	return v.Selector.Prepare(ctx, params)
}

func (v *InstantVectorSelector) AfterPrepare(ctx context.Context) error {
	return nil
}

func (v *InstantVectorSelector) FinishedReading(ctx context.Context) error {
	v.memoizedIterator = nil
	v.chunkIterator = nil

	v.Selector.Close()
	return nil
}

func (v *InstantVectorSelector) Finalize(ctx context.Context) (*types.OperatorEvaluationStats, annotations.Annotations, error) {
	stats := v.evaluationStats
	v.evaluationStats = nil
	return stats, v.annos, nil
}

func (v *InstantVectorSelector) Close() {
	// If the query fails, then FinishedReading above won't be called, so make sure to close the selector.
	v.Selector.Close()

	if v.evaluationStats != nil {
		v.evaluationStats.Close()
		v.evaluationStats = nil
	}
}

// interpolateHistogramAt linearly interpolates between two histograms (h1 at t1, h2 at t2) and
// returns the histogram value at time t for the smoothed instant-vector selector. It infers
// whether the pair is counter data from the samples' reset hints and delegates the interpolation
// to types.InterpolateHistograms.
func interpolateHistogramAt(h1 *histogram.FloatHistogram, t1 int64, h2 *histogram.FloatHistogram, t2, t int64, emitInfo func(annotations.HistogramOperation)) (*histogram.FloatHistogram, error) {
	// Treat the pair as counter data unless BOTH samples explicitly carry the gauge hint. If
	// either side could be a counter (UnknownCounterReset / CounterReset / NotCounterReset),
	// model a detected decrease as a reset from zero. Matches upstream Prometheus's
	// interpolateHistograms behaviour.
	isCounter := h1.CounterResetHint != histogram.GaugeType || h2.CounterResetHint != histogram.GaugeType
	return types.InterpolateHistograms(h1, t1, h2, t2, t, isCounter, emitInfo)
}
