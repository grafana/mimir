// SPDX-License-Identifier: AGPL-3.0-only

package continuoustest

import (
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"
)

const (
	maxComparisonDelta = 0.001
)

type generateHistogramFunc func(t time.Time) prompb.Histogram
type generateSeriesFunc func(name string, t time.Time, numSeries int) []prompb.TimeSeries
type generateValueFunc func(t time.Time) float64

var (
	generateHistogram = []generateHistogramFunc{
		func(t time.Time) prompb.Histogram {
			// int counter
			ts := t.UnixMilli()
			return remote.HistogramToHistogramProto(ts, generateIntHistogram(generateHistogramIntValue(t, false), false))
		},
		func(t time.Time) prompb.Histogram {
			// float counter
			ts := t.UnixMilli()
			return remote.FloatHistogramToHistogramProto(ts, generateFloatHistogram(generateHistogramFloatValue(t, false), false))
		},
		func(t time.Time) prompb.Histogram {
			// int gauge
			ts := t.UnixMilli()
			return remote.HistogramToHistogramProto(ts, generateIntHistogram(generateHistogramIntValue(t, true), true))
		},
		func(t time.Time) prompb.Histogram {
			// float gauge
			ts := t.UnixMilli()
			return remote.FloatHistogramToHistogramProto(ts, generateFloatHistogram(generateHistogramFloatValue(t, true), true))
		},
	}
	generateHistogramSeries = make([]generateSeriesFunc, 4)
	generateHistogramValue  = []generateValueFunc{
		func(t time.Time) float64 {
			return generateExpHistogramIntValue(t, false)
		},
		func(t time.Time) float64 {
			return generateExpHistogramFloatValue(t, false)
		},
		func(t time.Time) float64 {
			return generateExpHistogramIntValue(t, true)
		},
		func(t time.Time) float64 {
			return generateExpHistogramFloatValue(t, true)
		},
	}
)

func init() {
	for i := 0; i < 4; i++ {
		i := i
		generateHistogramSeries[i] = func(name string, t time.Time, numSeries int) []prompb.TimeSeries {
			return generateHistogramSeriesInner(name, t, numSeries, generateHistogram[i])
		}
	}
}

func alignTimestampToInterval(ts time.Time, interval time.Duration) time.Time {
	return ts.Truncate(interval)
}

// getQueryStep returns the query step to use to run a test query. The returned step
// is a guaranteed to be a multiple of alignInterval.
func getQueryStep(start, end time.Time, alignInterval time.Duration) time.Duration {
	const maxSamples = 1000

	// Compute the number of samples that we would have if we query every single sample.
	actualSamples := end.Sub(start) / alignInterval
	if actualSamples <= maxSamples {
		return alignInterval
	}

	// Adjust the query step based on the max steps spread over the query time range,
	// rounding it to alignInterval.
	step := end.Sub(start) / time.Duration(maxSamples)
	step = ((step / alignInterval) + 1) * alignInterval

	return step
}

func generatePosIntHistogram(value int64) *histogram.Histogram {
	return &histogram.Histogram{
		Sum:           float64(value * 10),
		Count:         uint64(value * 4),
		ZeroThreshold: 0.001,
		Schema:        2,
		PositiveSpans: []histogram.Span{
			{Offset: 0, Length: 1},
			{Offset: 3, Length: 1},
			{Offset: 2, Length: 2},
		},
		NegativeSpans: []histogram.Span{
			{Offset: 0, Length: 1},
			{Offset: 3, Length: 1},
			{Offset: 2, Length: 2},
		},
		PositiveBuckets: []int64{value, 0, 0, 0},
		NegativeBuckets: []int64{0, 0, 0, 0},
	}
}

func generateNegIntHistogram(value int64) *histogram.Histogram {
	return &histogram.Histogram{
		Sum:           float64(value * -10),
		Count:         uint64(value * 4),
		ZeroThreshold: 0.001,
		Schema:        2,
		PositiveSpans: []histogram.Span{
			{Offset: 0, Length: 1},
			{Offset: 3, Length: 1},
			{Offset: 2, Length: 2},
		},
		NegativeSpans: []histogram.Span{
			{Offset: 0, Length: 1},
			{Offset: 3, Length: 1},
			{Offset: 2, Length: 2},
		},
		PositiveBuckets: []int64{0, 0, 0, 0},
		NegativeBuckets: []int64{value, 0, 0, 0},
	}
}

func generatePosFloatHistogram(value float64) *histogram.FloatHistogram {
	return &histogram.FloatHistogram{
		Sum:           value * 10,
		Count:         value * 4,
		ZeroThreshold: 0.001,
		Schema:        2,
		PositiveSpans: []histogram.Span{
			{Offset: 0, Length: 1},
			{Offset: 3, Length: 1},
			{Offset: 2, Length: 2},
		},
		NegativeSpans: []histogram.Span{
			{Offset: 0, Length: 1},
			{Offset: 3, Length: 1},
			{Offset: 2, Length: 2},
		},
		PositiveBuckets: []float64{value, value, value, value},
		NegativeBuckets: []float64{0, 0, 0, 0},
	}
}

func generateNegFloatHistogram(value float64) *histogram.FloatHistogram {
	return &histogram.FloatHistogram{
		Sum:           value * -10,
		Count:         value * 4,
		ZeroThreshold: 0.001,
		Schema:        2,
		PositiveSpans: []histogram.Span{
			{Offset: 0, Length: 1},
			{Offset: 3, Length: 1},
			{Offset: 2, Length: 2},
		},
		NegativeSpans: []histogram.Span{
			{Offset: 0, Length: 1},
			{Offset: 3, Length: 1},
			{Offset: 2, Length: 2},
		},
		PositiveBuckets: []float64{0, 0, 0, 0},
		NegativeBuckets: []float64{value, value, value, value},
	}
}

func generateIntHistogram(value int64, gauge bool) *histogram.Histogram {
	var h *histogram.Histogram
	if value >= 0 {
		h = generatePosIntHistogram(value)
	} else {
		h = generateNegIntHistogram(-value)
	}
	if gauge {
		h.CounterResetHint = histogram.GaugeType
	}
	return h
}

func generateFloatHistogram(value float64, gauge bool) *histogram.FloatHistogram {
	var h *histogram.FloatHistogram
	if value >= 0 {
		h = generatePosFloatHistogram(value)
	} else {
		h = generateNegFloatHistogram(-value)
	}
	if gauge {
		h.CounterResetHint = histogram.GaugeType
	}
	return h
}

func generateSineWaveSeries(name string, t time.Time, numSeries int) []prompb.TimeSeries {
	out := make([]prompb.TimeSeries, 0, numSeries)
	value := generateSineWaveValue(t)
	ts := t.UnixMilli()

	for i := 0; i < numSeries; i++ {
		out = append(out, prompb.TimeSeries{
			Labels: []prompb.Label{{
				Name:  "__name__",
				Value: name,
			}, {
				Name:  "series_id",
				Value: strconv.Itoa(i),
			}},
			Samples: []prompb.Sample{{
				Value:     value,
				Timestamp: ts,
			}},
		})
	}

	return out
}

func generateHistogramSeriesInner(name string, t time.Time, numSeries int, histogramGenerator generateHistogramFunc) []prompb.TimeSeries {
	out := make([]prompb.TimeSeries, 0, numSeries)

	for i := 0; i < numSeries; i++ {
		out = append(out, prompb.TimeSeries{
			Labels: []prompb.Label{{
				Name:  "__name__",
				Value: name,
			}, {
				Name:  "series_id",
				Value: strconv.Itoa(i),
			}},
			Histograms: []prompb.Histogram{histogramGenerator(t)},
		})
	}

	return out
}

func generateSineWaveValue(t time.Time) float64 {
	period := 10 * time.Minute
	radians := 2 * math.Pi * float64(t.UnixNano()) / float64(period.Nanoseconds())
	return math.Sin(radians)
}

func generateHistogramIntValue(t time.Time, gauge bool) int64 {
	v := t.Unix()
	if gauge && t.Minute()%2 == 0 {
		return -v
	}
	return v
}

func generateHistogramFloatValue(t time.Time, gauge bool) float64 {
	v := float64(t.Unix()) / 500000
	if gauge && t.Minute()%2 == 0 {
		return -v
	}
	return v
}

func generateExpHistogramIntValue(t time.Time, gauge bool) float64 {
	return float64(generateHistogramIntValue(t, gauge)) * 10
}

func generateExpHistogramFloatValue(t time.Time, gauge bool) float64 {
	return generateHistogramFloatValue(t, gauge) * 10
}

// verifySamplesSum assumes the input matrix is the result of a range query summing the values
// of expectedSeries sine wave series and checks whether the actual values match the expected ones.
// Samples are checked in backward order, from newest to oldest. Returns error if values don't match,
// and the index of the last sample that matched the expectation or -1 if no sample matches.
func verifySamplesSum(matrix model.Matrix, expectedSeries int, expectedStep time.Duration, generateValue generateValueFunc) (lastMatchingIdx int, err error) {
	lastMatchingIdx = -1
	if len(matrix) != 1 {
		return lastMatchingIdx, fmt.Errorf("expected 1 series in the result but got %d", len(matrix))
	}

	samples := matrix[0].Values

	for idx := len(samples) - 1; idx >= 0; idx-- {
		sample := samples[idx]
		ts := time.UnixMilli(int64(sample.Timestamp)).UTC()

		// Assert on value.
		expectedValue := generateValue(ts) * float64(expectedSeries)
		if !compareSampleValues(float64(sample.Value), expectedValue) {
			return lastMatchingIdx, fmt.Errorf("sample at timestamp %d (%s) has value %f while was expecting %f", sample.Timestamp, ts.String(), sample.Value, expectedValue)
		}

		// Assert on sample timestamp. We expect no gaps.
		if idx < len(samples)-1 {
			nextTs := time.UnixMilli(int64(samples[idx+1].Timestamp)).UTC()
			expectedTs := nextTs.Add(-expectedStep)

			if ts.UnixMilli() != expectedTs.UnixMilli() {
				return lastMatchingIdx, fmt.Errorf("sample at timestamp %d (%s) was expected to have timestamp %d (%s) because next sample has timestamp %d (%s)",
					sample.Timestamp, ts.String(), expectedTs.UnixMilli(), expectedTs.String(), nextTs.UnixMilli(), nextTs.String())
			}
		}

		lastMatchingIdx = idx
	}

	return lastMatchingIdx, nil
}

func compareSampleValues(actual, expected float64) bool {
	delta := math.Abs((actual - expected) / maxComparisonDelta)
	return delta < maxComparisonDelta
}

func minTime(first, second time.Time) time.Time {
	if first.After(second) {
		return second
	}
	return first
}

func maxTime(first, second time.Time) time.Time {
	if first.After(second) {
		return first
	}
	return second
}

func randTime(min, max time.Time) time.Time {
	delta := max.Unix() - min.Unix()
	if delta <= 0 {
		return min
	}

	sec := rand.Int63n(delta) + min.Unix()
	return time.Unix(sec, 0)
}
