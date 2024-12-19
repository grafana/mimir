// SPDX-License-Identifier: AGPL-3.0-only

package continuoustest

import (
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/prompb"

	"github.com/grafana/mimir/pkg/mimirpb"
)

const (
	maxComparisonDeltaFloat     = 0.001
	maxComparisonDeltaHistogram = 0.01

	floatMetricName = "mimir_continuous_test_sine_wave_v2"
	floatTypeLabel  = "float"

	// A prime factor to generate extra values to make continuous-test series hash more uniformly.
	hashFactor = 2689
)

type generateHistogramFunc func(t time.Time) prompb.Histogram
type generateSeriesFunc func(name string, t time.Time, numSeries int) []prompb.TimeSeries
type generateValueFunc func(t time.Time) float64
type generateSampleHistogramFunc func(t time.Time, numSeries int) *model.SampleHistogram

type histogramProfile struct {
	metricName              string
	typeLabel               string
	generateHistogram       generateHistogramFunc
	generateSampleHistogram generateSampleHistogramFunc
	generateValue           generateValueFunc
	generateSeries          generateSeriesFunc
}

var (
	histogramProfiles = []histogramProfile{
		{
			metricName: "mimir_continuous_test_histogram_int_counter_v2",
			typeLabel:  "histogram_int_counter",
			generateHistogram: func(t time.Time) prompb.Histogram {
				ts := t.UnixMilli()
				return prompb.FromIntHistogram(ts, generateIntHistogram(generateHistogramIntValue(t, false), 1, false))
			},
			generateSampleHistogram: func(t time.Time, numSeries int) *model.SampleHistogram {
				return mimirpb.FromFloatHistogramToPromHistogram(generateIntHistogram(generateHistogramIntValue(t, false), numSeries, false).ToFloat(nil))
			},
		},
		{
			metricName: "mimir_continuous_test_histogram_float_counter_v2",
			typeLabel:  "histogram_float_counter",
			generateHistogram: func(t time.Time) prompb.Histogram {
				ts := t.UnixMilli()
				return prompb.FromFloatHistogram(ts, generateFloatHistogram(generateHistogramFloatValue(t, false), 1, false))
			},
			generateSampleHistogram: func(t time.Time, numSeries int) *model.SampleHistogram {
				return mimirpb.FromFloatHistogramToPromHistogram(generateFloatHistogram(generateHistogramFloatValue(t, false), numSeries, false))
			},
		},
		{
			metricName: "mimir_continuous_test_histogram_int_gauge_v2",
			typeLabel:  "histogram_int_gauge",
			generateHistogram: func(t time.Time) prompb.Histogram {
				ts := t.UnixMilli()
				return prompb.FromIntHistogram(ts, generateIntHistogram(generateHistogramIntValue(t, true), 1, true))
			},
			generateSampleHistogram: func(t time.Time, numSeries int) *model.SampleHistogram {
				return mimirpb.FromFloatHistogramToPromHistogram(generateIntHistogram(generateHistogramIntValue(t, true), numSeries, true).ToFloat(nil))
			},
		},
		{
			metricName: "mimir_continuous_test_histogram_float_gauge_v2",
			typeLabel:  "histogram_float_gauge",
			generateHistogram: func(t time.Time) prompb.Histogram {
				ts := t.UnixMilli()
				return prompb.FromFloatHistogram(ts, generateFloatHistogram(generateHistogramFloatValue(t, true), 1, true))
			},
			generateSampleHistogram: func(t time.Time, numSeries int) *model.SampleHistogram {
				return mimirpb.FromFloatHistogramToPromHistogram(generateFloatHistogram(generateHistogramFloatValue(t, true), numSeries, true))
			},
		},
	}
)

func init() {
	for i, histProfile := range histogramProfiles {
		histogramProfiles[i].generateValue = nil
		histogramProfiles[i].generateSeries = func(name string, t time.Time, numSeries int) []prompb.TimeSeries {
			return generateHistogramSeriesInner(name, t, numSeries, histProfile.generateHistogram)
		}
	}
}

type queryRecordingRuleFunc func(metricName, rangeVectorDuration string) string

func queryRecordingRule(metricName, rangeVectorDuration string) string {
	return fmt.Sprintf("%s[%s]", metricName, rangeVectorDuration)
}

type querySumFunc func(metricName string) string

func querySumFloat(metricName string) string {
	// We use max_over_time() with a 1s range selector in order to fetch only the samples we previously
	// wrote and ensure the PromQL lookback period doesn't influence query results. This help to avoid
	// false positives when finding the last written sample, or when restarting the testing tool with
	// a different number of configured series to write and read.
	return fmt.Sprintf("sum(max_over_time(%s[1s]))", metricName)
}

func querySumHist(metricName string) string {
	return fmt.Sprintf("sum(%s)", metricName)
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
		PositiveBuckets: []int64{value, 0, 0, 0},
	}
}

func generateNegIntHistogram(value int64) *histogram.Histogram {
	return &histogram.Histogram{
		Sum:           float64(value * -10),
		Count:         uint64(value * 4),
		ZeroThreshold: 0.001,
		Schema:        2,
		NegativeSpans: []histogram.Span{
			{Offset: 0, Length: 1},
			{Offset: 3, Length: 1},
			{Offset: 2, Length: 2},
		},
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
		PositiveBuckets: []float64{value, value, value, value},
	}
}

func generateNegFloatHistogram(value float64) *histogram.FloatHistogram {
	return &histogram.FloatHistogram{
		Sum:           value * -10,
		Count:         value * 4,
		ZeroThreshold: 0.001,
		Schema:        2,
		NegativeSpans: []histogram.Span{
			{Offset: 0, Length: 1},
			{Offset: 3, Length: 1},
			{Offset: 2, Length: 2},
		},
		NegativeBuckets: []float64{value, value, value, value},
	}
}

func generateIntHistogram(value int64, numSeries int, gauge bool) *histogram.Histogram {
	var h *histogram.Histogram
	value *= int64(numSeries)
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

func generateFloatHistogram(value float64, numSeries int, gauge bool) *histogram.FloatHistogram {
	var h *histogram.FloatHistogram
	value *= float64(numSeries)
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
			}, {
				Name:  "hash_extra",
				Value: strconv.Itoa(i * hashFactor),
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
			}, {
				Name:  "hash_extra",
				Value: strconv.Itoa(i * hashFactor),
			}},
			Histograms: []prompb.Histogram{histogramGenerator(t)},
		})
	}

	return out
}

func generateSineWaveValue(t time.Time) float64 {
	period := 10 * time.Minute
	radians := 2 * math.Pi * float64(t.UnixNano()) / float64(period.Nanoseconds())
	return math.Sin(radians) + 2
}

func generateHistogramIntValue(t time.Time, gauge bool) int64 {
	if gauge {
		return int64(generateSineWaveValue(t) * 100)
	}
	return t.Unix()
}

func generateHistogramFloatValue(t time.Time, gauge bool) float64 {
	if gauge {
		return generateSineWaveValue(t) / 10
	}
	return float64(t.Unix()) / 500000
}

// verifySamplesSum assumes the input matrix is the result of a range query summing the values
// of expectedSeries and checks whether the actual values match the expected ones.
// Samples are checked in backward order, from newest to oldest. Returns error if values don't match,
// and the index of the last sample that matched the expectation or -1 if no sample matches.
func verifySamplesSum(matrix model.Matrix, expectedSeries int, expectedStep time.Duration, generateValue generateValueFunc, generateSampleHistogram generateSampleHistogramFunc) (lastMatchingIdx int, err error) {
	lastMatchingIdx = -1
	if len(matrix) != 1 {
		return lastMatchingIdx, fmt.Errorf("expected 1 series in the result but got %d", len(matrix))
	}

	samples := matrix[0].Values
	histograms := matrix[0].Histograms
	haveSamples := len(samples) > 0
	haveHistograms := len(histograms) > 0
	if haveSamples && haveHistograms {
		return lastMatchingIdx, fmt.Errorf("expected only floats or histograms in the result but got both")
	}
	if !haveSamples && !haveHistograms {
		return lastMatchingIdx, fmt.Errorf("expected either floats or histograms in the result but got neither")
	}

	if haveHistograms {
		for idx := len(histograms) - 1; idx >= 0; idx-- {
			histogram := histograms[idx]
			if histogram.Histogram == nil {
				return lastMatchingIdx, fmt.Errorf("found null pointer in histogram")
			}
			ts := time.UnixMilli(int64(histogram.Timestamp)).UTC()

			// Assert on value.
			expectedHistogram := generateSampleHistogram(ts, expectedSeries)
			if !compareHistogramValues(histogram.Histogram, expectedHistogram, maxComparisonDeltaHistogram) {
				return lastMatchingIdx, fmt.Errorf("histogram at timestamp %d (%s) has sum %f while was expecting %f", histogram.Timestamp, ts.String(), histogram.Histogram.Sum, expectedHistogram.Sum)
			}

			// Assert on histogram timestamp. We expect no gaps.
			if idx < len(histograms)-1 {
				nextTs := time.UnixMilli(int64(histograms[idx+1].Timestamp)).UTC()
				expectedTs := nextTs.Add(-expectedStep)

				if ts.UnixMilli() != expectedTs.UnixMilli() {
					return lastMatchingIdx, fmt.Errorf("histogram at timestamp %d (%s) was expected to have timestamp %d (%s) because next histogram has timestamp %d (%s)",
						histogram.Timestamp, ts.String(), expectedTs.UnixMilli(), expectedTs.String(), nextTs.UnixMilli(), nextTs.String())
				}
			}

			lastMatchingIdx = idx
		}
		return lastMatchingIdx, nil
	}

	for idx := len(samples) - 1; idx >= 0; idx-- {
		sample := samples[idx]
		ts := time.UnixMilli(int64(sample.Timestamp)).UTC()

		// Assert on value.
		expectedValue := generateValue(ts) * float64(expectedSeries)
		if !compareFloatValues(float64(sample.Value), expectedValue, maxComparisonDeltaFloat) {
			comparison := formatExpectedAndActualValuesComparison(matrix, expectedSeries, generateValue)
			return lastMatchingIdx, fmt.Errorf("sample at timestamp %d (%s) has value %f while was expecting %f, full result comparison:\n%s", sample.Timestamp, ts.String(), sample.Value, expectedValue, comparison)
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

// accounts for float imprecision
func compareFloatValues(actual, expected, tolerance float64) bool {
	delta := math.Abs((actual - expected) / tolerance)
	return delta < tolerance
}

func compareHistogramValues(actual, expected *model.SampleHistogram, tolerance float64) bool {
	return compareFloatValues(float64(actual.Count), float64(expected.Count), tolerance) && compareFloatValues(float64(actual.Sum), float64(expected.Sum), tolerance) && compareHistogramBuckets(actual.Buckets, expected.Buckets, tolerance)
}

func compareHistogramBuckets(actual, expected model.HistogramBuckets, tolerance float64) bool {
	if len(actual) != len(expected) {
		return false
	}

	for i, bucket := range actual {
		if !compareHistogramBucketValues(bucket, expected[i], tolerance) {
			return false
		}
	}
	return true
}

func compareHistogramBucketValues(actual, expected *model.HistogramBucket, tolerance float64) bool {
	// the precision of lower/upper shouldn't change based on the range of the histogram counts/sums unlike the count
	return actual.Boundaries == expected.Boundaries && compareFloatValues(float64(actual.Lower), float64(expected.Lower), maxComparisonDeltaFloat) && compareFloatValues(float64(actual.Upper), float64(expected.Upper), maxComparisonDeltaFloat) && compareFloatValues(float64(actual.Count), float64(expected.Count), tolerance)
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

func formatExpectedAndActualValuesComparison(matrix model.Matrix, expectedSeries int, generateValue generateValueFunc) string {
	const precision = 4

	builder := strings.Builder{}
	builder.WriteString("Timestamp      Expected  Actual\n")

	samples := matrix[0].Values

	for _, sample := range samples {
		actual := float64(sample.Value)
		expected := float64(expectedSeries) * generateValue(sample.Timestamp.Time())
		match := compareFloatValues(actual, expected, maxComparisonDeltaFloat)

		builder.WriteString(strconv.FormatInt(int64(sample.Timestamp), 10))
		builder.WriteString(" (")
		builder.WriteString(sample.Timestamp.Time().UTC().Format(time.RFC3339))
		builder.WriteString(")  ")
		builder.WriteString(strconv.FormatFloat(expected, 'f', precision, 64))
		builder.WriteString("  ")
		builder.WriteString(strconv.FormatFloat(actual, 'f', precision, 64))

		if !match {
			builder.WriteString("  (value differs!)")
		}

		builder.WriteString("\n")
	}

	return builder.String()
}
