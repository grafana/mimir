// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/tsdb/block_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package tsdbcodec

import (
	"math/rand/v2"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/tsdbutil"
)

const intervalSeconds = 10
const interval = intervalSeconds * time.Second

const NumIntervals = 10000 + int(time.Minute/interval) + 1 // The longest-range test we run has 10000 steps with a 1m range selector, so make sure we have slightly more data than that.

var DefaultMetricSizes = []int{1, 100, 2000}

const DefaultHistogramBuckets = 5

func GenerateTestLabelSets(metricSizes []int, numHistogramBuckets int) []labels.Labels {
	if numHistogramBuckets <= 0 {
		numHistogramBuckets = DefaultHistogramBuckets
	}

	totalMetrics := 0

	for _, size := range metricSizes {
		totalMetrics += (2 + numHistogramBuckets + 1 + 1) * size // 2 non-histogram metrics + 5 metrics for histogram buckets + 1 metric for +Inf histogram bucket + 1 metric for native-histograms
	}

	metrics := make([]labels.Labels, 0, totalMetrics)

	for _, size := range metricSizes {
		aName := "a_" + strconv.Itoa(size)
		bName := "b_" + strconv.Itoa(size)
		histogramName := "h_" + strconv.Itoa(size)
		nativeHistogramName := "nh_" + strconv.Itoa(size)

		if size == 1 {
			// We don't want a "l" label on metrics with one series (some test cases rely on this label not being present).
			metrics = append(metrics, labels.FromStrings("__name__", aName))
			metrics = append(metrics, labels.FromStrings("__name__", bName))
			for le := 0; le < numHistogramBuckets; le++ {
				metrics = append(metrics, labels.FromStrings("__name__", histogramName, "le", strconv.Itoa(le)))
			}
			metrics = append(metrics, labels.FromStrings("__name__", histogramName, "le", "+Inf"))
			metrics = append(metrics, labels.FromStrings("__name__", nativeHistogramName))
		} else {
			for i := 0; i < size; i++ {
				metrics = append(metrics, labels.FromStrings("__name__", aName, "l", strconv.Itoa(i)))
				metrics = append(metrics, labels.FromStrings("__name__", bName, "l", strconv.Itoa(i)))
				for le := 0; le < numHistogramBuckets; le++ {
					metrics = append(metrics, labels.FromStrings("__name__", histogramName, "l", strconv.Itoa(i), "le", strconv.Itoa(le)))
				}
				metrics = append(metrics, labels.FromStrings("__name__", histogramName, "l", strconv.Itoa(i), "le", "+Inf"))
				metrics = append(metrics, labels.FromStrings("__name__", nativeHistogramName, "l", strconv.Itoa(i)))
			}
		}
	}
	return metrics
}

func GenerateTestStorageSeriesFromLabelSets(metricLabelSets []labels.Labels) []storage.Series {
	// Batch samples into separate requests
	// There is no precise science behind this number: based on a few experiments,
	// batching by 1000 gives a good balance between peak memory consumption and run time.
	//
	// (francoposa) Batching logic is left over from pkg/streamingpromql/benchmarks/ingester.go;
	// in that code it is used to control how many series are in memory before being written.
	// I kept the code but flattened its effect here to just return one big slice of series,
	// but if we have a need to control the number of series in memory at any time,
	// we can use batching and return some sort of iterator of slices of series instead.
	batchSize := 1000

	//histogramSpans := []histogram.Span{{Offset: 0, Length: 2}, {Offset: 1, Length: 2}}

	storageSeries := make([]storage.Series, 0, len(metricLabelSets))

	for start := 0; start < NumIntervals; start += batchSize {
		end := start + batchSize
		if end > NumIntervals {
			end = NumIntervals
		}

		sampleCount := end - start

		for metricIdx, labelSet := range metricLabelSets {
			series := promql.Series{}

			if strings.HasPrefix(labelSet.Get("__name__"), "nh_") {
				if cap(series.Histograms) < sampleCount {
					series.Histograms = make([]promql.HPoint, sampleCount)
				}

				series.Histograms = series.Histograms[:sampleCount]

				for ts := start; ts < end; ts++ {
					// TODO(jhesketh): Fix this with some better data
					series.Histograms[ts-start].T = int64(ts) * interval.Milliseconds()
					series.Histograms[ts-start].H = tsdbutil.GenerateTestFloatHistogram(int64(metricIdx))
				}
			} else {
				if cap(series.Floats) < sampleCount {
					series.Floats = make([]promql.FPoint, sampleCount)
				}

				series.Floats = series.Floats[:sampleCount]

				for ts := start; ts < end; ts++ {
					series.Floats[ts-start].T = int64(ts) * interval.Milliseconds()
					series.Floats[ts-start].F = float64(ts) + float64(metricIdx)/float64(len(metricLabelSets))
				}
			}

			storageSeries = append(storageSeries, promql.NewStorageSeries(series))
		}
	}

	return storageSeries
}

//// genSeries generates series of float64 samples with a given number of labels and values.
//func genFPoints(totalSeries, labelCount int, mint, maxt int64) []promql.FPoint {
//	return genSeriesFromSampleGenerator(totalSeries, labelCount, mint, maxt, 1, generateFPoint)
//}
//func generateFPoint(ts int64, i int, lenMetricLabelSets int) promql.FPoint {
//	return promql.FPoint{T: ts, F: float64(ts) + float64(i)/float64(lenMetricLabelSets)}
//}
//
//func genSeriesFromSampleGenerator(
//	totalSeries, labelCount int,
//	mint, maxt, step int64,
//	generator func(ts int64, i int, lenMetricLabelSets int) promql.FPoint) []promql.FPoint {
//	if totalSeries == 0 || labelCount == 0 {
//		return nil
//	}
//
//	series := make([]promql.FPoint, totalSeries)
//
//	for i := 0; i < totalSeries; i++ {
//		//lbls := make(map[string]string, labelCount)
//		//lbls[defaultLabelName] = strconv.Itoa(i)
//		//for j := 1; len(lbls) < labelCount; j++ {
//		//	lbls[defaultLabelName+strconv.Itoa(j)] = defaultLabelValue + strconv.Itoa(j)
//		//}
//		samples := make([]promql.FPoint, 0, (maxt-mint)/step+1)
//		for t := mint; t < maxt; t += step {
//			samples = append(samples, generator(t, i, labelCount))
//		}
//		//series[i] = storage.NewListSeries(labels.FromMap(lbls), samples)
//	}
//	return series
//}

const (
	defaultLabelName  = "__name__"
	defaultLabelValue = "labelValue"
)

// genSeries generates series of float64 samples with a given number of labels and values.
func genSeries(labelCardinalities []int, minT int, sampleCount int) []storage.Series {
	maxT := minT + sampleCount

	totalSeries := 0
	for _, labelCount := range labelCardinalities {
		totalSeries += 2 * labelCount // one series each for a_ and b_ metric names
	}

	if len(labelCardinalities) == 0 {
		return nil
	}

	metricLabels := make([]labels.Labels, 0, sampleCount)

	for _, labelCount := range labelCardinalities {
		aMetricName := "a_" + strconv.Itoa(labelCount)
		bMetricName := "b_" + strconv.Itoa(labelCount)

		if labelCount == 1 {
			// metricLabels with one series only have the __name__ label
			metricLabels = append(metricLabels, labels.FromStrings("__name__", aMetricName))
			metricLabels = append(metricLabels, labels.FromStrings("__name__", bMetricName))
		} else {
			// metricLabels with more than one series have the __name__ label
			// and a one "l" label with `labelCount` different values
			for i := 0; i < labelCount; i++ {
				labelValue := strconv.Itoa(i)
				metricLabels = append(metricLabels, labels.FromStrings("__name__", aMetricName, "l", labelValue))
				metricLabels = append(metricLabels, labels.FromStrings("__name__", bMetricName, "l", labelValue))
			}
		}
	}

	series := make([]storage.Series, totalSeries)
	for i, metricLabelSet := range metricLabels {
		samples := make([]chunks.Sample, 0, sampleCount)
		for t := minT; t < maxT; t++ {
			samples = append(samples, sample{
				t: int64(t) * interval.Milliseconds(),
				f: float64(t) + float64(i)/float64(totalSeries),
			})
		}
		series[i] = storage.NewListSeries(metricLabelSet, samples)
	}
	return series
}

//// genHistogramSeries generates series of histogram samples with a given number of labels and values.
//func genHistogramSeries(totalSeries, labelCount int, mint, maxt, step int64, floatHistogram bool) []storage.Series {
//	return genSeriesFromSampleGenerator(totalSeries, labelCount, mint, maxt, step, func(ts int64) chunks.Sample {
//		h := &histogram.Histogram{
//			Count:         7 + uint64(ts*5),
//			ZeroCount:     2 + uint64(ts),
//			ZeroThreshold: 0.001,
//			Sum:           18.4 * rand.Float64(),
//			Schema:        1,
//			PositiveSpans: []histogram.Span{
//				{Offset: 0, Length: 2},
//				{Offset: 1, Length: 2},
//			},
//			PositiveBuckets: []int64{ts + 1, 1, -1, 0},
//		}
//		if ts != mint {
//			// By setting the counter reset hint to "no counter
//			// reset" for all histograms but the first, we cover the
//			// most common cases. If the series is manipulated later
//			// or spans more than one block when ingested into the
//			// storage, the hint has to be adjusted. Note that the
//			// storage itself treats this particular hint the same
//			// as "unknown".
//			h.CounterResetHint = histogram.NotCounterReset
//		}
//		if floatHistogram {
//			return sample{t: ts, fh: h.ToFloat(nil)}
//		}
//		return sample{t: ts, h: h}
//	})
//}

//// genHistogramAndFloatSeries generates series of mixed histogram and float64 samples with a given number of labels and values.
//func genHistogramAndFloatSeries(totalSeries, labelCount int, mint, maxt, step int64, floatHistogram bool) []storage.Series {
//	floatSample := false
//	count := 0
//	return genSeriesFromSampleGenerator(totalSeries, labelCount, mint, maxt, step, func(ts int64) chunks.Sample {
//		count++
//		var s sample
//		if floatSample {
//			s = sample{t: ts, f: rand.Float64()}
//		} else {
//			h := &histogram.Histogram{
//				Count:         7 + uint64(ts*5),
//				ZeroCount:     2 + uint64(ts),
//				ZeroThreshold: 0.001,
//				Sum:           18.4 * rand.Float64(),
//				Schema:        1,
//				PositiveSpans: []histogram.Span{
//					{Offset: 0, Length: 2},
//					{Offset: 1, Length: 2},
//				},
//				PositiveBuckets: []int64{ts + 1, 1, -1, 0},
//			}
//			if count > 1 && count%5 != 1 {
//				// Same rationale for this as above in
//				// genHistogramSeries, just that we have to be
//				// smarter to find out if the previous sample
//				// was a histogram, too.
//				h.CounterResetHint = histogram.NotCounterReset
//			}
//			if floatHistogram {
//				s = sample{t: ts, fh: h.ToFloat(nil)}
//			} else {
//				s = sample{t: ts, h: h}
//			}
//		}
//
//		if count%5 == 0 {
//			// Flip the sample type for every 5 samples.
//			floatSample = !floatSample
//		}
//
//		return s
//	})
//}

func genSeriesFromSampleGenerator(totalSeries, labelCount int, mint, maxt, step int64) []storage.Series {
	if totalSeries == 0 || labelCount == 0 {
		return nil
	}

	series := make([]storage.Series, totalSeries)

	for i := 0; i < totalSeries; i++ {
		lbls := make(map[string]string, labelCount)
		lbls[defaultLabelName] = strconv.Itoa(i)
		for j := 1; len(lbls) < labelCount; j++ {
			lbls[defaultLabelName+strconv.Itoa(j)] = defaultLabelValue + strconv.Itoa(j)
		}
		samples := make([]chunks.Sample, 0, (maxt-mint)/step+1)
		for t := mint; t < maxt; t += step {
			samples = append(samples, sample{t: t, f: rand.Float64()})
		}
		series[i] = storage.NewListSeries(labels.FromMap(lbls), samples)
	}
	return series
}

// populateSeries generates series from given labels, mint and maxt.
func populateSeries(lbls []map[string]string, mint, maxt int64) []storage.Series {
	if len(lbls) == 0 {
		return nil
	}

	series := make([]storage.Series, 0, len(lbls))
	for _, lbl := range lbls {
		if len(lbl) == 0 {
			continue
		}
		samples := make([]chunks.Sample, 0, maxt-mint+1)
		for t := mint; t <= maxt; t++ {
			samples = append(samples, sample{t: t, f: rand.Float64()})
		}
		series = append(series, storage.NewListSeries(labels.FromMap(lbl), samples))
	}
	return series
}

type sample struct {
	t  int64
	f  float64
	h  *histogram.Histogram
	fh *histogram.FloatHistogram
}

func newSample(t int64, v float64, h *histogram.Histogram, fh *histogram.FloatHistogram) chunks.Sample {
	return sample{t, v, h, fh}
}

func (s sample) T() int64                      { return s.t }
func (s sample) F() float64                    { return s.f }
func (s sample) H() *histogram.Histogram       { return s.h }
func (s sample) FH() *histogram.FloatHistogram { return s.fh }

func (s sample) Type() chunkenc.ValueType {
	switch {
	case s.h != nil:
		return chunkenc.ValHistogram
	case s.fh != nil:
		return chunkenc.ValFloatHistogram
	default:
		return chunkenc.ValFloat
	}
}

func (s sample) Copy() chunks.Sample {
	c := sample{t: s.t, f: s.f}
	if s.h != nil {
		c.h = s.h.Copy()
	}
	if s.fh != nil {
		c.fh = s.fh.Copy()
	}
	return c
}
