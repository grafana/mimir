// SPDX-License-Identifier: AGPL-3.0-only

package tsdbcodec

import (
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
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

	histogramSpans := []histogram.Span{{Offset: 0, Length: 2}, {Offset: 1, Length: 2}}

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
					series.Histograms[ts-start].H = &histogram.FloatHistogram{
						Count:         12,
						ZeroCount:     2,
						ZeroThreshold: 0.001,
						Sum:           18.4,
						Schema:        0,
						NegativeSpans: histogramSpans,
						PositiveSpans: histogramSpans,
					}
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
