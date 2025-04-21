// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/tsdb/block_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package tsdbcodec

import (
	"strconv"
	"time"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
)

const intervalSeconds = 10
const interval = intervalSeconds * time.Second

const NumIntervals = 10000 + int(time.Minute/interval) + 1 // The longest-range test we run has 10000 steps with a 1m range selector, so make sure we have slightly more data than that.

var DefaultMetricSizes = []int{1, 100, 2000}

const DefaultHistogramBuckets = 5

func GenerateTestLabelSets(labelCardinalities []int, sampleCount int) []labels.Labels {
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
	return metricLabels
}

func GenerateTestStorageSeriesFromLabelSets(
	metricLabelSets []labels.Labels, labelCardinalities []int, minT int, sampleCount int,
) []storage.Series {
	maxT := minT + sampleCount

	totalSeries := 0
	for _, labelCount := range labelCardinalities {
		totalSeries += 2 * labelCount // one series each for a_ and b_ metric names
	}

	if len(labelCardinalities) == 0 {
		return nil
	}

	series := make([]storage.Series, totalSeries)
	for i, metricLabelSet := range metricLabelSets {
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

func equalSeries(series1, series2 storage.Series) bool {
	iter1 := series1.Iterator(nil)
	iter2 := series2.Iterator(nil)

	for {
		nextValType1 := iter1.Next()
		nextValType2 := iter2.Next()

		// if one iterator ends (returns ValNone) before the other, they are not equal
		if nextValType1 != nextValType2 {
			return false
		}

		// nextVals are same type;
		// if both iterators are ValNone and we did not fail previously,
		// then both are done and the series are equal
		if nextValType1 == chunkenc.ValNone {
			return true
		}

		switch nextValType1 {
		case chunkenc.ValFloat:
			t1, v1 := iter1.At()
			t2, v2 := iter2.At()
			if t1 != t2 || v1 != v2 {
				return false
			}
		case chunkenc.ValHistogram:
			t1, h1 := iter1.AtHistogram(nil)
			t2, h2 := iter2.AtHistogram(nil)
			if t1 != t2 || h1 != h2 {
				return false
			}
		case chunkenc.ValFloatHistogram:
			t1, fh1 := iter1.AtFloatHistogram(nil)
			t2, fh2 := iter2.AtFloatHistogram(nil)
			if t1 != t2 || fh1 != fh2 {
				return false
			}
		default:
			// unknown sample type
			return false
		}
	}
}
