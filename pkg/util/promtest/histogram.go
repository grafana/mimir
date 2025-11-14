// SPDX-License-Identifier: AGPL-3.0-only

package promtest

import (
	"fmt"
	"slices"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// HasNativeHistogram checks if a gathered metric has a native histogram.
//
// A histogram uses native format if it uses negative/positive span or zero_count > 0.
// If zero_count is 0, positive span always contains a no-op span.
func HasNativeHistogram(g prometheus.Gatherer, metricName string) error {
	family, err := gatherMetricFamily(g, metricName)
	if err != nil {
		return err
	}
	histograms, err := getHistograms(family)
	if err != nil {
		return err
	}
	if len(histograms) == 0 {
		return fmt.Errorf("metric family %s has no histograms", metricName)
	}
	for i, h := range histograms {
		switch {
		case h.ZeroCount != nil && *h.ZeroCount > 0:
		case h.ZeroCountFloat != nil && *h.ZeroCountFloat > 0:
		case len(h.PositiveSpan) > 0:
		case len(h.NegativeSpan) > 0:
		default:
			return fmt.Errorf("metric %d is not a native histogram", i)
		}
	}
	return nil
}

// HasSampleCount checks if a gathered metric has a specific sample count.
func HasSampleCount(g prometheus.Gatherer, metricName string, wantCount float64) error {
	family, err := gatherMetricFamily(g, metricName)
	if err != nil {
		return err
	}
	histograms, err := getHistograms(family)
	if err != nil {
		return err
	}
	for i, h := range histograms {
		var sampleCount float64
		switch {
		case h.SampleCount != nil:
			sampleCount = float64(*h.SampleCount)
		case h.SampleCountFloat != nil:
			sampleCount = *h.SampleCountFloat
		default:
			return fmt.Errorf("histogram %d with missing sample count", i)
		}
		if sampleCount != wantCount {
			return fmt.Errorf("histogram %d should have %f samples but got %f", i, wantCount, sampleCount)
		}
	}
	return nil
}

func getHistograms(family *dto.MetricFamily) ([]*dto.Histogram, error) {
	feType := family.Type
	if feType == nil || (*feType != dto.MetricType_HISTOGRAM && (*feType != dto.MetricType_GAUGE_HISTOGRAM)) {
		return nil, fmt.Errorf("family is not a histogram")
	}
	if len(family.Metric) == 0 {
		return nil, fmt.Errorf("family has no metrics")
	}
	out := make([]*dto.Histogram, len(family.Metric))
	for i, m := range family.Metric {
		if m.Histogram == nil {
			return nil, fmt.Errorf("metric %d has no histogram", i)
		}
		out[i] = m.Histogram
	}
	return out, nil
}

func gatherMetricFamily(g prometheus.Gatherer, metricName string) (*dto.MetricFamily, error) {
	got, err := g.Gather()
	if err != nil {
		return nil, fmt.Errorf("gathering metrics failed: %w", err)
	}
	index := slices.IndexFunc(got, func(mf *dto.MetricFamily) bool {
		return mf.GetName() == metricName
	})
	if index == -1 {
		return nil, fmt.Errorf("metric family %s not found", metricName)
	}
	return got[index], nil
}
