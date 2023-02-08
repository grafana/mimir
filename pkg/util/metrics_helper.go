// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/metrics_helper.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package util

import (
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/prometheus/model/labels"
)

// FromLabelPairsToLabels converts dto.LabelPair into labels.Labels.
func FromLabelPairsToLabels(pairs []*dto.LabelPair) labels.Labels {
	builder := labels.NewBuilder(nil)
	for _, pair := range pairs {
		builder.Set(pair.GetName(), pair.GetValue())
	}
	return builder.Labels(nil)
}

// GetSumOfHistogramSampleCount returns the sum of samples count of histograms matching the provided metric name
// and optional label matchers. Returns 0 if no metric matches.
func GetSumOfHistogramSampleCount(families []*dto.MetricFamily, metricName string, matchers labels.Selector) uint64 {
	sum := uint64(0)

	for _, metric := range families {
		if metric.GetName() != metricName {
			continue
		}

		if metric.GetType() != dto.MetricType_HISTOGRAM {
			continue
		}

		for _, series := range metric.GetMetric() {
			if !matchers.Matches(FromLabelPairsToLabels(series.GetLabel())) {
				continue
			}

			histogram := series.GetHistogram()
			sum += histogram.GetSampleCount()
		}
	}

	return sum
}
