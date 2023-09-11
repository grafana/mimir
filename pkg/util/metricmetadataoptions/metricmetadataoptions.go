// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/metricmetadataoptions/metricmetadataoptions.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package metricmetadataoptions

type MetricMetadataOptions struct {
	Limit          int32
	LimitPerMetric int32
	Metric         string
}

func DefaultMetricMetadataOptions() MetricMetadataOptions {
	return MetricMetadataOptions{
		Limit:          -1,
		LimitPerMetric: -1,
		Metric:         "",
	}
}
