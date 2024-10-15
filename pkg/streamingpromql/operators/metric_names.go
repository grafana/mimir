// SPDX-License-Identifier: AGPL-3.0-only

package operators

import (
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

// MetricNames captures and stores the metric names of each series for later use in an operator.
//
// For example, it can be used to retrieve the metric name of a series for use in an annotation
// that can only be generated once the series data has been examined.
type MetricNames struct {
	names []string
}

func (n *MetricNames) CaptureMetricNames(metadata []types.SeriesMetadata) {
	n.names = make([]string, len(metadata))

	for i, series := range metadata {
		n.names[i] = series.Labels.Get(labels.MetricName)
	}
}

func (n *MetricNames) GetMetricNameForSeries(seriesIndex int) string {
	return n.names[seriesIndex]
}
