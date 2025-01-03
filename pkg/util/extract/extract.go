// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/extract/extract.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package extract

import (
	"fmt"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/mimirpb_custom"
)

var (
	errNoMetricNameLabel = fmt.Errorf("No metric name label")
)

// UnsafeMetricNameFromLabelAdapters extracts the metric name from a list of LabelPairs.
// The returned metric name string is a reference to the label value (no copy).
func UnsafeMetricNameFromLabelAdapters(labels []mimirpb_custom.LabelAdapter) (string, error) {
	for _, label := range labels {
		if label.Name == model.MetricNameLabel {
			return label.Value, nil
		}
	}
	return "", errNoMetricNameLabel
}

// MetricNameFromLabels extracts the metric name from a list of Prometheus Labels.
func MetricNameFromLabels(lbls labels.Labels) (metricName string, err error) {
	metricName = lbls.Get(model.MetricNameLabel)
	if metricName == "" {
		err = errNoMetricNameLabel
	}
	return
}
