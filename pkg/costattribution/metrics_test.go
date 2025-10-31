// SPDX-License-Identifier: AGPL-3.0-only

package costattribution

import (
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewDescriptor(t *testing.T) {
	name := "test_metric"
	help := "A test metric"
	labels := []string{"label1", "label2"}
	constLabels := prometheus.Labels{"const_label": "value"}

	desc := newDescriptor(name, help, labels, constLabels)

	assert.NotNil(t, desc)
	assert.NotNil(t, desc.desc)
	assert.Equal(t, name, desc.name)
	assert.Equal(t, labels, desc.labels)
}

func TestDescriptor_Validate_Valid(t *testing.T) {
	testCases := map[string]struct {
		metricName  string
		help        string
		labels      []string
		constLabels prometheus.Labels
		expectedErr error
	}{
		"metric with valid labels and without constLabels": {
			metricName:  "test_metric_with_labels",
			help:        "A test metric with labels",
			labels:      []string{"label1", "label2"},
			constLabels: nil,
			expectedErr: nil,
		},
		"metric with valid labels and const labels": {
			metricName:  "test_metric_with_both_labels",
			help:        "A test metric with both label types",
			labels:      []string{"var_label"},
			constLabels: prometheus.Labels{"const_label": "value"},
			expectedErr: nil,
		},
		"metric with invalid label": {
			metricName:  "test_metric",
			help:        "Metric with invalid label",
			labels:      []string{"__bad_label__"},
			constLabels: nil,
			expectedErr: fmt.Errorf(`descriptor Desc{fqName: "test_metric", help: "Metric with invalid label", constLabels: {}, variableLabels: {__bad_label__}} is invalid: "__bad_label__" is not a valid label name for metric "test_metric"`),
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			desc := newDescriptor(tc.metricName, tc.help, tc.labels, tc.constLabels)
			err := desc.validate()
			if tc.expectedErr != nil {
				require.Error(t, err)
				require.EqualError(t, err, tc.expectedErr.Error())
			} else {
				require.NoError(t, err)
			}
		})
	}
}
