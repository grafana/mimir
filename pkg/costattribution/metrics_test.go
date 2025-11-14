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
			desc, err := newDescriptor(tc.metricName, tc.help, tc.labels, tc.constLabels)
			if tc.expectedErr != nil {
				require.Error(t, err)
				require.EqualError(t, err, tc.expectedErr.Error())
				require.Nil(t, desc)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, desc.desc)
				assert.Equal(t, tc.metricName, desc.name)
				assert.Equal(t, tc.labels, desc.labels)
			}
		})
	}
}
