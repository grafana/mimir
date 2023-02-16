// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/metrics_helper_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package util

import (
	"testing"

	dskit_metrics "github.com/grafana/dskit/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMatchesSelectors(t *testing.T) {
	tests := []struct {
		name      string
		selectors labels.Labels
		expected  bool
	}{
		{
			name: "with some matching selectors",
			selectors: labels.Labels{
				labels.Label{Name: "lbl1", Value: "value1"},
			},
			expected: true,
		},
		{
			name: "with all matching selectors",
			selectors: labels.Labels{
				labels.Label{Name: "lbl1", Value: "value1"},
				labels.Label{Name: "lbl2", Value: "value2"},
			},
			expected: true,
		},
		{
			name: "with non-matching selectors",
			selectors: labels.Labels{
				labels.Label{Name: "lbl1", Value: "barbaz"},
			},
			expected: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			counter := promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
				Name: "mimir_test_metric",
				Help: "a test metric",
			}, []string{"lbl1", "lbl2"})

			counter.With(prometheus.Labels{"lbl1": "value1", "lbl2": "value2"}).Add(50)

			metrics, err := dskit_metrics.NewMetricFamilyMapFromGatherer(reg)
			require.NoError(t, err)
			metric := metrics["mimir_test_metric"].Metric[0]
			assert.Equalf(t, tt.expected, MatchesSelectors(metric, tt.selectors), "MatchesSelectors(%v, %v)", metric, tt.selectors)
		})
	}
}
