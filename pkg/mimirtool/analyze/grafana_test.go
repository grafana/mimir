// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/cortex-tools/blob/main/pkg/analyse/grafana.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package analyze

import (
	"testing"
)

func TestParseQuery(t *testing.T) {
	tests := []struct {
		query           string
		expectedMetrics map[string]struct{}
		shouldError     bool
		errorMsg        string
	}{
		{
			query: `sum(rate(my_metric[$__interval])) by (my_label) > 0`,
			expectedMetrics: map[string]struct{}{
				"my_metric": {},
			},
			shouldError: false,
			errorMsg:    "",
		},
		{
			query: `sum(rate(my_metric[$interval])) by (my_label) > 0`,
			expectedMetrics: map[string]struct{}{
				"my_metric": {},
			},
			shouldError: false,
			errorMsg:    "",
		},
		{
			query: `sum(rate(my_metric[$resolution])) by (my_label) > 0`,
			expectedMetrics: map[string]struct{}{
				"my_metric": {},
			},
			shouldError: false,
			errorMsg:    "",
		},
		{
			query: `sum(rate(my_metric[$__rate_interval])) by (my_label) > 0`,
			expectedMetrics: map[string]struct{}{
				"my_metric": {},
			},
			shouldError: false,
			errorMsg:    "",
		},
		{
			query: `sum(rate(my_metric[$__range])) by (my_label) > 0`,
			expectedMetrics: map[string]struct{}{
				"my_metric": {},
			},
			shouldError: false,
			errorMsg:    "",
		},
		{
			query: `sum(rate(my_metric[$agregation_window])) by (my_label) > 0`,
			expectedMetrics: map[string]struct{}{
				"my_metric": {},
			},
			shouldError: false,
			errorMsg:    "",
		},
		{
			query: `sum(my_metric)`,
			expectedMetrics: map[string]struct{}{
				"my_metric": {},
			},
			shouldError: false,
			errorMsg:    "",
		},
		{
			query: `my_metric`,
			expectedMetrics: map[string]struct{}{
				"my_metric": {},
			},
			shouldError: false,
			errorMsg:    "",
		},
		{
			query: `my_metric{label=${value}}`,
			expectedMetrics: map[string]struct{}{
				"my_metric": {},
			},
			shouldError: false,
			errorMsg:    "",
		},
		{
			query: `my_metric{label=${value:format}}`,
			expectedMetrics: map[string]struct{}{
				"my_metric": {},
			},
			shouldError: false,
			errorMsg:    "",
		},
		{
			query: `my_metric{label=$value}`,
			expectedMetrics: map[string]struct{}{
				"my_metric": {},
			},
			shouldError: false,
			errorMsg:    "",
		},
		{
			query: `$my_metric{label=$value}`,
			expectedMetrics: map[string]struct{}{
				"variable": {},
			},
			shouldError: false,
			errorMsg:    "",
		},
		{
			query: `${my_metric}{label=$value}`,
			expectedMetrics: map[string]struct{}{
				"variable": {},
			},
			shouldError: false,
			errorMsg:    "",
		},
		{
			query: `${my_metric:format}{label=$value}`,
			expectedMetrics: map[string]struct{}{
				"variable": {},
			},
			shouldError: false,
			errorMsg:    "",
		},
	}

	for _, test := range tests {
		metrics := make(map[string]struct{})
		err := parseQuery(test.query, metrics)

		if test.shouldError && err == nil {
			t.Errorf("Expected error, but got no error for query: %s", test.query)
		}

		if !test.shouldError && err != nil {
			t.Errorf("Unexpected error for query: %s: %v", test.query, err)
		}

		if len(metrics) != len(test.expectedMetrics) {
			t.Errorf("For query %s: Expected %d metrics, but got %d", test.query, len(test.expectedMetrics), len(metrics))
		} else {
			for m := range metrics {
				if _, ok := test.expectedMetrics[m]; !ok {
					t.Errorf("For query %s: Unexpected metric found: %s", test.query, m)
				}
			}
		}
	}
}
