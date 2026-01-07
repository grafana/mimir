// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFindSegmentLabelCandidates(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		expected []string
	}{
		{
			name:     "single selector with equal matcher",
			query:    `metric{cluster="us-east-1"}`,
			expected: []string{"__name__", "cluster"},
		},
		{
			name:     "single selector with multiple equal matchers",
			query:    `metric{cluster="us-east-1", namespace="production"}`,
			expected: []string{"__name__", "cluster", "namespace"},
		},
		{
			name:     "single selector with regexp set matcher",
			query:    `metric{cluster=~"us-east-1|us-west-2"}`,
			expected: []string{"__name__", "cluster"},
		},
		{
			name:     "single selector with non-set regexp matcher",
			query:    `metric{cluster=~"us-.*"}`,
			expected: []string{"__name__"},
		},
		{
			name:     "single selector with not-equal matcher",
			query:    `metric{cluster!="us-east-1"}`,
			expected: []string{"__name__"},
		},
		{
			name:     "single selector with not-regexp matcher",
			query:    `metric{cluster!~"us-.*"}`,
			expected: []string{"__name__"},
		},
		{
			name:     "two aggregations with same label in both",
			query:    `sum(rate(metric1{cluster="us-east-1"}[5m])) + sum(rate(metric2{cluster="us-west-2"}[5m]))`,
			expected: []string{"__name__", "cluster"},
		},
		{
			name:     "two aggregations with label only in first",
			query:    `sum(rate(metric1{cluster="us-east-1"}[5m])) + sum(rate(metric2{namespace="production"}[5m]))`,
			expected: []string{"__name__"},
		},
		{
			name:     "two aggregations with common and different labels",
			query:    `sum(rate(metric1{cluster="us-east-1", foo="bar"}[5m])) + sum(rate(metric2{cluster="us-west-2", baz="qux"}[5m]))`,
			expected: []string{"__name__", "cluster"},
		},
		{
			name:     "aggregation with by clause - label in selector",
			query:    `sum by (cluster) (rate(metric{cluster="us-east-1"}[5m]))`,
			expected: []string{"__name__", "cluster"},
		},
		{
			name:     "binary operation with both sides having same label",
			query:    `sum(rate(metric1{cluster="us-east-1"}[5m])) / sum(rate(metric2{cluster="us-east-1"}[5m]))`,
			expected: []string{"__name__", "cluster"},
		},
		{
			name:     "subquery with label",
			query:    `max_over_time(rate(metric{cluster="us-east-1"}[5m])[1h:])`,
			expected: []string{"__name__", "cluster"},
		},
		{
			name:     "mixed equal and regexp set matchers",
			query:    `sum(rate(metric{cluster="us-east-1", env=~"prod|staging"}[5m]))`,
			expected: []string{"__name__", "cluster", "env"},
		},
		{
			name:     "three aggregations with common label",
			query:    `sum(rate(metric1{cluster="a"}[5m])) + sum(rate(metric2{cluster="b"}[5m])) + sum(rate(metric3{cluster="c"}[5m]))`,
			expected: []string{"__name__", "cluster"},
		},
		{
			name:     "no matchers besides metric name",
			query:    `metric`,
			expected: []string{"__name__"},
		},
		{
			name:     "empty query returns error",
			query:    ``,
			expected: nil,
		},
		{
			name:     "invalid query returns error",
			query:    `metric{invalid`,
			expected: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := FindSegmentLabelCandidates(tc.query)

			if tc.expected == nil {
				// Expect an error for invalid queries.
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.ElementsMatch(t, tc.expected, result)
		})
	}
}
