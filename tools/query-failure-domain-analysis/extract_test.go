// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtractMetricSelectors(t *testing.T) {
	tests := []struct {
		name           string
		expr           string
		wantNames      []string
		wantRegexCount int
		wantUnnamed    bool
		wantNegName    bool
	}{
		{
			name:      "single bare metric",
			expr:      `cpu_usage`,
			wantNames: []string{"cpu_usage"},
		},
		{
			name:      "metric with labels",
			expr:      `cpu_usage{pod="p1"}`,
			wantNames: []string{"cpu_usage"},
		},
		{
			name:      "binary op references two metrics",
			expr:      `cpu_usage / cpu_limit`,
			wantNames: []string{"cpu_limit", "cpu_usage"},
		},
		{
			name:      "duplicate name dedup",
			expr:      `cpu_usage + cpu_usage`,
			wantNames: []string{"cpu_usage"},
		},
		{
			name:      "explicit __name__ matcher",
			expr:      `{__name__="cpu_usage"}`,
			wantNames: []string{"cpu_usage"},
		},
		{
			name:           "regex matcher records compiled regex",
			expr:           `{__name__=~"cpu_(usage|limit)"}`,
			wantRegexCount: 1,
		},
		{
			name:        "no __name__ matcher means full shard",
			expr:        `count({namespace="prod"})`,
			wantUnnamed: true,
		},
		{
			name:      "complex query with range, subquery, aggregate",
			expr:      `sum(rate(http_requests_total{status="200"}[5m])) / sum(rate(http_requests_total[5m]))`,
			wantNames: []string{"http_requests_total"},
		},
		{
			name:        "negative regex on name leaves universe open",
			expr:        `{__name__!~"foo_.*", job="api"}`,
			wantNegName: true,
		},
		{
			name:      "function call wrapping vector selector",
			expr:      `histogram_quantile(0.9, sum by (le) (rate(request_duration_seconds_bucket[5m])))`,
			wantNames: []string{"request_duration_seconds_bucket"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pq, err := extractMetricSelectors(tc.expr)
			require.NoError(t, err)

			got := append([]string(nil), pq.EqualityNames...)
			sort.Strings(got)
			assert.Equal(t, tc.wantNames, got, "equality names")
			assert.Equal(t, tc.wantRegexCount, len(pq.NameRegexes), "regex count")
			assert.Equal(t, tc.wantUnnamed, pq.HasUnnamedSelector, "unnamed selector")
			assert.Equal(t, tc.wantNegName, pq.HasNegativeNameRegex, "negative name matcher")
		})
	}
}

func TestExtractMetricSelectors_ParseError(t *testing.T) {
	_, err := extractMetricSelectors(`this is not valid promql {`)
	require.Error(t, err)
}

func TestExtractMetricSelectors_FullShardDetection(t *testing.T) {
	// Multiple selectors where at least one has no __name__: query is unbucketable.
	pq, err := extractMetricSelectors(`cpu_usage + on() count({pod="x"})`)
	require.NoError(t, err)
	assert.True(t, pq.fullShard(), "expected full-shard because of bare label selector")
}
