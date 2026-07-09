// SPDX-License-Identifier: AGPL-3.0-only

package readtee

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRewriteQuery(t *testing.T) {
	const replica = 2

	tests := []struct {
		name  string
		query string
		want  string
	}{
		{
			name:  "equal matcher suffixed, __name__ unchanged",
			query: `http_requests_total{job="api"}`,
			want:  `http_requests_total{job="api_amp2"}`,
		},
		{
			name:  "__name__ matcher not suffixed, other label suffixed",
			query: `{__name__="up", cluster="c1"}`,
			want:  `{__name__="up",cluster="c1_amp2"}`,
		},
		{
			name:  "regexp matcher grouped and suffixed",
			query: `x{job=~"api.*"}`,
			want:  `x{job=~"(?:api.*)_amp2"}`,
		},
		{
			name:  "not-equal matcher suffixed",
			query: `x{job!="api"}`,
			want:  `x{job!="api_amp2"}`,
		},
		{
			name:  "not-regexp matcher grouped and suffixed",
			query: `x{job!~"api.*"}`,
			want:  `x{job!~"(?:api.*)_amp2"}`,
		},
		{
			name:  "absence matcher unchanged",
			query: `x{job=""}`,
			want:  `x{job=""}`,
		},
		{
			name:  "presence matcher unchanged",
			query: `x{job!=""}`,
			want:  `x{job!=""}`,
		},
		{
			name:  "multi-matcher with aggregation grouping unchanged",
			query: `sum by(job)(rate(m{cluster="c1",job="api"}[5m]))`,
			want:  `sum by (job) (rate(m{cluster="c1_amp2",job="api_amp2"}[5m]))`,
		},
		{
			name:  "binary op rewrites both sides, grouping labels untouched",
			query: `sum(a{x="1"}) / on(x) sum(b{x="2"})`,
			want:  `sum(a{x="1_amp2"}) / on (x) sum(b{x="2_amp2"})`,
		},
		{
			name:  "offset and @ modifiers preserved",
			query: `a{x="1"} offset 5m @ 100`,
			want:  `a{x="1_amp2"} @ 100.000 offset 5m`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := rewriteQuery(tc.query, replica)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)

			// Round-trip: the rewritten output must still parse as valid PromQL.
			_, err = promQLParser.ParseExpr(got)
			require.NoError(t, err, "rewritten query must be valid PromQL: %q", got)
		})
	}
}

func TestRewriteQuery_InvalidPromQL(t *testing.T) {
	_, err := rewriteQuery(`this is )( not promql`, 2)
	require.Error(t, err)
}

func TestRewriteSelector(t *testing.T) {
	const replica = 2

	tests := []struct {
		name string
		sel  string
		want string
	}{
		{
			name: "match[] selector: instance suffixed, __name__ unchanged",
			sel:  `{__name__="up",instance="i1"}`,
			want: `up{instance="i1_amp2"}`,
		},
		{
			name: "match[] with metric name and regexp",
			sel:  `node_cpu{mode=~"idle|user"}`,
			want: `node_cpu{mode=~"(?:idle|user)_amp2"}`,
		},
		{
			name: "match[] absence matcher unchanged",
			sel:  `metric{label=""}`,
			want: `metric{label=""}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := rewriteSelector(tc.sel, replica)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)

			// Round-trip: the rewritten selector must still parse.
			_, err = promQLParser.ParseMetricSelector(got)
			require.NoError(t, err, "rewritten selector must be valid: %q", got)
		})
	}
}

func TestRewriteSelector_InvalidSelector(t *testing.T) {
	_, err := rewriteSelector(`{not a selector`, 2)
	require.Error(t, err)
}

func TestAmpSuffix(t *testing.T) {
	require.Equal(t, "_amp2", ampSuffix(2))
	require.Equal(t, "_amp3", ampSuffix(3))
	require.Equal(t, "_amp10", ampSuffix(10))
}

// TestRewriteRegexpAlignment documents that the grouped regex actually matches the amplified
// value "<original>_amp{k}" produced by write-tee, since Prometheus regex matchers are fully
// anchored.
func TestRewriteRegexpAlignment(t *testing.T) {
	got, err := rewriteQuery(`x{job=~"api.*"}`, 3)
	require.NoError(t, err)
	require.Equal(t, `x{job=~"(?:api.*)_amp3"}`, got)

	// Extract the compiled matcher and verify it matches an amplified value but not a bare one.
	m, err := promQLParser.ParseMetricSelector(got)
	require.NoError(t, err)
	var jobMatcher = m[0]
	for _, mm := range m {
		if mm.Name == "job" {
			jobMatcher = mm
		}
	}
	require.True(t, jobMatcher.Matches("apifoo_amp3"), "should match amplified value")
	require.False(t, jobMatcher.Matches("apifoo"), "should not match un-amplified value")
}
