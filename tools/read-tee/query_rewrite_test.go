// SPDX-License-Identifier: AGPL-3.0-only

package readtee

import (
	"testing"

	"github.com/prometheus/prometheus/model/labels"
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
			got, err := rewriteQuery(tc.query, replica, rewriteOptions{})
			require.NoError(t, err)
			require.Equal(t, tc.want, got)

			// Round-trip: the rewritten output must still parse as valid PromQL.
			_, err = promQLParser.ParseExpr(got)
			require.NoError(t, err, "rewritten query must be valid PromQL: %q", got)
		})
	}
}

func TestRewriteQuery_InvalidPromQL(t *testing.T) {
	_, err := rewriteQuery(`this is )( not promql`, 2, rewriteOptions{})
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
			got, err := rewriteSelector(tc.sel, replica, rewriteOptions{})
			require.NoError(t, err)
			require.Equal(t, tc.want, got)

			// Round-trip: the rewritten selector must still parse.
			_, err = promQLParser.ParseMetricSelector(got)
			require.NoError(t, err, "rewritten selector must be valid: %q", got)
		})
	}
}

func TestRewriteSelector_InvalidSelector(t *testing.T) {
	_, err := rewriteSelector(`{not a selector`, 2, rewriteOptions{})
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
	got, err := rewriteQuery(`x{job=~"api.*"}`, 3, rewriteOptions{})
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

func TestRewriteQuery_ExcludeAmplifiedNegative(t *testing.T) {
	const replica = 2
	opts := rewriteOptions{excludeAmplifiedNegative: true}

	tests := []struct {
		name  string
		query string
		want  string
	}{
		{
			name:  "equal matcher still suffixed with replica",
			query: `x{job="api"}`,
			want:  `x{job="api_amp2"}`,
		},
		{
			name:  "regexp matcher still suffixed with replica",
			query: `x{job=~"api.*"}`,
			want:  `x{job=~"(?:api.*)_amp2"}`,
		},
		{
			name:  "not-equal becomes not-regexp excluding value and all amp variants",
			query: `x{job!="api"}`,
			want:  `x{job!~"api(?:_amp[0-9]+)?"}`,
		},
		{
			name:  "not-regexp excludes matches and all amp variants",
			query: `x{job!~"foo|bar"}`,
			want:  `x{job!~"(?:foo|bar)(?:_amp[0-9]+)?"}`,
		},
		{
			name:  "mixed positive suffixed, negative excludes all variants",
			query: `x{cluster="c1",job!~"foo|bar"}`,
			want:  `x{cluster="c1_amp2",job!~"(?:foo|bar)(?:_amp[0-9]+)?"}`,
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
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := rewriteQuery(tc.query, replica, opts)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)

			_, err = promQLParser.ParseExpr(got)
			require.NoError(t, err, "rewritten query must be valid PromQL: %q", got)
		})
	}
}

// TestRewriteQuery_ExcludeAmplifiedNegativeSemantics verifies the compiled matchers behave as
// intended: negative matchers exclude the original value and its _amp{N} variants only, without
// over-excluding values that merely share the prefix, and != values are treated literally.
func TestRewriteQuery_ExcludeAmplifiedNegativeSemantics(t *testing.T) {
	opts := rewriteOptions{excludeAmplifiedNegative: true}

	extract := func(t *testing.T, query, label string) *labels.Matcher {
		t.Helper()
		got, err := rewriteQuery(query, 3, opts)
		require.NoError(t, err)
		ms, err := promQLParser.ParseMetricSelector(got)
		require.NoError(t, err)
		for _, m := range ms {
			if m.Name == label {
				return m
			}
		}
		t.Fatalf("no matcher for label %q in %q", label, got)
		return nil
	}

	t.Run("not-equal excludes value and amp variants only", func(t *testing.T) {
		m := extract(t, `x{job!="api"}`, "job")
		require.False(t, m.Matches("api"))
		require.False(t, m.Matches("api_amp3"))
		require.False(t, m.Matches("api_amp10"))
		require.True(t, m.Matches("apix"))
		require.True(t, m.Matches("other"))
	})

	t.Run("not-equal treats the value literally (regex-quoted)", func(t *testing.T) {
		m := extract(t, `x{job!="a.b"}`, "job")
		require.False(t, m.Matches("a.b"))
		require.False(t, m.Matches("a.b_amp2"))
		require.True(t, m.Matches("axb"), "the dot must be literal, not a wildcard")
	})

	t.Run("not-regexp is precise and covers amp variants", func(t *testing.T) {
		m := extract(t, `x{job!~"foo|bar"}`, "job")
		require.False(t, m.Matches("foo"))
		require.False(t, m.Matches("bar"))
		require.False(t, m.Matches("foo_amp5"))
		require.False(t, m.Matches("bar_amp1"))
		require.True(t, m.Matches("baz"))
		require.True(t, m.Matches("baz_amp5"))
		require.True(t, m.Matches("foobar"), "must not over-exclude values that merely share the prefix")
	})
}
