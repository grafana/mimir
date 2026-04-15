// SPDX-License-Identifier: AGPL-3.0-only

package binops

import (
	"fmt"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func TestBuildMatchers(t *testing.T) {
	t.Run("single series single hint", func(t *testing.T) {
		series := generateSeriesMetadata("http_requests_total", 1)
		hints := &Hints{Include: []string{"container"}}
		expected := types.Matchers([]types.Matcher{{
			Type:  labels.MatchRegexp,
			Name:  "container",
			Value: "querier",
		}})

		res := BuildMatchers(series, hints)
		require.Equal(t, expected, res)
	})

	t.Run("multiple series single hint", func(t *testing.T) {
		series := generateSeriesMetadata("http_requests_total", 3)
		hints := &Hints{Include: []string{"container"}}
		expected := types.Matchers([]types.Matcher{{
			Type:  labels.MatchRegexp,
			Name:  "container",
			Value: "querier|query-frontend|store-gateway",
		}})

		res := BuildMatchers(series, hints)
		require.Equal(t, expected, res)
	})

	t.Run("multiple series multiple hints", func(t *testing.T) {
		series := generateSeriesMetadata("http_requests_total", 3)
		hints := &Hints{Include: []string{"container", "region"}}
		expected := types.Matchers([]types.Matcher{
			{
				Type:  labels.MatchRegexp,
				Name:  "container",
				Value: "querier|query-frontend|store-gateway",
			},
			{
				Type:  labels.MatchRegexp,
				Name:  "region",
				Value: "prod-test-1|prod-test-2|prod-test-3",
			},
		})

		res := BuildMatchers(series, hints)
		require.Equal(t, expected, res)
	})

	t.Run("too many values single hint", func(t *testing.T) {
		series := generateSeriesMetadata("http_requests_total", 128)
		hints := &Hints{Include: []string{"pod"}}

		res := BuildMatchers(series, hints)
		require.Empty(t, res)
	})

	t.Run("too many values multiple hints", func(t *testing.T) {
		series := generateSeriesMetadata("http_requests_total", 128)
		hints := &Hints{Include: []string{"pod", "container"}}
		expected := types.Matchers([]types.Matcher{{
			Type:  labels.MatchRegexp,
			Name:  "container",
			Value: "querier|query-frontend|store-gateway",
		}})

		res := BuildMatchers(series, hints)
		require.Equal(t, expected, res)
	})
}

func generateSeriesMetadata(name string, num int) []types.SeriesMetadata {
	var out []types.SeriesMetadata

	for i := range num {
		var container string
		switch i % 3 {
		case 0:
			container = "querier"
		case 1:
			container = "query-frontend"
		case 2:
			container = "store-gateway"
		}

		out = append(out, types.SeriesMetadata{
			Labels: labels.FromMap(map[string]string{
				"__name__":  name,
				"container": container,
				"pod":       fmt.Sprintf("%s-%d", container, i),
				"region":    fmt.Sprintf("prod-test-%d", i%3+1),
			}),
		})
	}

	return out
}

func TestFilterMatchersForVectorMatching(t *testing.T) {
	regionMatcher := types.Matcher{Type: labels.MatchEqual, Name: "region", Value: "us"}
	zoneMatcher := types.Matcher{Type: labels.MatchEqual, Name: "zone", Value: "1"}
	nameMatcher := types.Matcher{Type: labels.MatchEqual, Name: model.MetricNameLabel, Value: "foo"}

	testCases := map[string]struct {
		matchers       types.Matchers
		vectorMatching parser.VectorMatching
		expected       types.Matchers
	}{
		"nil matchers returns nil unchanged": {
			matchers:       nil,
			vectorMatching: parser.VectorMatching{On: true, MatchingLabels: []string{"zone"}},
			expected:       nil,
		},
		"on() with matchers returns nil (no labels match)": {
			matchers:       types.Matchers{regionMatcher, zoneMatcher},
			vectorMatching: parser.VectorMatching{On: true, MatchingLabels: []string{}},
			expected:       nil,
		},
		"on(zone) keeps only zone matchers": {
			matchers:       types.Matchers{regionMatcher, zoneMatcher},
			vectorMatching: parser.VectorMatching{On: true, MatchingLabels: []string{"zone"}},
			expected:       types.Matchers{zoneMatcher},
		},
		"on(zone) with no matching matchers in input returns empty": {
			matchers:       types.Matchers{regionMatcher},
			vectorMatching: parser.VectorMatching{On: true, MatchingLabels: []string{"zone"}},
			expected:       types.Matchers{},
		},
		"without(zone) drops zone and __name__ matchers, keeps others": {
			matchers:       types.Matchers{regionMatcher, zoneMatcher, nameMatcher},
			vectorMatching: parser.VectorMatching{On: false, MatchingLabels: []string{"zone"}},
			expected:       types.Matchers{regionMatcher},
		},
		"without() default matching passes all matchers through unchanged": {
			matchers:       types.Matchers{regionMatcher, zoneMatcher},
			vectorMatching: parser.VectorMatching{On: false, MatchingLabels: []string{}},
			expected:       types.Matchers{regionMatcher, zoneMatcher},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			result := filterMatchersForVectorMatching(tc.matchers, tc.vectorMatching)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestBuildMatchersAdditional(t *testing.T) {
	t.Run("empty metadata returns nil", func(t *testing.T) {
		res := BuildMatchers(nil, &Hints{Include: []string{"container"}})
		require.Nil(t, res)
	})

	t.Run("hint label absent from all series returns no matcher for that label", func(t *testing.T) {
		// Series have no "cluster" label; BuildMatchers should produce no matcher for it.
		series := generateSeriesMetadata("http_requests_total", 3)
		hints := &Hints{Include: []string{"cluster"}}

		res := BuildMatchers(series, hints)
		require.Empty(t, res)
	})

	t.Run("label values with regex special characters are escaped", func(t *testing.T) {
		series := []types.SeriesMetadata{
			{Labels: labels.FromStrings("region", "us-east.1")},
			{Labels: labels.FromStrings("region", "us-west[2]")},
		}
		hints := &Hints{Include: []string{"region"}}
		// regexp.QuoteMeta escapes '.', '[', ']' etc. Note that '-' (hyphen) is NOT
		// escaped because it is only special inside character classes and is safe at
		// the top level of a regex alternation.
		expected := types.Matchers{{
			Type:  labels.MatchRegexp,
			Name:  "region",
			Value: `us-east\.1|us-west\[2\]`,
		}}

		res := BuildMatchers(series, hints)
		require.Equal(t, expected, res)
	})

	t.Run("exactly maxHintMatcherValues unique values produces a matcher", func(t *testing.T) {
		// generateSeriesMetadata gives each series a unique pod value, so 64 series → 64
		// unique pod values, which is exactly the limit. A matcher should still be produced.
		series := generateSeriesMetadata("http_requests_total", maxHintMatcherValues)
		hints := &Hints{Include: []string{"pod"}}

		res := BuildMatchers(series, hints)
		require.Len(t, res, 1, "expected one matcher for pod")
	})

	t.Run("one above maxHintMatcherValues unique values suppresses that label's matcher", func(t *testing.T) {
		series := generateSeriesMetadata("http_requests_total", maxHintMatcherValues+1)
		hints := &Hints{Include: []string{"pod", "container"}}
		// pod overflows (65 unique values), container stays within limit (3 values cycling).
		expected := types.Matchers{{
			Type:  labels.MatchRegexp,
			Name:  "container",
			Value: "querier|query-frontend|store-gateway",
		}}

		res := BuildMatchers(series, hints)
		require.Equal(t, expected, res)
	})
}

func TestBuildMatchersForWithout(t *testing.T) {
	testCases := map[string]struct {
		series         []types.SeriesMetadata
		excludedLabels []string
		expected       types.Matchers
	}{
		"empty series returns nil": {
			series:         []types.SeriesMetadata{},
			excludedLabels: []string{"zone"},
			expected:       nil,
		},
		"nil series returns nil": {
			series:         nil,
			excludedLabels: []string{"zone"},
			expected:       nil,
		},
		"all non-name labels excluded returns nil": {
			series: []types.SeriesMetadata{
				{Labels: labels.FromStrings("__name__", "metric", "zone", "1")},
			},
			excludedLabels: []string{"zone"},
			expected:       nil,
		},
		"__name__ is always excluded even when not in excludedLabels": {
			series: []types.SeriesMetadata{
				{Labels: labels.FromStrings("__name__", "metric", "zone", "1")},
			},
			excludedLabels: []string{},
			// __name__ must not appear in the matcher, only zone.
			expected: types.Matchers{{Type: labels.MatchRegexp, Name: "zone", Value: "1"}},
		},
		"excluded label is dropped, other labels produce matchers": {
			series: []types.SeriesMetadata{
				{Labels: labels.FromStrings("__name__", "metric", "region", "us", "zone", "1")},
			},
			excludedLabels: []string{"zone"},
			expected: types.Matchers{
				{Type: labels.MatchRegexp, Name: "region", Value: "us"},
			},
		},
		"heterogeneous series: union of all non-excluded labels across all series": {
			// Series 1 has zone+region; series 2 has zone+pod. After excluding zone, both
			// region and pod are included even though neither appears in all series.
			series: []types.SeriesMetadata{
				{Labels: labels.FromStrings("__name__", "metric", "region", "us", "zone", "1")},
				{Labels: labels.FromStrings("__name__", "metric", "pod", "a", "zone", "2")},
			},
			excludedLabels: []string{"zone"},
			expected: types.Matchers{
				{Type: labels.MatchRegexp, Name: "pod", Value: "a"},
				{Type: labels.MatchRegexp, Name: "region", Value: "us"},
			},
		},
		"multiple excluded labels": {
			series: []types.SeriesMetadata{
				{Labels: labels.FromStrings("env", "prod", "region", "us", "zone", "1")},
			},
			excludedLabels: []string{"zone", "region"},
			expected: types.Matchers{
				{Type: labels.MatchRegexp, Name: "env", Value: "prod"},
			},
		},
		"excluded label not present in any series is harmless": {
			series: []types.SeriesMetadata{
				{Labels: labels.FromStrings("region", "us", "zone", "1")},
			},
			// "cluster" doesn't appear in any series; excluding it changes nothing.
			excludedLabels: []string{"cluster"},
			expected: types.Matchers{
				{Type: labels.MatchRegexp, Name: "region", Value: "us"},
				{Type: labels.MatchRegexp, Name: "zone", Value: "1"},
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			result := buildMatchersForWithout(tc.series, tc.excludedLabels)
			require.Equal(t, tc.expected, result)
		})
	}
}

func BenchmarkBuildMatchers(b *testing.B) {
	series := generateSeriesMetadata("http_requests_total", 1024)

	b.Run("container", func(b *testing.B) {
		for b.Loop() {
			_ = BuildMatchers(series, &Hints{Include: []string{"container"}})
		}
	})
	b.Run("container,region", func(b *testing.B) {
		for b.Loop() {
			_ = BuildMatchers(series, &Hints{Include: []string{"container", "region"}})
		}
	})
	b.Run("container,region,pod", func(b *testing.B) {
		for b.Loop() {
			_ = BuildMatchers(series, &Hints{Include: []string{"container", "region", "pod"}})
		}
	})
}
