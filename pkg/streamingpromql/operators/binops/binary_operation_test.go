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
