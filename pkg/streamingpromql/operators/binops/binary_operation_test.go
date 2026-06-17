// SPDX-License-Identifier: AGPL-3.0-only

package binops

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/labels"
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

		res := BuildMatchers(context.Background(), log.NewNopLogger(), series, hints)
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

		res := BuildMatchers(context.Background(), log.NewNopLogger(), series, hints)
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

		res := BuildMatchers(context.Background(), log.NewNopLogger(), series, hints)
		require.Equal(t, expected, res)
	})

	t.Run("too many values single hint", func(t *testing.T) {
		series := generateSeriesMetadata("http_requests_total", 128)
		hints := &Hints{Include: []string{"pod"}}

		res := BuildMatchers(context.Background(), log.NewNopLogger(), series, hints)
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

		res := BuildMatchers(context.Background(), log.NewNopLogger(), series, hints)
		require.Equal(t, expected, res)
	})

	t.Run("nil hints: returns nil without generating matchers", func(t *testing.T) {
		series := generateSeriesMetadata("http_requests_total", 3)
		res := BuildMatchers(context.Background(), log.NewNopLogger(), series, nil)
		require.Nil(t, res)
	})

	t.Run("without matching: generates matchers for all non-excluded labels present on all series", func(t *testing.T) {
		// generateSeriesMetadata produces series with __name__, container, pod, region.
		// __name__ is always skipped; container, pod and region each appear on all 3 series.
		series := generateSeriesMetadata("http_requests_total", 3)
		hints := &Hints{}
		expected := types.Matchers([]types.Matcher{
			{Type: labels.MatchRegexp, Name: "container", Value: "querier|query-frontend|store-gateway"},
			{Type: labels.MatchRegexp, Name: "pod", Value: "querier-0|query-frontend-1|store-gateway-2"},
			{Type: labels.MatchRegexp, Name: "region", Value: "prod-test-1|prod-test-2|prod-test-3"},
		})

		res := BuildMatchers(context.Background(), log.NewNopLogger(), series, hints)
		require.Equal(t, expected, res)
	})

	t.Run("without matching with excluded label: excluded label does not appear in matchers", func(t *testing.T) {
		series := generateSeriesMetadata("http_requests_total", 3)
		hints := &Hints{Exclude: []string{"pod", "region"}}
		expected := types.Matchers([]types.Matcher{
			{Type: labels.MatchRegexp, Name: "container", Value: "querier|query-frontend|store-gateway"},
		})

		res := BuildMatchers(context.Background(), log.NewNopLogger(), series, hints)
		require.Equal(t, expected, res)
	})

	t.Run("without matching with empty series: returns nil", func(t *testing.T) {
		hints := &Hints{}
		res := BuildMatchers(context.Background(), log.NewNopLogger(), nil, hints)
		require.Nil(t, res)
	})

	t.Run("without matching with too many values: label with too many unique values is skipped", func(t *testing.T) {
		// generateSeriesMetadata with 128 series produces 128 unique pod values (one per series),
		// exceeding maxHintMatcherValues (64). Pod should be skipped but container (3 unique) and
		// region (3 unique) should still produce matchers.
		series := generateSeriesMetadata("http_requests_total", 128)
		hints := &Hints{} // exclude-matching mode with no exclusions
		res := BuildMatchers(context.Background(), log.NewNopLogger(), series, hints)

		// pod has 128 unique values → skipped; container and region have ≤64 values → included.
		require.Len(t, res, 2)
		require.Equal(t, "container", res[0].Name)
		require.Equal(t, "region", res[1].Name)
	})

	t.Run("without matching with heterogeneous labels: absent label matched with empty string", func(t *testing.T) {
		series := []types.SeriesMetadata{
			{Labels: labels.FromStrings("env", "prod", "region", "us-east")},
			{Labels: labels.FromStrings("env", "prod")}, // no region label
		}
		hints := &Hints{} // exclude-matching, no exclusions
		expected := types.Matchers{
			{Type: labels.MatchRegexp, Name: "env", Value: "prod"},
			{Type: labels.MatchRegexp, Name: "region", Value: "|us-east"},
		}

		res := BuildMatchers(context.Background(), log.NewNopLogger(), series, hints)
		// region is absent from one series, so the matcher includes the empty string
		// to also match RHS series without a region label.
		require.Equal(t, expected, res)
	})

	t.Run("without matching excludes all labels: returns nil", func(t *testing.T) {
		series := generateSeriesMetadata("http_requests_total", 3)
		// Exclude every non-__name__ label that generateSeriesMetadata produces.
		hints := &Hints{Exclude: []string{"container", "pod", "region"}}
		res := BuildMatchers(context.Background(), log.NewNopLogger(), series, hints)
		require.Nil(t, res)
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

func BenchmarkBuildMatchers(b *testing.B) {
	series := generateSeriesMetadata("http_requests_total", 1024)
	ctx := context.Background()
	logger := log.NewNopLogger()

	b.Run("container", func(b *testing.B) {
		for b.Loop() {
			_ = BuildMatchers(ctx, logger, series, &Hints{Include: []string{"container"}})
		}
	})
	b.Run("container,region", func(b *testing.B) {
		for b.Loop() {
			_ = BuildMatchers(ctx, logger, series, &Hints{Include: []string{"container", "region"}})
		}
	})
	b.Run("container,region,pod", func(b *testing.B) {
		for b.Loop() {
			_ = BuildMatchers(ctx, logger, series, &Hints{Include: []string{"container", "region", "pod"}})
		}
	})
}
