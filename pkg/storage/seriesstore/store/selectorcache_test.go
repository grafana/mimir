// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"context"
	"fmt"
	"slices"
	"testing"

	promlabels "github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
)

func TestSelectorIDIsOfTheMatchersInAnyOrder(t *testing.T) {
	compile := func(matchers ...LabelMatcher) []compiledMatcher {
		compiled, err := compileMatchers(matchers)
		require.NoError(t, err)
		return compiled
	}
	name, job, zone := matcher(0, "__name__", "metric"), matcher(0, "job", "api"), matcher(2, "zone", "eu-.*")
	require.Equal(t, selectorID(compile(name, job, zone)), selectorID(compile(zone, name, job)))
	require.True(t, sameSelector(selectorParts(compile(name, job, zone)), compile(zone, name, job)))
	for _, other := range [][]compiledMatcher{
		compile(name, matcher(1, "job", "api")),
		compile(name, matcher(0, "api", "job")),
		compile(name, job, job),
	} {
		require.NotEqual(t, selectorID(compile(name, job)), selectorID(other), "%v", other)
		require.False(t, sameSelector(selectorParts(compile(name, job)), other), "%v", other)
	}
}

// A selector whose id another kept selector has gets none of what that one matched.
func TestSelectorCacheTellsApartSelectorsOfTheSameID(t *testing.T) {
	compile := func(matchers ...LabelMatcher) []compiledMatcher {
		compiled, err := compileMatchers(matchers)
		require.NoError(t, err)
		return compiled
	}
	var cache selectorCache
	first := compile(matcher(0, "__name__", "metric"), matcher(0, "job", "api"))
	second := compile(matcher(0, "__name__", "metric"), matcher(0, "job", "db"))
	kept := cache.get(42, first, 1)
	kept.keep(0, &cachedShard{group: 1, version: 1})
	require.Same(t, kept, cache.get(42, first, 1))
	other := cache.get(42, second, 1)
	require.NotSame(t, kept, other)
	require.Nil(t, other.shards[0].Load())
	require.Same(t, kept, cache.get(42, first, 1), "the kept one stays")
}

// The shards of a query keep their own series of a selector, which together are the unsharded query's, as they were
// kept and once series are added.
func TestEngineShardsOfASelectorKeepTheirOwnSeries(t *testing.T) {
	ctx := context.Background()
	engine, err := OpenEngine(t.TempDir(), "tenant", EngineOptions{Shards: 4, SecondaryHashFunction: secondaryHash})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	add := func(from, to int) {
		app := engine.Appender(ctx)
		for n := from; n < to; n++ {
			_, err := app.Append(0, promlabels.FromStrings("__name__", "metric", "job", fmt.Sprintf("job-%d", n%5), "pod", fmt.Sprintf("pod-%d", n)), 0, 1000, 1, nil, nil, storage.AppendV2Options{})
			require.NoError(t, err)
		}
		require.NoError(t, app.Commit())
	}
	selectSeries := func(index, count uint64, matchers ...*promlabels.Matcher) []string {
		q, err := engine.ChunkQuerier(0, 2000)
		require.NoError(t, err)
		defer func() { require.NoError(t, q.Close()) }()
		set := q.Select(ctx, true, &storage.SelectHints{Start: 0, End: 2000, ShardIndex: index, ShardCount: count}, matchers...)
		var got []string
		for set.Next() {
			got = append(got, set.At().Labels().String())
		}
		require.NoError(t, set.Err())
		return got
	}
	name := promlabels.MustNewMatcher(promlabels.MatchEqual, "__name__", "metric")
	job := promlabels.MustNewMatcher(promlabels.MatchEqual, "job", "job-3")
	add(0, 8000)
	for round := range 3 {
		if round == 2 {
			add(8000, 9000)
		}
		for _, matchers := range [][]*promlabels.Matcher{{name}, {name, job}} {
			expected := selectSeries(0, 0, matchers...)
			for _, count := range []uint64{4, 16} {
				var union []string
				for index := range count {
					union = append(union, selectSeries(index, count, matchers...)...)
				}
				slices.Sort(union)
				require.Equal(t, expected, union, "round %d, %d shards of %v", round, count, matchers)
			}
		}
	}
}
