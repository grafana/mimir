// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"context"
	"fmt"
	"slices"
	"strings"
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

// A selector whose matchers are longer than a kept one's may be, like info()'s alternation of every target it joins, is
// looked up afresh each time, and takes none of the cache's bytes.
func TestSelectorCacheDoesNotKeepSelectorsOverItsBound(t *testing.T) {
	compile := func(matchers ...LabelMatcher) []compiledMatcher {
		compiled, err := compileMatchers(matchers)
		require.NoError(t, err)
		return compiled
	}
	var cache selectorCache
	small := compile(matcher(0, "__name__", "target_info"), matcher(0, "instance", "a"))
	kept := cache.get(1, small, 1)
	require.Same(t, kept, cache.get(1, small, 1))
	keptBytes := cache.bytes.Load()
	require.Positive(t, keptBytes)

	large := compile(matcher(0, "__name__", "target_info"), matcher(0, "instance", strings.Repeat("a", maxCachedSelectorBytes)))
	require.NotSame(t, cache.get(2, large, 1), cache.get(2, large, 1))
	require.Equal(t, keptBytes, cache.bytes.Load())
	require.Len(t, cache.selectors, 1)
}

// Selectors that each fit, but don't all together, are kept up to the bound on their bytes, past which what is kept is
// dropped for the next ones.
func TestSelectorCacheKeepsAtMostItsBytesOfSelectors(t *testing.T) {
	defer func(bytes int) { maxCachedSelectorsBytes = bytes }(maxCachedSelectorsBytes)
	compile := func(matchers ...LabelMatcher) []compiledMatcher {
		compiled, err := compileMatchers(matchers)
		require.NoError(t, err)
		return compiled
	}
	// Alike in size: each takes the first one's bytes.
	selectorOf := func(n int) []compiledMatcher {
		return compile(matcher(0, "__name__", "metric"), matcher(0, "job", fmt.Sprintf("job-%02d", n)))
	}
	var cache selectorCache
	cache.get(0, selectorOf(0), 1)
	maxCachedSelectorsBytes = int(3 * cache.bytes.Load())
	dropped := false
	for n := 1; n < 10; n++ {
		kept := cache.get(uint64(n), selectorOf(n), 1)
		require.LessOrEqual(t, cache.bytes.Load(), int64(maxCachedSelectorsBytes))
		require.Same(t, kept, cache.get(uint64(n), selectorOf(n), 1), "the newest is kept")
		dropped = dropped || len(cache.selectors) == 1
	}
	require.True(t, dropped, "the bound was reached")
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
