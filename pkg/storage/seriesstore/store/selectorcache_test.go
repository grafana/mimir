// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"testing"

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
