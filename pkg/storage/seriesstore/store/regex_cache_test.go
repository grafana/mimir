// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/DmitriyVTitov/size"
	prometheuslabels "github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

// emptyRegexCache empties the compiled regexes kept, so a test sees only its own, and sets the bounds on them back after
// it.
func emptyRegexCache(t *testing.T) {
	regexCache.Lock()
	clear(regexCache.byPattern)
	regexCache.bytes = 0
	regexCache.Unlock()
	t.Cleanup(func(total, each int) func() {
		return func() { maxCachedRegexesBytes, maxCachedRegexBytes = total, each }
	}(maxCachedRegexesBytes, maxCachedRegexBytes))
}

// targetsAlternation is a pattern as info() selects target_info's series with: an alternation of every target's
// instance, quoted.
func targetsAlternation(prefix string, targets int) string {
	instances := make([]string, targets)
	for n := range instances {
		instances[n] = regexp.QuoteMeta(fmt.Sprintf("%s10.0.%d.%d:9100", prefix, n/256, n%256))
	}
	return strings.Join(instances, "|")
}

// A kept pattern's compiled regex is shared by the query shards of a query and by the next queries, and with it its
// verdicts. One too big to keep, like info()'s alternation of many targets, is compiled for each query instead, and keeps
// no verdicts of its own, which the list of slow regexes would hold it by.
func TestAnchoredRegexKeepsTheRegexesSmallEnoughToShare(t *testing.T) {
	emptyRegexCache(t)
	defer func(ns int64) { slowRegexNs = ns }(slowRegexNs)
	// Every regex that keeps verdicts proves slow, from its first samples.
	slowRegexNs = 0
	checkValues := func(re *compiledRegex) {
		for n := range regexMemoSamples + 1 {
			re.memoMatch(fmt.Sprintf("value-%d", n))
		}
	}

	small := "(?i).*vault.*"
	kept, err := anchoredRegex(small)
	require.NoError(t, err)
	again, err := anchoredRegex(small)
	require.NoError(t, err)
	require.Same(t, kept, again)
	checkValues(kept)
	require.Equal(t, regexMemoSlow, kept.verdicts.state.Load(), "a kept regex keeps the verdicts of a slow one")

	large := targetsAlternation("", 300)
	maxCachedRegexBytes = 4 * regexCache.bytes
	matcher, err := prometheuslabels.NewFastRegexMatcher(large)
	require.NoError(t, err)
	require.Greater(t, size.Of(matcher), maxCachedRegexBytes, "the precondition: the alternation is over the bound")
	keptBytes := regexCache.bytes
	first, err := anchoredRegex(large)
	require.NoError(t, err)
	second, err := anchoredRegex(large)
	require.NoError(t, err)
	require.NotSame(t, first, second)
	require.Equal(t, keptBytes, regexCache.bytes)
	checkValues(first)
	require.Equal(t, regexMemoFast, first.verdicts.state.Load())
	require.Nil(t, first.verdicts.table.Load())
	require.True(t, first.isMatch("10.0.1.4:9100"), "it still matches what it accepts")
}

// Patterns that each fit, but don't all together, are kept up to the bound on their bytes, past which what is kept is
// dropped for the next ones: queries with ever new patterns can't grow the cache.
func TestAnchoredRegexKeepsAtMostItsBytesOfRegexes(t *testing.T) {
	emptyRegexCache(t)
	// Alike in size: each takes the first one's bytes, or close.
	patternOf := func(n int) string { return targetsAlternation(fmt.Sprintf("p%02d-", n), 20) }
	_, err := anchoredRegex(patternOf(0))
	require.NoError(t, err)
	maxCachedRegexesBytes = 3 * regexCache.bytes
	dropped := false
	for n := 1; n < 10; n++ {
		re, err := anchoredRegex(patternOf(n))
		require.NoError(t, err)
		require.LessOrEqual(t, regexCache.bytes, maxCachedRegexesBytes)
		again, err := anchoredRegex(patternOf(n))
		require.NoError(t, err)
		require.Same(t, re, again, "the newest is kept")
		dropped = dropped || len(regexCache.byPattern) == 1
	}
	require.True(t, dropped, "the bound was reached")
}
