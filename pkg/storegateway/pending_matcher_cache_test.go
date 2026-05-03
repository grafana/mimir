// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShouldCachePendingMatcher(t *testing.T) {
	mustMatcher := func(mt labels.MatchType, n, v string) *labels.Matcher {
		m, err := labels.NewMatcher(mt, n, v)
		require.NoError(t, err)
		return m
	}

	tests := map[string]struct {
		matcher *labels.Matcher
		expect  bool
	}{
		"equal is excluded":              {mustMatcher(labels.MatchEqual, "k", "v"), false},
		"not equal is excluded":          {mustMatcher(labels.MatchNotEqual, "k", "v"), false},
		"regex with set fast-path":       {mustMatcher(labels.MatchRegexp, "k", "a|b|c"), false},
		"regex without set fast-path":    {mustMatcher(labels.MatchRegexp, "k", "prod-.*"), true},
		"not regex without set fast-path": {mustMatcher(labels.MatchNotRegexp, "k", "prod-.*"), true},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expect, shouldCachePendingMatcher(tc.matcher))
		})
	}
}

func TestPendingMatcherCache_NilSafe(t *testing.T) {
	// A nil cache must fall through to direct matcher evaluation, so that
	// callers can use a single match() call without checking for nil first.
	var c *pendingMatcherCache
	m, err := labels.NewMatcher(labels.MatchRegexp, "k", "prod-.*")
	require.NoError(t, err)

	assert.True(t, c.match(m, "prod-eu-west-2"))
	assert.False(t, c.match(m, "dev-us-central-1"))
}

func TestNewPendingMatcherCache_OmitsUncacheableMatchers(t *testing.T) {
	mustMatcher := func(mt labels.MatchType, n, v string) *labels.Matcher {
		m, err := labels.NewMatcher(mt, n, v)
		require.NoError(t, err)
		return m
	}

	// All-uncacheable input should produce a nil cache so the iterator's hot
	// path is branch-free.
	allUncacheable := []*labels.Matcher{
		mustMatcher(labels.MatchEqual, "k", "v"),
		mustMatcher(labels.MatchRegexp, "k", "a|b"), // SetMatches fast-path
	}
	assert.Nil(t, newPendingMatcherCache(allUncacheable))

	// Empty matcher list should return nil too.
	assert.Nil(t, newPendingMatcherCache(nil))

	// A mix should produce a cache that holds only the cacheable entries.
	cacheable := mustMatcher(labels.MatchRegexp, "k", "prod-.*")
	mixed := []*labels.Matcher{
		mustMatcher(labels.MatchEqual, "k", "v"),
		cacheable,
		mustMatcher(labels.MatchRegexp, "k", "a|b"),
	}
	c := newPendingMatcherCache(mixed)
	require.NotNil(t, c)
	assert.Len(t, c.perMatcher, 1)
	assert.Contains(t, c.perMatcher, cacheable)
}

func TestPendingMatcherCache_MatchEquivalentToDirect(t *testing.T) {
	// For every matcher type, cache result must match direct evaluation, on
	// both miss (first call) and hit (second call) paths.
	mustMatcher := func(mt labels.MatchType, n, v string) *labels.Matcher {
		m, err := labels.NewMatcher(mt, n, v)
		require.NoError(t, err)
		return m
	}

	matchers := []*labels.Matcher{
		mustMatcher(labels.MatchEqual, "namespace", "prod"),
		mustMatcher(labels.MatchNotEqual, "namespace", "dev"),
		mustMatcher(labels.MatchRegexp, "cluster", "prod-.*"),
		mustMatcher(labels.MatchRegexp, "zone", "a|b|c"),
		mustMatcher(labels.MatchNotRegexp, "instance", "i-[0-9]+-special"),
	}

	values := []string{"prod", "dev", "prod-eu-west-2", "prod-us-central-1", "a", "b", "z", "i-1234-special", "i-5678", ""}

	c := newPendingMatcherCache(matchers)
	for _, m := range matchers {
		for _, v := range values {
			expected := m.Matches(v)
			assert.Equal(t, expected, c.match(m, v), "miss: matcher=%s value=%q", m.String(), v)
			// hit: same call again must still agree
			assert.Equal(t, expected, c.match(m, v), "hit: matcher=%s value=%q", m.String(), v)
		}
	}
}

func TestPendingMatcherCache_MemoizationSavesEvaluation(t *testing.T) {
	// A matcher type that lets us count evaluations: wrap labels.Matcher's
	// MatchString function via a counting matcher. We can't override
	// matcher.Matches since it dispatches on internal state, so we instead
	// verify the LRU stores entries — after V distinct calls, only V entries
	// exist regardless of how many total calls were made.
	mustMatcher := func(mt labels.MatchType, n, v string) *labels.Matcher {
		m, err := labels.NewMatcher(mt, n, v)
		require.NoError(t, err)
		return m
	}

	m := mustMatcher(labels.MatchRegexp, "cluster", "prod-.*")
	c := newPendingMatcherCache([]*labels.Matcher{m})
	require.NotNil(t, c)

	// Repeat the same N values many times; the LRU should hold exactly V
	// entries afterward.
	values := []string{"prod-eu-west-2", "prod-us-central-1", "dev-us-east-0"}
	for repeat := 0; repeat < 1000; repeat++ {
		for _, v := range values {
			c.match(m, v)
		}
	}

	cache, ok := c.perMatcher[m]
	require.True(t, ok)
	assert.Equal(t, len(values), len(cache), "cache should hold exactly V distinct values after 1000 repetitions")
}
