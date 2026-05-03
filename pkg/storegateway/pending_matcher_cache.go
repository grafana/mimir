// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"github.com/prometheus/prometheus/model/labels"
)

// pendingMatcherCacheMaxEntries is the per-matcher soft cap on cached
// (label-value, matched-bool) pairs. It exists to bound worst-case memory
// when a matcher is run against a high-cardinality label (e.g. regex against
// __name__ or instance) — once the cap is exceeded for a given matcher, that
// matcher falls back to direct evaluation for the remainder of the iterator's
// lifetime.
//
// The cache is request-scoped (one per filteringSeriesChunkRefsSetIterator,
// itself one per block), so even at the cap each cache instance sums to at
// most cap × (avg-key-len + 8 bytes) ≈ 80 KB. Across many concurrent queries
// in a busy store-gateway this is bounded and dwarfed by the existing
// per-block iterator state.
const pendingMatcherCacheMaxEntries = 1024

// pendingMatcherCache memoizes matcher.Matches(labelValue) results across
// the series stream of a single filteringSeriesChunkRefsSetIterator. Within
// one block iteration, label values recur frequently across series (e.g.
// many series sharing the same `cluster`, `namespace`, `instance`), so a
// cache keyed by label-value string converts an O(N series) regex run into
// an O(V distinct values) one.
//
// Implementation note: a plain map keyed by string is intentionally simpler
// than an LRU. The cache lives only for the duration of one block's
// iteration; we don't need eviction for staleness, only a soft cap to
// protect against pathological cardinality. When the cap is hit for a
// matcher, that matcher's slot is set to nil (sentinel) and subsequent
// lookups fall back to direct evaluation. This is cheaper at allocation
// time than an LRU (no doubly-linked list + map+entry pairs).
type pendingMatcherCache struct {
	// perMatcher maps each cacheable matcher pointer to its
	// label-value→matched table. A nil table means "this matcher
	// previously exceeded the soft cap; stop caching for it" — see match.
	perMatcher map[*labels.Matcher]map[string]bool
}

// newPendingMatcherCache returns a cache primed for the matchers that
// benefit from memoization. Returns nil if no matcher benefits — callers
// must handle the nil receiver via match (which is nil-safe).
func newPendingMatcherCache(matchers []*labels.Matcher) *pendingMatcherCache {
	if len(matchers) == 0 {
		return nil
	}
	c := &pendingMatcherCache{
		perMatcher: make(map[*labels.Matcher]map[string]bool, len(matchers)),
	}
	for _, m := range matchers {
		if !shouldCachePendingMatcher(m) {
			continue
		}
		// Lazy: leave the value nil; the first match() call will allocate
		// the inner map. This keeps zero-call matchers free.
		c.perMatcher[m] = nil
	}
	if len(c.perMatcher) == 0 {
		return nil
	}
	return c
}

// shouldCachePendingMatcher returns true for matchers where memoizing
// (value -> matched) saves work over direct evaluation.
//
// Excluded:
//   - MatchEqual / MatchNotEqual: a string compare is cheaper than a map probe.
//   - MatchRegexp / MatchNotRegexp where SetMatches is non-empty: the labels
//     package's existing fast path is already a hashmap probe of literal
//     alternatives, so caching adds no value.
//
// Included: regex matchers without a literal-set fast path — the ones whose
// evaluation runs the backtracking engine on every call.
func shouldCachePendingMatcher(m *labels.Matcher) bool {
	switch m.Type {
	case labels.MatchEqual, labels.MatchNotEqual:
		return false
	case labels.MatchRegexp, labels.MatchNotRegexp:
		if sm := m.SetMatches(); len(sm) > 0 {
			return false
		}
		return true
	}
	return false
}

// match returns matcher.Matches(value), consulting the cache for matchers
// that shouldCachePendingMatcher selected. Nil-safe: a nil receiver always
// falls through to matcher.Matches.
//
// Bailout: if a matcher's table exceeds pendingMatcherCacheMaxEntries, its
// entry is removed from c.perMatcher; subsequent calls land on
// `registered == false` and fall straight through to direct evaluation,
// freeing the table for GC.
func (c *pendingMatcherCache) match(m *labels.Matcher, value string) bool {
	if c == nil {
		return m.Matches(value)
	}
	tbl, registered := c.perMatcher[m]
	if !registered {
		// Matcher was filtered out by shouldCachePendingMatcher, or this
		// matcher has bailed out of caching due to cap hit.
		return m.Matches(value)
	}
	if tbl != nil {
		if cached, hit := tbl[value]; hit {
			return cached
		}
	}
	matched := m.Matches(value)
	c.store(m, tbl, value, matched)
	return matched
}

// store inserts the (value, matched) pair into the matcher's table,
// allocating the inner map on first use. If the table is at the soft cap,
// the matcher's entry is removed from c.perMatcher (bailout): future
// lookups will see registered=false and call matcher.Matches directly.
func (c *pendingMatcherCache) store(m *labels.Matcher, tbl map[string]bool, value string, matched bool) {
	if tbl == nil {
		tbl = make(map[string]bool, 16)
		c.perMatcher[m] = tbl
	}
	if len(tbl) >= pendingMatcherCacheMaxEntries {
		delete(c.perMatcher, m)
		return
	}
	tbl[value] = matched
}
