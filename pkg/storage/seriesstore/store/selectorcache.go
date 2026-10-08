// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// What a selector matches in a name's series of a shard is kept for the next time it comes in: rulers and dashboards
// send the same selectors again every minute, and finding the few series that match among thousands by their labels
// was most of what a lookup cost. What is kept is by the version of the name's series, which changes whenever one is
// added or removed, so a lookup either uses the series of the version it finds or looks again. The series are only the
// candidates: they are checked against the matchers when used, so nothing here can make a lookup return a series that
// doesn't match.
const (
	// A name with fewer series in a shard is scanned each time.
	minCachedGroup = 8
	// A selector matching more series in a shard than this isn't kept.
	maxCachedRefs = 20_000
	// How many selectors are kept; at this many, they are all dropped, which the selectors in use fill again.
	maxCachedSelectors = 8192
)

type selectorCache struct {
	mu        sync.RWMutex
	selectors map[string]*cachedSelector
}

// cachedSelector is what one selector matches, by shard.
type cachedSelector struct {
	shards []atomic.Pointer[cachedShard]
}

// cachedShard is immutable: the series of the name's group in a shard that matched the selector, as of the group's
// version.
type cachedShard struct {
	group   uint32
	version uint64
	refs    []seriesRef
}

func (c *selectorCache) get(key string, shards int) *cachedSelector {
	c.mu.RLock()
	selector := c.selectors[key]
	c.mu.RUnlock()
	if selector != nil {
		return selector
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if selector = c.selectors[key]; selector != nil {
		return selector
	}
	if c.selectors == nil || len(c.selectors) >= maxCachedSelectors {
		c.selectors = map[string]*cachedSelector{}
	}
	selector = &cachedSelector{shards: make([]atomic.Pointer[cachedShard], shards)}
	c.selectors[key] = selector
	return selector
}

// selectorKey identifies the matchers of a selector that are not of the query shard: the shards of a query are the
// same selector.
func selectorKey(matchers []compiledMatcher) string {
	parts := make([]string, 0, len(matchers))
	for index := range matchers {
		m := &matchers[index]
		value := m.value
		if m.re != nil {
			value = m.re.pattern
		}
		parts = append(parts, m.name+"\x00"+strconv.Itoa(int(m.kind))+"\x00"+value)
	}
	slices.Sort(parts)
	return strings.Join(parts, "\x01")
}

// cacheableName returns the name a selector has when it is one that is kept, and the matchers besides it: a selector
// with a metric name, which the series of a group already match.
func cacheableName(matchers []compiledMatcher) (name string, rest []compiledMatcher, ok bool) {
	for index := range matchers {
		m := &matchers[index]
		if m.name == metricNameLabel {
			if m.kind != kindEqual || ok {
				return "", nil, false
			}
			name, ok = m.value, true
			continue
		}
		rest = append(rest, *m)
	}
	return name, rest, ok
}

// matchingCached is matching for a selector that is kept: with the series it matched in this shard when the name's
// group hasn't changed since, and the scan that finds them otherwise. queryShard is the query's shard matchers,
// labelMatchers the others, and rest the others but the name.
func (b *seriesByName) matchingCached(selector *cachedSelector, shard int, name string, queryShard, labelMatchers, rest []compiledMatcher, visit func(entry *seriesEntry) bool) {
	groupID, ok := b.names[name]
	if !ok {
		return
	}
	g := &b.groups[groupID]
	inQueryShard := func(entry *seriesEntry) bool {
		for index := range queryShard {
			if entry.series.shardHash%queryShard[index].shardCount != queryShard[index].shardIndex {
				return false
			}
		}
		return true
	}
	if len(g.entries) < minCachedGroup {
		b.matching(labelMatchers, func(entry *seriesEntry) bool { return !inQueryShard(entry) || visit(entry) })
		return
	}
	if cached := selector.shards[shard].Load(); cached != nil && cached.group == groupID && cached.version == g.version {
		b.visitRefs(cached.refs, func(entry *seriesEntry) bool {
			if !inQueryShard(entry) || !matches(entry.labels, rest) {
				return true
			}
			return visit(entry)
		})
		return
	}
	var refs []seriesRef
	b.matching(labelMatchers, func(entry *seriesEntry) bool {
		refs = append(refs, seriesRef{groupID, entry.hash})
		return !inQueryShard(entry) || visit(entry)
	})
	if len(refs) <= maxCachedRefs {
		selector.shards[shard].Store(&cachedShard{group: groupID, version: g.version, refs: sortRefs(refs)})
	}
}
