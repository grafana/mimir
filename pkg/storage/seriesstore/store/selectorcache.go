// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
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
	// A selector without a metric name that took less than this to find its series isn't kept.
	// A selector matching more series in a shard than this isn't kept: 4 bytes each.
	maxGeneralIDs = 65_536
	// How many series that came after what is kept a lookup tries before keeping them too.
	generalTailRefresh = 256
	// A selector matching more series in a shard than this isn't kept: a lookup that returns that many spends its time
	// building the result, and a few of them were most of what the cache held.
	maxCachedRefs = 256
	// How many series, in all the selectors, are kept: 16 bytes each. At this many, they are all dropped.
	maxCachedTotalRefs = 2_000_000
	// How many selectors are kept; at this many, they are all dropped, which the selectors in use fill again.
	maxCachedSelectors = 8192
)

// minGeneralBuild is how long a selector without a metric name takes to find its series before they are kept. A variable
// for tests.
var minGeneralBuild = 100 * time.Microsecond

type selectorCache struct {
	mu        sync.RWMutex
	selectors map[string]*cachedSelector
	// How many series the selectors hold.
	refs atomic.Int64
}

// cachedSelector is what one selector matches, by shard.
type cachedSelector struct {
	owner  *selectorCache
	shards []atomic.Pointer[cachedShard]
}

// keep records what a selector matched in a shard, and drops everything the cache holds when it holds too much.
func (c *cachedSelector) keep(shard int, kept *cachedShard) {
	added := int64(len(kept.refs) + len(kept.ids))
	if old := c.shards[shard].Swap(kept); old != nil {
		added -= int64(len(old.refs) + len(old.ids))
	}
	if c.owner.refs.Add(added) > maxCachedTotalRefs {
		c.owner.mu.Lock()
		c.owner.selectors = map[string]*cachedSelector{}
		c.owner.refs.Store(0)
		c.owner.mu.Unlock()
	}
}

// cachedShard is immutable: the series of the name's group in a shard that matched the selector, as of the group's
// version.
type cachedShard struct {
	group   uint32
	version uint64
	refs    []seriesRef
	// For a selector without a metric name: the ids of the matching series among those the shard had by upto, of the
	// postings that were built by epoch of that series-by-name.
	owner *seriesByName
	epoch uint64
	upto  uint32
	ids   []uint32
	// For a name selector whose group had few series: only the group, which is scanned.
	small bool
	// For a name selector of a name the tenant has no series of in the shard: how many names it had, which a new one changes.
	absent bool
	names  int
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
		c.refs.Store(0)
	}
	selector = &cachedSelector{owner: c, shards: make([]atomic.Pointer[cachedShard], shards)}
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
	inQueryShard := func(entry *seriesEntry) bool {
		for index := range queryShard {
			if entry.series.shardHash%queryShard[index].shardCount != queryShard[index].shardIndex {
				return false
			}
		}
		return true
	}
	// What was kept for this selector names its group, so a lookup that finds it unchanged doesn't look the name up.
	cached := selector.shards[shard].Load()
	if cached != nil && cached.owner == b && cached.absent && cached.names == len(b.groupNames) {
		return
	}
	if cached != nil && cached.owner == b && !cached.absent && int(cached.group) < len(b.groups) && cached.version == b.groups[cached.group].version {
		g := &b.groups[cached.group]
		switch {
		case cached.small && len(g.entries) < minCachedGroup:
			// A group this small is scanned.
			for index := range g.entries {
				entry := &g.entries[index]
				if inQueryShard(entry) && matches(entry.labels, rest) && !visit(entry) {
					return
				}
			}
			return
		case !cached.small && len(g.entries) >= minCachedGroup:
			b.visitRefs(cached.refs, func(entry *seriesEntry) bool {
				if !inQueryShard(entry) || !matches(entry.labels, rest) {
					return true
				}
				return visit(entry)
			})
			return
		}
	}
	groupID, ok := b.names[name]
	if !ok {
		selector.keep(shard, &cachedShard{owner: b, absent: true, names: len(b.groupNames)})
		return
	}
	g := &b.groups[groupID]
	if len(g.entries) < minCachedGroup {
		selector.keep(shard, &cachedShard{owner: b, group: groupID, version: g.version, small: true})
		b.matching(labelMatchers, func(entry *seriesEntry) bool { return !inQueryShard(entry) || visit(entry) })
		return
	}
	if cached != nil && !cached.absent && !cached.small && cached.group == groupID && cached.version == g.version {
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
		refs = append(refs, b.refs[entry.id])
		return !inQueryShard(entry) || visit(entry)
	})
	if len(refs) <= maxCachedRefs {
		selector.keep(shard, &cachedShard{owner: b, group: groupID, version: g.version, refs: slices.Clone(sortRefs(refs))})
	}
}

// matchingGeneral is matching for a selector without a metric name that comes in again, which it answers from the ids of
// the series it matched, and only looks at the series that came after: a series' labels never change, ids are never reused,
// and a removed series' ref is marked, so what was found stays right. queryShard is the query's shard matchers and labelMatchers the others.
func (b *seriesByName) matchingGeneral(selector *cachedSelector, shard int, queryShard, labelMatchers []compiledMatcher, visit func(entry *seriesEntry) bool) {
	inShard := func(entry *seriesEntry) bool {
		for index := range queryShard {
			if entry.series.shardHash%queryShard[index].shardCount != queryShard[index].shardIndex {
				return false
			}
		}
		return true
	}
	cached := selector.shards[shard].Load()
	if cached == nil || cached.owner != b || cached.epoch != b.idEpoch || cached.ids == nil {
		start := time.Now()
		var ids []uint32
		stopped := false
		b.matching(labelMatchers, func(entry *seriesEntry) bool {
			ids = append(ids, entry.id)
			if !inShard(entry) {
				return true
			}
			if !visit(entry) {
				stopped = true
				return false
			}
			return true
		})
		if !stopped && len(ids) <= maxGeneralIDs && time.Since(start) >= minGeneralBuild {
			slices.Sort(ids)
			if ids == nil {
				ids = []uint32{}
			}
			selector.keep(shard, &cachedShard{owner: b, epoch: b.idEpoch, upto: uint32(len(b.refs)), ids: ids})
		}
		return
	}
	for _, id := range cached.ids {
		if entry := b.entryOf(id); entry != nil && inShard(entry) {
			if !visit(entry) {
				return
			}
		}
	}
	// The series that came after what was kept are checked against the matchers.
	var added []uint32
	for id := cached.upto; id < uint32(len(b.refs)); id++ {
		entry := b.entryOf(id)
		if entry == nil || !matches(entry.labels, labelMatchers) {
			continue
		}
		added = append(added, id)
		if inShard(entry) && !visit(entry) {
			return
		}
	}
	// A short kept list is copied cheaply, so it is refreshed sooner than a long one: the series that came after it are
	// checked against the matchers by every lookup until then.
	if int(uint32(len(b.refs))-cached.upto) >= min(generalTailRefresh, max(16, len(cached.ids)/32)) && len(cached.ids)+len(added) <= maxGeneralIDs {
		ids := make([]uint32, 0, len(cached.ids)+len(added))
		ids = append(append(ids, cached.ids...), added...)
		selector.keep(shard, &cachedShard{owner: b, epoch: b.idEpoch, upto: uint32(len(b.refs)), ids: ids})
	}
}

// entryOf returns the live series with the id, or nil for a removed one.
func (b *seriesByName) entryOf(id uint32) *seriesEntry {
	if int(id) >= len(b.refs) {
		return nil
	}
	ref := b.refs[id]
	if ref.index < 0 {
		return nil
	}
	entries := b.groups[ref.group].entries
	if int(ref.index) >= len(entries) || entries[ref.index].id != id {
		return nil
	}
	return &entries[ref.index]
}
