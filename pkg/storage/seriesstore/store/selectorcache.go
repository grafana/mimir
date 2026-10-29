// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"math/bits"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/grafana/mimir/pkg/storage/seriesstore/labels"
)

// What a selector matches in a name's series of a shard is kept for the next time it comes in: rulers and dashboards
// send the same selectors again every minute, and finding the few series that match among thousands by their labels
// was most of what a lookup cost. What is kept is by the version of the name's series, which changes whenever one is
// added or removed, so a lookup either uses the series of the version it finds or looks again: a name selector's series
// are the ones that matched, at their positions in the group, and a general selector's are candidates checked against the
// matchers when used.
const (
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
	selectors map[uint64]*cachedSelector
	// How many series the selectors hold.
	refs atomic.Int64
}

// cachedSelector is what one selector matches, by shard.
type cachedSelector struct {
	owner  *selectorCache
	shards []atomic.Pointer[cachedShard]
	// The selector's matchers, which tell it apart from another of the same id.
	parts []selectorPart
	// For a query shard's selector, the parts of its matchers but the shard's, which the cold blocks' candidates are kept
	// by: made once, rather than for every query that keeps them.
	coldParts atomic.Pointer[[]selectorPart]
}

// unshardedParts returns the parts of matchers, the selector's but its query shard's, made once.
func (c *cachedSelector) unshardedParts(matchers []compiledMatcher) []selectorPart {
	if parts := c.coldParts.Load(); parts != nil {
		return *parts
	}
	parts := selectorParts(matchers)
	c.coldParts.CompareAndSwap(nil, &parts)
	return *c.coldParts.Load()
}

// keep records what a selector matched in a shard, and drops everything the cache holds when it holds too much.
func (c *cachedSelector) keep(shard int, kept *cachedShard) {
	added := int64(len(kept.refs) + len(kept.ids))
	if old := c.shards[shard].Swap(kept); old != nil {
		added -= int64(len(old.refs) + len(old.ids))
	}
	if c.owner.refs.Add(added) > maxCachedTotalRefs {
		c.owner.mu.Lock()
		c.owner.selectors = map[uint64]*cachedSelector{}
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
	// For a name selector of a name the tenant has no series of in the shard: how many names it had, which a new one changes.
	absent bool
	names  int
}

func (c *selectorCache) get(id uint64, matchers []compiledMatcher, shards int) *cachedSelector {
	c.mu.RLock()
	selector := c.selectors[id]
	c.mu.RUnlock()
	if selector != nil && sameSelector(selector.parts, matchers) {
		return selector
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if selector = c.selectors[id]; selector != nil {
		if sameSelector(selector.parts, matchers) {
			return selector
		}
		// Another selector of the same id is kept: this one is looked up afresh each time, which a collision of 64-bit
		// hashes makes all but never happen.
		return &cachedSelector{owner: &selectorCache{}, shards: make([]atomic.Pointer[cachedShard], shards)}
	}
	if c.selectors == nil || len(c.selectors) >= maxCachedSelectors {
		c.selectors = map[uint64]*cachedSelector{}
		c.refs.Store(0)
	}
	selector = &cachedSelector{owner: c, shards: make([]atomic.Pointer[cachedShard], shards), parts: selectorParts(matchers)}
	c.selectors[id] = selector
	return selector
}

// generalSelector is xored into the id of a selector without a metric name equality, which is kept apart from the others.
const generalSelector = 0x9e3779b97f4a7c15

// selectorPart is a matcher as a selector's id is of it: a query shard's by its index and count.
type selectorPart struct {
	kind                   matcherKind
	name, value            string
	shardIndex, shardCount uint64
}

// selectorID identifies the matchers of a selector that are not of the query shard, in any order: the shards of a query
// are the same selector. It is a sum of each matcher's hash, which a query computes without allocating, where joining
// the matchers into a key allocated a few times for every query. Two selectors can share an id: the caches keep the
// matchers of what they keep, and tell them apart with sameSelector.
func selectorID(matchers []compiledMatcher) uint64 {
	var id uint64
	for index := range matchers {
		m := &matchers[index]
		value := m.value
		if m.re != nil {
			value = m.re.pattern
		}
		// Rotated apart, so a name and a value swapped, or the same matcher twice, aren't the same id.
		id += bits.RotateLeft64(labels.ValueHash(m.name), 17) ^ labels.ValueHash(value) ^ (uint64(m.kind)+1)*0xff51afd7ed558ccd ^
			bits.RotateLeft64(m.shardIndex*0x9e3779b97f4a7c15+m.shardCount, 31)
	}
	return id
}

// selectorParts copies what identifies each matcher, which outlives the query whose strings they may be.
func selectorParts(matchers []compiledMatcher) []selectorPart {
	parts := make([]selectorPart, len(matchers))
	for index := range matchers {
		m := &matchers[index]
		value := m.value
		if m.re != nil {
			value = m.re.pattern
		}
		parts[index] = selectorPart{kind: m.kind, name: strings.Clone(m.name), value: strings.Clone(value), shardIndex: m.shardIndex, shardCount: m.shardCount}
	}
	return parts
}

// sameSelector reports whether the matchers are the selector of parts, in any order.
func sameSelector(parts []selectorPart, matchers []compiledMatcher) bool {
	if len(parts) != len(matchers) {
		return false
	}
	for index := range matchers {
		m := &matchers[index]
		value := m.value
		if m.re != nil {
			value = m.re.pattern
		}
		if !slices.Contains(parts, selectorPart{kind: m.kind, name: m.name, value: value, shardIndex: m.shardIndex, shardCount: m.shardCount}) {
			return false
		}
	}
	return true
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
		if entries := b.groups[cached.group].entries; keptAt(cached.refs, entries) {
			// The kept series are the ones that matched, where they were: the group's version is the one they were found
			// at, so none was added or removed since, and labels never change. Checking each against the matchers again
			// was most of what a lookup of a kept selector cost.
			for _, ref := range cached.refs {
				if !visit(&entries[ref.index]) {
					return
				}
			}
			return
		}
	}
	groupID, ok := b.names[name]
	if !ok {
		selector.keep(shard, &cachedShard{owner: b, absent: true, names: len(b.groupNames)})
		return
	}
	g := &b.groups[groupID]
	// A sharded query's selector is kept for its shard, with only the shard's series: each of its shards finds its own
	// series rather than every shard scanning the group again. Past what is kept, the series are no longer collected,
	// and a lookup that stopped early didn't find them all.
	var refs []seriesRef
	tooMany, stopped := false, false
	b.matching(labelMatchers, func(entry *seriesEntry) bool {
		if !inQueryShard(entry) {
			return true
		}
		if !tooMany {
			index := (uintptr(unsafe.Pointer(entry)) - uintptr(unsafe.Pointer(unsafe.SliceData(g.entries)))) / unsafe.Sizeof(seriesEntry{})
			if refs = append(refs, seriesRef{groupID, int32(index), entry.hash}); len(refs) > maxCachedRefs {
				refs, tooMany = nil, true
			}
		}
		if !visit(entry) {
			stopped = true
			return false
		}
		return true
	})
	if !tooMany && !stopped {
		selector.keep(shard, &cachedShard{owner: b, group: groupID, version: g.version, refs: slices.Clone(sortRefs(refs))})
	}
}

// keptAt reports whether each of the refs is at its position in entries, which the group's version not changing since they
// were kept makes so: a lookup that found otherwise looks again rather than return what may be another series.
func keptAt(refs []seriesRef, entries []seriesEntry) bool {
	for _, ref := range refs {
		if int(ref.index) >= len(entries) || entries[ref.index].hash != ref.hash {
			return false
		}
	}
	return true
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
