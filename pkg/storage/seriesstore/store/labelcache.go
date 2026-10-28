// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"maps"
	"slices"
	"sync"

	"github.com/grafana/mimir/pkg/storage/seriesstore/labels"
)

// maxCachedValues is how many distinct values of one label a shard caches: a label with more is
// looked up by scanning the series.
const maxCachedValues = 1 << 20

// headLabels answers label names and values lookups over every series of a shard without a
// matcher, which would otherwise visit each series. A series that left the head is not part of
// the answer, so it only answers while no series of the shard is evicted from it.
type headLabels struct {
	mu       sync.Mutex
	removals uint64
	epoch    uint64
	// The postings' ids the dictionaries were taken by: a rebuild of the postings, once many series were removed,
	// numbers the series anew, and the dictionaries start over.
	dictionaryEpoch uint64
	// How many of the shard's series, by their ref, were checked for being evicted, and how many were.
	upto    int
	evicted int
	values  map[uint32]*labelValueSet
	// The values of each label in the shard, with those of series removed since, which only select none: apart from
	// values, which a removal clears, so that idle eviction doesn't have every lookup visit the shard's series again.
	dictionaries map[uint32]*labelValueSet
}

type labelValueSet struct {
	upto     int
	set      map[string]struct{}
	overflow bool
	// The set's values in order, once a lookup needed them so.
	sorted []string
}

// refresh brings the cache up to date with the shard's series, and reports whether none is
// evicted. Callers hold the shard's lock, so its series don't change meanwhile.
func (h *headLabels) refresh(b *seriesByName, epoch uint64) bool {
	if h.removals != b.removals || h.epoch != epoch {
		h.removals, h.epoch, h.upto, h.evicted, h.values = b.removals, epoch, 0, 0, nil
	}
	for ; h.upto < len(b.refs); h.upto++ {
		b.visitRefs(b.refs[h.upto:h.upto+1], func(entry *seriesEntry) bool {
			if entry.series.headEvicted {
				h.evicted++
			}
			return true
		})
	}
	return h.evicted == 0
}

// headLabelNames returns the label names of the shard's series, unsorted, when it can without
// visiting them.
func (b *seriesByName) headLabelNames(epoch uint64) ([]string, bool) {
	b.headLabels.mu.Lock()
	defer b.headLabels.mu.Unlock()
	if !b.headLabels.refresh(b, epoch) {
		return nil, false
	}
	var names []string
	if b.len > 0 {
		names = append(names, metricNameLabel)
	}
	for id, count := range b.labelSeries {
		if count > 0 {
			names = append(names, labels.Name(uint32(id)))
		}
	}
	return names, true
}

// valueSet brings the values of the label in the cache up to date with the shard's series, and
// returns them, or nil when there are too many to keep. Callers hold the shard's lock and the
// cache's.
func (h *headLabels) valueSet(b *seriesByName, id uint32) *labelValueSet {
	return valueSetIn(&h.values, b, id)
}

// valueSetIn is valueSet over the sets, the head's values or its dictionaries.
func valueSetIn(sets *map[uint32]*labelValueSet, b *seriesByName, id uint32) *labelValueSet {
	vs := (*sets)[id]
	if vs == nil {
		vs = &labelValueSet{set: map[string]struct{}{}}
		if *sets == nil {
			*sets = map[uint32]*labelValueSet{}
		}
		(*sets)[id] = vs
	}
	if vs.overflow {
		return nil
	}
	// Only the series added since the last lookup.
	for ; vs.upto < len(b.refs); vs.upto++ {
		b.visitRefs(b.refs[vs.upto:vs.upto+1], func(entry *seriesEntry) bool {
			if value, ok := labelValue(entry.labels, id); ok {
				if _, seen := vs.set[value]; !seen {
					vs.set[detach(value)] = struct{}{}
				}
			}
			return true
		})
		if len(vs.set) > maxCachedValues {
			vs.overflow, vs.set, vs.sorted = true, nil, nil
			return nil
		}
	}
	return vs
}

// headLabelValues returns the values of the label of the shard's series, as a set that callers
// must not change, when it can without visiting every series each time.
func (b *seriesByName) headLabelValues(id uint32, epoch uint64) (map[string]struct{}, bool) {
	h := &b.headLabels
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.refresh(b, epoch) {
		return nil, false
	}
	vs := h.valueSet(b, id)
	if vs == nil {
		return nil, false
	}
	return vs.set, true
}

// labelDictionary returns the distinct values of the label in the shard, sorted, which callers
// must not change, or false when there are too many to keep. Series that left the shard may
// still be among them: a value only selects the series that have it.
func (b *seriesByName) labelDictionary(id uint32) ([]string, bool) {
	h := &b.headLabels
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.dictionaryEpoch != b.idEpoch {
		h.dictionaryEpoch, h.dictionaries = b.idEpoch, nil
	}
	if h.dictionaries[id] == nil {
		b.startDictionary(id)
	}
	vs := valueSetIn(&h.dictionaries, b, id)
	if vs != nil && len(vs.set) > 2*b.seriesWith(id, true)+1024 {
		// Mostly the values of removed series, of a label of many: started over from the series there are.
		b.startDictionary(id)
		vs = valueSetIn(&h.dictionaries, b, id)
	}
	if vs == nil {
		return nil, false
	}
	if len(vs.sorted) != len(vs.set) {
		vs.sorted = slices.Sorted(maps.Keys(vs.set))
	}
	return vs.sorted, true
}

// startDictionary starts the label's dictionary from its postings: the value of a series of each, rather than of every
// series of the shard, of which most share their value with many others. Callers hold the cache's lock.
func (b *seriesByName) startDictionary(id uint32) {
	h := &b.headLabels
	if h.dictionaries == nil {
		h.dictionaries = map[uint32]*labelValueSet{}
	}
	var postings map[uint64]uint32
	if int(id) < len(b.postings) {
		postings = b.postings[id]
	}
	if len(postings) > maxCachedValues {
		h.dictionaries[id] = &labelValueSet{overflow: true}
		return
	}
	// The postings have every series of the refs: the series added after them are the cache's to add.
	vs := &labelValueSet{set: make(map[string]struct{}, len(postings)), upto: len(b.refs)}
	add := func(seriesID uint32) bool {
		entry := b.entryOf(seriesID)
		if entry == nil {
			return false
		}
		if value, ok := labelValue(entry.labels, id); ok {
			vs.set[detach(value)] = struct{}{}
		}
		return true
	}
	for _, posting := range postings {
		one, many := b.posting(posting)
		if many == nil {
			add(one)
			continue
		}
		// The first series still there: the others were removed since the postings were built.
		for _, seriesID := range many {
			if add(seriesID) {
				break
			}
		}
	}
	h.dictionaries[id] = vs
}

// headMetricNames returns the metric names of the shard's series.
func (b *seriesByName) headMetricNames() []string {
	var names []string
	for groupID := range b.groups {
		if len(b.groups[groupID].entries) > 0 {
			names = append(names, b.groupNames[groupID])
		}
	}
	return names
}

// maxValuesByPostings is how many values a label can have for labelValuesByPostings to look each up: dashboard
// variables' labels have few, under matchers that select most of a tenant's series, or none.
const maxValuesByPostings = 1024

// labelValuesByPostings adds to values those of the label that a series the matchers select and include accepts has,
// finding one such series in each value's postings rather than visiting every series the matchers select, as
// Prometheus's head does. It reports false, having added none, when the label has too many values, or when the series
// it checks that don't match outnumber what visiting the selected series would likely take: then the matchers are
// narrow, and the caller visits them. Callers hold the shard's lock.
//
// pause, called for each posting the lookup without matchers reads, stops it when it returns false.
func (b *seriesByName) labelValuesByPostings(id uint32, matchers []compiledMatcher, include func(*seriesEntry) bool, pause pauser, values map[string]struct{}) bool {
	// The metric name has no postings: its series are a group, which without matchers has its name once one of its
	// series is included. Without matchers, this answers once a series of the shard left the head, when the head's
	// cached values don't.
	if labels.Name(id) == metricNameLabel {
		if len(matchers) > 0 {
			return false
		}
		b.metricNamesIncluded(include, pause, values)
		return true
	}
	// Label names are numbered for the process, as they first appear: one past the shard's postings is on none of its
	// series, which a lookup of every label of a tenant asks each shard for.
	if int(id) >= len(b.postings) {
		return true
	}
	if len(matchers) == 0 {
		b.labelValuesOfPostings(id, include, pause, values)
		return true
	}
	dictionary, ok := b.labelDictionary(id)
	if !ok || len(dictionary) > maxValuesByPostings {
		return false
	}
	// Each value's first series matches when the matchers are wide; narrow ones spend this and fall back.
	budget := 64 + 4*len(dictionary)
	// Visiting the series an equality selects costs less than the budget: the matchers are narrow.
	if b.selectedAtMost(matchers) <= budget {
		return false
	}
	postings := b.postings[id]
	found := make([]string, 0, len(dictionary))
	for _, value := range dictionary {
		posting, ok := postings[postingKey(value)]
		if !ok {
			continue
		}
		one, many := b.posting(posting)
		if many == nil {
			many = []uint32{one}
		}
		for _, seriesID := range many {
			entry := b.entryOf(seriesID)
			if entry == nil {
				continue
			}
			// The postings are by the value's hash: another value can share it.
			if got, ok := labelValue(entry.labels, id); !ok || got != value {
				continue
			}
			if matches(entry.labels, matchers) && include(entry) {
				found = append(found, value)
				break
			}
			if budget--; budget < 0 {
				return false
			}
		}
	}
	for _, value := range found {
		values[value] = struct{}{}
	}
	return true
}

// labelValuesOfPostings adds the values of the label of a series that include accepts, from each posting's first such
// series, as Prometheus's head lists a label's values from its index: the postings are by a 64-bit hash of the value,
// which two values of a label share too rarely to matter, so the lookup reads one series per value however many the
// label has, rather than every series of the shard. Callers hold the shard's lock.
func (b *seriesByName) labelValuesOfPostings(id uint32, include func(*seriesEntry) bool, pause pauser, values map[string]struct{}) {
	add := func(seriesID uint32) bool {
		entry := b.entryOf(seriesID)
		if entry == nil || !include(entry) {
			return false
		}
		if value, ok := labelValue(entry.labels, id); ok {
			values[value] = struct{}{}
		}
		return true
	}
	for _, posting := range b.postings[id] {
		if pause != nil && !pause.pause() {
			return
		}
		one, many := b.posting(posting)
		if many == nil {
			add(one)
			continue
		}
		for _, seriesID := range many {
			if add(seriesID) {
				break
			}
		}
	}
}

// metricNamesIncluded adds the names of the name groups that have a series include accepts: the first of a group that it
// does, which is most often its first, has its name, rather than every series of the shard being looked up for it.
// Callers hold the shard's lock.
func (b *seriesByName) metricNamesIncluded(include func(*seriesEntry) bool, pause pauser, values map[string]struct{}) {
	for groupID := range b.groups {
		name := b.groupNames[groupID]
		if _, seen := values[name]; seen {
			continue
		}
		for index := 0; index < len(b.groups[groupID].entries); index++ {
			if pause != nil && !pause.pause() {
				return
			}
			if include(&b.groups[groupID].entries[index]) {
				values[name] = struct{}{}
				break
			}
		}
	}
}

// selectedAtMost returns how many series the matchers' equalities select at most, by their postings and the metric's
// series, without visiting them: the number of series of the shard when no equality narrows them.
func (b *seriesByName) selectedAtMost(matchers []compiledMatcher) int {
	most := b.len
	for index := range matchers {
		matcher := &matchers[index]
		if matcher.kind != kindEqual || matcher.value == "" {
			continue
		}
		if matcher.name == metricNameLabel {
			group, ok := b.names[matcher.value]
			if !ok {
				return 0
			}
			most = min(most, len(b.groups[group].entries))
			continue
		}
		name, known := labels.Lookup(matcher.name)
		if !known || int(name) >= len(b.postings) {
			return 0
		}
		posting, ok := b.postings[name][postingKey(matcher.value)]
		if !ok {
			return 0
		}
		most = min(most, b.postingLen(posting))
	}
	return most
}
