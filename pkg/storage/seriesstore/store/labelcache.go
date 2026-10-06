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
	// How many of the shard's series, by their ref, were checked for being evicted, and how many were.
	upto    int
	evicted int
	values  map[uint32]*labelValueSet
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
	vs := h.values[id]
	if vs == nil {
		vs = &labelValueSet{set: map[string]struct{}{}}
		if h.values == nil {
			h.values = map[uint32]*labelValueSet{}
		}
		h.values[id] = vs
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
	if h.removals != b.removals {
		h.removals, h.upto, h.evicted, h.values = b.removals, 0, 0, nil
	}
	vs := h.valueSet(b, id)
	if vs == nil {
		return nil, false
	}
	if len(vs.sorted) != len(vs.set) {
		vs.sorted = slices.Sorted(maps.Keys(vs.set))
	}
	return vs.sorted, true
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
