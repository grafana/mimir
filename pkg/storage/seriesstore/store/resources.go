// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"cmp"
	"context"
	"encoding/binary"
	"math"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/RoaringBitmap/roaring/roaring64"
	promlabels "github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/seriesstore/chunks"
	"github.com/grafana/mimir/pkg/storage/seriesstore/labels"
)

// The engine's native resources: the OTel resource attributes of each series, as versions over time, in two levels.
// A series has runs of an identity, a resource's identifying attributes, and an identity has versions of its
// description, the resource's other attributes. Identities and descriptions are values of dictionaries, by id: a
// series' runs hold references to their identities, and an identity's versions to their descriptions.
//
// Without EntityRefs, the identifying attributes are service.name, service.namespace and service.instance.id, which
// job and instance derive from, as #17740 splits them.

func isIdentifyingResourceAttribute(key string) bool {
	return key == "service.name" || key == "service.namespace" || key == "service.instance.id"
}

// encodeResourceAttributes appends to into the canonical encoding of the attributes that are identifying, or not:
// sorted by key, each its key, type and value. It's the dictionaries' key, which compares exactly, and what their
// values decode from. order is scratch.
func encodeResourceAttributes(into []byte, attributes []mimirpb.ResourceAttribute, identifying bool, order []int) ([]byte, []int) {
	order = order[:0]
	for i := range attributes {
		if isIdentifyingResourceAttribute(attributes[i].Key) == identifying {
			order = append(order, i)
		}
	}
	slices.SortStableFunc(order, func(a, b int) int { return strings.Compare(attributes[a].Key, attributes[b].Key) })
	for _, i := range order {
		a := &attributes[i]
		into = binary.AppendUvarint(into, uint64(len(a.Key)))
		into = append(into, a.Key...)
		into = append(into, byte(a.Type))
		switch a.Type {
		case mimirpb.RESOURCE_ATTRIBUTE_BOOL:
			if a.BoolValue {
				into = append(into, 1)
			} else {
				into = append(into, 0)
			}
		case mimirpb.RESOURCE_ATTRIBUTE_INT:
			into = binary.AppendVarint(into, a.IntValue)
		case mimirpb.RESOURCE_ATTRIBUTE_DOUBLE:
			into = binary.LittleEndian.AppendUint64(into, math.Float64bits(a.DoubleValue))
		case mimirpb.RESOURCE_ATTRIBUTE_BYTES:
			into = binary.AppendUvarint(into, uint64(len(a.BytesValue)))
			into = append(into, a.BytesValue...)
		default:
			into = binary.AppendUvarint(into, uint64(len(a.StringValue)))
			into = append(into, a.StringValue...)
		}
	}
	return into, order
}

// decodeResourceAttributes appends to into the attributes of a canonical encoding, whose strings refer to it.
func decodeResourceAttributes(encoded string, into []mimirpb.ResourceAttribute) []mimirpb.ResourceAttribute {
	for len(encoded) > 0 {
		var a mimirpb.ResourceAttribute
		var n int
		a.Key, encoded = readEncodedString(encoded)
		a.Type, encoded = mimirpb.ResourceAttribute_Type(encoded[0]), encoded[1:]
		switch a.Type {
		case mimirpb.RESOURCE_ATTRIBUTE_BOOL:
			a.BoolValue, encoded = encoded[0] == 1, encoded[1:]
		case mimirpb.RESOURCE_ATTRIBUTE_INT:
			a.IntValue, n = binary.Varint([]byte(encoded[:min(len(encoded), binary.MaxVarintLen64)]))
			encoded = encoded[n:]
		case mimirpb.RESOURCE_ATTRIBUTE_DOUBLE:
			a.DoubleValue, encoded = math.Float64frombits(binary.LittleEndian.Uint64([]byte(encoded[:8]))), encoded[8:]
		case mimirpb.RESOURCE_ATTRIBUTE_BYTES:
			var value string
			value, encoded = readEncodedString(encoded)
			a.BytesValue = []byte(value)
		default:
			a.StringValue, encoded = readEncodedString(encoded)
		}
		into = append(into, a)
	}
	return into
}

func readEncodedString(encoded string) (string, string) {
	length, n := binary.Uvarint([]byte(encoded[:min(len(encoded), binary.MaxVarintLen64)]))
	end := n + int(length)
	return encoded[n:end], encoded[end:]
}

// resourceDictionary holds an engine's distinct values of one kind, identities or descriptions, each once as its
// canonical encoding, by id: like the metric metadata dictionary, ids are handed out with a reference, and a value
// with none left goes at the next compaction.
type resourceDictionary struct {
	mu   sync.Mutex
	ids  map[string]uint32
	free []uint32
	// The next id never given out; 0 is no value.
	next  uint32
	pages atomic.Pointer[[]*resourcePage]

	values atomic.Int64
	bytes  atomic.Int64
}

const resourcePageSize = 1024

type resourcePage [resourcePageSize]resourceValue

type resourceValue struct {
	encoded string
	refs    atomic.Int64
}

// resourceUnknown marks a run or a version as coalesced: what changed over a limit, which it stands for. Its id is
// the latest value's, or 0 when it's unknown.
const resourceUnknown = uint32(1) << 31

func newResourceDictionary() *resourceDictionary {
	d := &resourceDictionary{ids: map[string]uint32{}, next: 1}
	d.pages.Store(&[]*resourcePage{})
	return d
}

func (d *resourceDictionary) entry(id uint32) *resourceValue {
	id &^= resourceUnknown
	pages := *d.pages.Load()
	return &pages[id/resourcePageSize][id%resourcePageSize]
}

// intern returns the id of the value encoded, with a reference that the caller releases, and whether it's new. A new
// value isn't taken past maxBytes of the dictionaries' values, which total counts, or when allowNew refuses it: its id
// is then 0.
func (d *resourceDictionary) intern(encoded []byte, total *atomic.Int64, maxBytes int64, allowNew func() bool) (uint32, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if id, ok := d.ids[string(encoded)]; ok {
		d.entry(id).refs.Add(1)
		return id, false
	}
	if maxBytes > 0 && total.Load()+int64(len(encoded)) > maxBytes || allowNew != nil && !allowNew() {
		return 0, false
	}
	var id uint32
	if n := len(d.free); n > 0 {
		id, d.free = d.free[n-1], d.free[:n-1]
	} else {
		id = d.next
		if id == resourceUnknown {
			panic("native resource dictionary has no ids left")
		}
		d.next++
		if pages := *d.pages.Load(); int(id/resourcePageSize) >= len(pages) {
			grown := append(slices.Clip(pages), &resourcePage{})
			d.pages.Store(&grown)
		}
	}
	value := d.entry(id)
	// Copied, so that it refers to nothing of the request or record it came from.
	value.encoded = string(encoded)
	value.refs.Store(1)
	d.ids[value.encoded] = id
	d.values.Add(1)
	d.bytes.Add(int64(len(encoded)))
	total.Add(int64(len(encoded)))
	return id, true
}

func (d *resourceDictionary) acquire(id uint32) {
	if id&^resourceUnknown != 0 {
		d.entry(id).refs.Add(1)
	}
}

func (d *resourceDictionary) release(id uint32) {
	if id&^resourceUnknown != 0 {
		d.entry(id).refs.Add(-1)
	}
}

func (d *resourceDictionary) value(id uint32) string {
	if id&^resourceUnknown == 0 {
		return ""
	}
	return d.entry(id).encoded
}

// reclaim drops the values without references, calling dropped with each one's id first, and returns how many.
func (d *resourceDictionary) reclaim(total *atomic.Int64, dropped func(id uint32)) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	reclaimed := 0
	for encoded, id := range d.ids {
		value := d.entry(id)
		if value.refs.Load() > 0 {
			continue
		}
		if dropped != nil {
			dropped(id)
		}
		delete(d.ids, encoded)
		d.free = append(d.free, id)
		d.bytes.Add(-int64(len(encoded)))
		total.Add(-int64(len(encoded)))
		*value = resourceValue{}
		reclaimed++
	}
	d.values.Add(-int64(reclaimed))
	return reclaimed
}

// ResourceLimits are the native resources' soft limits, in sample time: past them, changes coalesce into a range of
// unknown resource until their window ends, and are counted, but samples are never rejected. 0 is no limit.
type ResourceLimits struct {
	// The window the limits count in.
	WindowMs int64
	// How many times a series' identity may change per window.
	MaxIdentityChangesPerSeries int
	// How many versions of its description an identity may have per window.
	MaxVersionsPerIdentity int
	// How many new identities the engine takes per window, and how many bytes the dictionaries' values may have.
	MaxNewIdentities int
	MaxBytes         int64
}

func (l ResourceLimits) windowOf(t int64) int64 {
	return t - ((t%l.WindowMs)+l.WindowMs)%l.WindowMs
}

// ResourceLimit is one of the native resources' limits.
type ResourceLimit int

const (
	// A series' identity changes per window.
	ResourceIdentityChangeLimit ResourceLimit = iota
	// An identity's versions per window.
	ResourceVersionLimit
	// New identities per window, and the dictionaries' bytes.
	ResourceDictionaryLimit
)

// resourceStats are an engine's counters of what its native resources didn't keep as they came.
type resourceStats struct {
	// Each range coalesced over a limit, by the limit, and the values the limits on new identities and on bytes
	// refused.
	coalescedIdentityChanges, coalescedVersions, refused atomic.Uint64
	// Observations older than their shard's base, whose runs start at the base instead.
	clamped atomic.Uint64
	// Lookups of a resource by a batch's memo, and the ones it didn't have yet.
	memoHits, memoMisses atomic.Uint64
	// The older runs and versions evicted past maxResourceRunsPerSeries.
	evictions atomic.Uint64
	// Called with each change over a limit, once set at the engine's open.
	onLimited func(ResourceLimit)
}

func (s *resourceStats) limited(limit ResourceLimit) {
	switch limit {
	case ResourceIdentityChangeLimit:
		s.coalescedIdentityChanges.Add(1)
	case ResourceVersionLimit:
		s.coalescedVersions.Add(1)
	case ResourceDictionaryLimit:
		s.refused.Add(1)
	}
	if s.onLimited != nil {
		s.onLimited(limit)
	}
}

// maxResourceRunsPerSeries bounds a series' older runs and an identity's older versions, whatever the limits let
// through, such as out-of-order changes: past it, the oldest go, and are counted.
const maxResourceRunsPerSeries = 64

// resourceRun is a value from a time until the next run's start, or for the current one, on.
type resourceRun struct {
	from int64
	id   uint32
}

// resourceChanges are a series' or an identity's changes in the window it last changed in, and when its current run
// is a coalesced one, the end of its window.
type resourceChanges struct {
	window, coalescedEnd int64
	count                int
}

func (c *resourceChanges) charge(t int64, max int, l ResourceLimits) bool {
	if max <= 0 || l.WindowMs <= 0 {
		return true
	}
	window := l.windowOf(t)
	switch {
	case window < c.window:
		// An out-of-order change, which an older window doesn't count again.
		return true
	case window > c.window:
		*c = resourceChanges{window: window}
	}
	if c.count >= max {
		return false
	}
	c.count++
	return true
}

// resourceIdentities is the engine's identities' state: their versions, and the reverse index from attributes to
// identities to series. Its lock is taken once per batch for each resource, and for each series whose identity
// changes.
type resourceIdentities struct {
	mu       sync.Mutex
	versions map[uint32][]resourceRun
	changes  map[uint32]resourceChanges
	// The identities with an indexed attribute, by the attribute's encoding, and the series whose runs have each
	// identity.
	byAttribute map[string]*roaring64.Bitmap
	series      map[uint32]*roaring64.Bitmap
	// The new identities' count in the window of the newest.
	newWindow int64
	newCount  int
}

func newResourceIdentities() *resourceIdentities {
	return &resourceIdentities{versions: map[uint32][]resourceRun{}, changes: map[uint32]resourceChanges{}, byAttribute: map[string]*roaring64.Bitmap{}, series: map[uint32]*roaring64.Bitmap{}}
}

// resources is an engine's native resources, which every shard's series refer to.
type resources struct {
	identities, descriptions *resourceDictionary
	// The bytes of both dictionaries' values.
	bytes atomic.Int64
	state *resourceIdentities
	stats resourceStats
	// The descriptive attributes the reverse index has, besides the identifying ones.
	indexed atomic.Pointer[map[string]struct{}]
	limits  atomic.Pointer[ResourceLimits]
}

func newResources() *resources {
	r := &resources{identities: newResourceDictionary(), descriptions: newResourceDictionary(), state: newResourceIdentities()}
	r.indexed.Store(&map[string]struct{}{})
	r.limits.Store(&ResourceLimits{})
	return r
}

// indexIdentity puts the identity under each of its identifying attributes, and the indexed ones of description, in
// the reverse index. The caller holds the state's lock.
func (r *resources) indexIdentityLocked(identity, description uint32) {
	indexed := *r.indexed.Load()
	var attributes [16]mimirpb.ResourceAttribute
	add := func(encoded string, only map[string]struct{}) {
		var buf []byte
		for _, a := range decodeResourceAttributes(encoded, attributes[:0]) {
			if only != nil {
				if _, ok := only[a.Key]; !ok {
					continue
				}
			}
			buf, _ = encodeResourceAttributes(buf[:0], []mimirpb.ResourceAttribute{a}, isIdentifyingResourceAttribute(a.Key), nil)
			bitmap, ok := r.state.byAttribute[string(buf)]
			if !ok {
				bitmap = roaring64.New()
				r.state.byAttribute[string(buf)] = bitmap
			}
			bitmap.Add(uint64(identity))
		}
	}
	add(r.identities.value(identity), nil)
	if len(indexed) > 0 && description&^resourceUnknown != 0 {
		add(r.descriptions.value(description), indexed)
	}
}

// observeIdentity records that identity has description from t on, as a new identity or a version of one, within the
// limit on versions. The caller holds a reference to both.
func (r *resources) observeIdentity(identity, description uint32, t int64, isNew bool) {
	r.state.mu.Lock()
	defer r.state.mu.Unlock()
	versions := r.state.versions[identity]
	if isNew || len(versions) == 0 {
		r.descriptions.acquire(description)
		r.state.versions[identity] = []resourceRun{{from: t, id: description}}
		r.indexIdentityLocked(identity, description)
		return
	}
	current := &versions[len(versions)-1]
	if current.id&resourceUnknown != 0 {
		c := r.state.changes[identity]
		if t < c.coalescedEnd {
			// Within the coalesced version's window: the latest description stands for it.
			if current.id&^resourceUnknown != description {
				r.descriptions.acquire(description)
				r.descriptions.release(current.id)
				current.id = description | resourceUnknown
			}
			return
		}
		// The window ended: the latest description goes on as a version of its own.
		latest := current.id &^ resourceUnknown
		r.descriptions.acquire(latest)
		versions = append(versions, resourceRun{from: c.coalescedEnd, id: latest})
		current = &versions[len(versions)-1]
		r.state.changes[identity] = resourceChanges{window: c.coalescedEnd}
	}
	if current.id == description || t < current.from {
		// The usual observation; or an out-of-order one, which an identity's versions don't take.
		r.state.versions[identity] = versions
		return
	}
	limits := r.limits.Load()
	c := r.state.changes[identity]
	r.descriptions.acquire(description)
	if !c.charge(t, limits.MaxVersionsPerIdentity, *limits) {
		window := limits.windowOf(t)
		c = resourceChanges{window: window, coalescedEnd: window + limits.WindowMs, count: limits.MaxVersionsPerIdentity}
		versions = append(versions, resourceRun{from: t, id: description | resourceUnknown})
		r.stats.limited(ResourceVersionLimit)
	} else {
		versions = append(versions, resourceRun{from: t, id: description})
	}
	r.state.changes[identity] = c
	if evict := len(versions) - 1 - maxResourceRunsPerSeries; evict > 0 {
		for _, v := range versions[:evict] {
			r.descriptions.release(v.id)
		}
		versions = slices.Delete(versions, 0, evict)
		r.stats.evictions.Add(uint64(evict))
	}
	r.state.versions[identity] = versions
	if indexed := *r.indexed.Load(); len(indexed) > 0 {
		r.indexIdentityLocked(identity, description)
	}
}

// allowNewIdentity counts a new identity at t against the limit on new identities, and reports whether it's within
// it. The caller holds the identities dictionary's lock.
func (r *resources) allowNewIdentity(t int64) bool {
	limits := r.limits.Load()
	if limits.MaxNewIdentities <= 0 || limits.WindowMs <= 0 {
		return true
	}
	r.state.mu.Lock()
	defer r.state.mu.Unlock()
	if window := limits.windowOf(t); window > r.state.newWindow {
		r.state.newWindow, r.state.newCount = window, 0
	}
	if r.state.newCount >= limits.MaxNewIdentities {
		return false
	}
	r.state.newCount++
	return true
}

// addSeries and removeSeries keep the reverse index's series of an identity, as a series' runs take it or give it up.
func (r *resources) addSeries(identity uint32, ref uint64) {
	if identity&^resourceUnknown == 0 {
		return
	}
	identity &^= resourceUnknown
	r.state.mu.Lock()
	defer r.state.mu.Unlock()
	bitmap, ok := r.state.series[identity]
	if !ok {
		bitmap = roaring64.New()
		r.state.series[identity] = bitmap
	}
	bitmap.Add(ref)
}

func (r *resources) removeSeries(identity uint32, ref uint64) {
	if identity&^resourceUnknown == 0 {
		return
	}
	identity &^= resourceUnknown
	r.state.mu.Lock()
	defer r.state.mu.Unlock()
	if bitmap, ok := r.state.series[identity]; ok {
		bitmap.Remove(ref)
	}
}

// seriesWithAttribute returns the series whose runs have an identity with the attribute, as the reverse index has it.
func (r *resources) seriesWithAttribute(a mimirpb.ResourceAttribute) *roaring64.Bitmap {
	encoded, _ := encodeResourceAttributes(nil, []mimirpb.ResourceAttribute{a}, isIdentifyingResourceAttribute(a.Key), nil)
	r.state.mu.Lock()
	defer r.state.mu.Unlock()
	out := roaring64.New()
	identities, ok := r.state.byAttribute[string(encoded)]
	if !ok {
		return out
	}
	for it := identities.Iterator(); it.HasNext(); {
		if series, ok := r.state.series[uint32(it.Next())]; ok {
			out.Or(series)
		}
	}
	return out
}

// trim drops the identities' versions that end at or before minTime, the head's min time, and reclaims the values
// without references, dropping reclaimed identities from the reverse index.
func (r *resources) trim(minTime int64) {
	r.state.mu.Lock()
	for identity, versions := range r.state.versions {
		keep := 0
		for keep+1 < len(versions) && versions[keep+1].from <= minTime {
			r.descriptions.release(versions[keep].id)
			keep++
		}
		if keep > 0 {
			r.state.versions[identity] = slices.Clone(versions[keep:])
		}
	}
	r.state.mu.Unlock()

	r.identities.reclaim(&r.bytes, func(identity uint32) {
		r.state.mu.Lock()
		defer r.state.mu.Unlock()
		for _, v := range r.state.versions[identity] {
			r.descriptions.release(v.id)
		}
		delete(r.state.versions, identity)
		delete(r.state.changes, identity)
		delete(r.state.series, identity)
		for encoded, bitmap := range r.state.byAttribute {
			if bitmap.Remove(uint64(identity)); bitmap.IsEmpty() {
				delete(r.state.byAttribute, encoded)
			}
		}
	})
	r.descriptions.reclaim(&r.bytes, nil)
}

// shardResources is a shard's part of an engine's native resources: the identity runs of its series, guarded by the
// shard's lock. A series' current run is inline in it, as an identity id and the start of its run, an offset from the
// shard's base; the older runs of the series whose identity ever changed are here, by series reference, oldest first.
// Every run holds a reference to its identity.
type shardResources struct {
	r *resources
	// What run starts are offsets from, in sample time; set by the first observation.
	base    int64
	baseSet bool
	older   map[uint64][]resourceRun
	changes map[uint64]resourceChanges
	// The series with an identity, and the older runs of all of them.
	series, runs int
}

// seriesResource is a series' current identity: its id, or 0 for none, and the start of its run.
type seriesResource struct {
	resourceID, resourceStart uint32
}

func newShardResources(r *resources) *shardResources {
	return &shardResources{r: r, older: map[uint64][]resourceRun{}, changes: map[uint64]resourceChanges{}}
}

func (m *shardResources) start(series *Series) int64 {
	return m.base + int64(series.resourceStart)
}

// offset is t as an offset from the base, which it sets on the first observation; t before the base clamps to it.
func (m *shardResources) offset(t, marginMs int64) uint32 {
	if !m.baseSet {
		m.base, m.baseSet = t-marginMs, true
	}
	switch delta := t - m.base; {
	case delta < 0:
		m.r.stats.clamped.Add(1)
		return 0
	case delta > math.MaxUint32:
		m.r.stats.clamped.Add(1)
		return math.MaxUint32
	default:
		return uint32(delta)
	}
}

// observe records that series has identity id from t on, an id the caller holds a reference to, within the limit on
// the series' identity changes: past it, the series' run is of unknown resource until the window ends, with the latest
// identity. Equal neighbouring runs merge, and an observation older than the current run goes among the older ones.
func (m *shardResources) observe(series *Series, id uint32, t, marginMs int64) {
	if series.resourceID == 0 {
		m.r.identities.acquire(id)
		m.r.addSeries(id, series.ref)
		series.resourceID, series.resourceStart = id, m.offset(t, marginMs)
		m.series++
		return
	}
	start := m.start(series)
	if t >= start && series.resourceID == id {
		return
	}
	limits := m.r.limits.Load()
	if series.resourceID&resourceUnknown != 0 {
		c := m.changes[series.ref]
		if t >= start && t < c.coalescedEnd {
			if latest := series.resourceID &^ resourceUnknown; latest != id {
				m.r.identities.acquire(id)
				m.r.addSeries(id, series.ref)
				m.r.identities.release(latest)
				series.resourceID = id | resourceUnknown
			}
			return
		}
		if latest := series.resourceID &^ resourceUnknown; t >= c.coalescedEnd && latest != 0 {
			// The window ended: the latest identity goes on as a run of its own. A resource the limits refused has no
			// identity to go on with, so its run just ends with the next observation.
			m.r.identities.acquire(latest)
			m.older[series.ref] = append(m.older[series.ref], resourceRun{from: start, id: series.resourceID})
			m.runs++
			series.resourceID, series.resourceStart = latest, m.offset(c.coalescedEnd, marginMs)
			m.changes[series.ref] = resourceChanges{window: c.coalescedEnd}
			start = m.start(series)
			if series.resourceID == id {
				return
			}
		}
	}
	if t >= start {
		c := m.changes[series.ref]
		m.r.identities.acquire(id)
		m.r.addSeries(id, series.ref)
		m.older[series.ref] = append(m.older[series.ref], resourceRun{from: start, id: series.resourceID})
		m.runs++
		if !c.charge(t, limits.MaxIdentityChangesPerSeries, *limits) {
			window := limits.windowOf(t)
			c = resourceChanges{window: window, coalescedEnd: window + limits.WindowMs, count: limits.MaxIdentityChangesPerSeries}
			series.resourceID = id | resourceUnknown
			m.r.stats.limited(ResourceIdentityChangeLimit)
		} else {
			series.resourceID = id
		}
		series.resourceStart = m.offset(t, marginMs)
		m.changes[series.ref] = c
		m.capRuns(series)
		return
	}
	// Before the current run: put among the series' runs, unless a run of the same identity or a coalesced one
	// already covers t.
	runs := append(m.older[series.ref], resourceRun{from: start, id: series.resourceID})
	at, found := slices.BinarySearchFunc(runs, t, func(r resourceRun, t int64) int { return cmp.Compare(r.from, t) })
	switch {
	case found && runs[at].id == id:
		return
	case found && runs[at].id&resourceUnknown != 0 || !found && at > 0 && (runs[at-1].id == id || runs[at-1].id&resourceUnknown != 0):
		return
	case found:
		m.r.removeSeriesIfGone(series.ref, runs[at].id, runs, at)
		m.r.identities.release(runs[at].id)
		runs[at].id = id
	default:
		runs = slices.Insert(runs, at, resourceRun{from: t, id: id})
	}
	m.r.identities.acquire(id)
	m.r.addSeries(id, series.ref)
	merged := runs[:1]
	for _, run := range runs[1:] {
		if last := merged[len(merged)-1]; last.id == run.id {
			m.r.identities.release(run.id)
			continue
		}
		merged = append(merged, run)
	}
	newest := merged[len(merged)-1]
	series.resourceID, series.resourceStart = newest.id, m.offset(newest.from, marginMs)
	m.runs -= len(m.older[series.ref])
	if older := merged[:len(merged)-1]; len(older) > 0 {
		m.older[series.ref] = slices.Clip(older)
		m.runs += len(older)
		m.capRuns(series)
	} else {
		delete(m.older, series.ref)
	}
}

// removeSeriesIfGone drops the series from the reverse index's series of identity, the identity of runs[at], when no
// other run of the series has it.
func (r *resources) removeSeriesIfGone(ref uint64, identity uint32, runs []resourceRun, at int) {
	for i, run := range runs {
		if i != at && run.id&^resourceUnknown == identity&^resourceUnknown {
			return
		}
	}
	r.removeSeries(identity, ref)
}

// capRuns evicts the series' oldest runs past maxResourceRunsPerSeries.
func (m *shardResources) capRuns(series *Series) {
	older := m.older[series.ref]
	evict := len(older) - maxResourceRunsPerSeries
	if evict <= 0 {
		return
	}
	m.releaseRuns(series, older[:evict], older[evict:])
	kept := copy(older, older[evict:])
	clear(older[kept:])
	m.older[series.ref] = older[:kept]
	m.runs -= evict
	m.r.stats.evictions.Add(uint64(evict))
}

// releaseRuns gives up the gone runs' identities, and the reverse index's series of those the series' kept runs and
// current run don't have.
func (m *shardResources) releaseRuns(series *Series, gone, kept []resourceRun) {
	for _, run := range gone {
		m.r.identities.release(run.id)
		has := series.resourceID&^resourceUnknown == run.id&^resourceUnknown
		for _, k := range kept {
			has = has || k.id&^resourceUnknown == run.id&^resourceUnknown
		}
		if !has {
			m.r.removeSeries(run.id, series.ref)
		}
	}
}

// trim drops series' older runs that end at or before minTime.
func (m *shardResources) trim(series *Series, minTime int64) {
	older := m.older[series.ref]
	keep := 0
	for keep < len(older) {
		end := m.start(series)
		if keep+1 < len(older) {
			end = older[keep+1].from
		}
		if end > minTime {
			break
		}
		keep++
	}
	if keep == 0 {
		return
	}
	m.releaseRuns(series, older[:keep], older[keep:])
	m.runs -= keep
	if keep == len(older) {
		delete(m.older, series.ref)
		return
	}
	m.older[series.ref] = slices.Clone(older[keep:])
}

// drop gives up series' resources, as the series leaves the engine's memory.
func (m *shardResources) drop(series *Series) {
	if series.resourceID == 0 {
		return
	}
	for _, run := range m.older[series.ref] {
		m.r.identities.release(run.id)
		m.r.removeSeries(run.id, series.ref)
	}
	m.runs -= len(m.older[series.ref])
	delete(m.older, series.ref)
	delete(m.changes, series.ref)
	m.r.identities.release(series.resourceID)
	m.r.removeSeries(series.resourceID, series.ref)
	series.resourceID, series.resourceStart = 0, 0
	m.series--
}

func (m *shardResources) runsOf(series *Series, into []resourceRun) []resourceRun {
	if series.resourceID == 0 {
		return into
	}
	into = append(into, m.older[series.ref]...)
	return append(into, resourceRun{from: m.start(series), id: series.resourceID})
}

// resourceObservation is a batch's resource: its identity and description ids, which the memo holds a reference to,
// 0 for an identity the limits refused, and whether the identity's version is recorded yet.
type resourceObservation struct {
	identity, description uint32
	isNew, recorded       bool
}

// resourceMemo is a batch's resources, by the record's resource ref or by entry, each with references it holds until
// release.
type resourceMemo struct {
	r         *resources
	byRef     map[uint32]*resourceObservation
	byEntry   map[*mimirpb.ResourceEntry]*resourceObservation
	byContext map[*storage.ResourceContext]*resourceObservation
	buf       []byte
	order     []int
	scratch   []mimirpb.ResourceAttribute
}

func (e *Engine) newResourceMemo() *resourceMemo {
	return &resourceMemo{r: e.resources}
}

// resolve interns attributes for an observation at t.
func (m *resourceMemo) resolve(attributes []mimirpb.ResourceAttribute, t int64) *resourceObservation {
	m.r.stats.memoMisses.Add(1)
	limits := m.r.limits.Load()
	o := &resourceObservation{}
	m.buf, m.order = encodeResourceAttributes(m.buf[:0], attributes, true, m.order)
	o.identity, o.isNew = m.r.identities.intern(m.buf, &m.r.bytes, limits.MaxBytes, func() bool { return m.r.allowNewIdentity(t) })
	if o.identity == 0 {
		m.r.stats.limited(ResourceDictionaryLimit)
		return o
	}
	m.buf, m.order = encodeResourceAttributes(m.buf[:0], attributes, false, m.order)
	if o.description, _ = m.r.descriptions.intern(m.buf, &m.r.bytes, limits.MaxBytes, nil); o.description == 0 {
		// The identity is known, its description isn't.
		o.description = resourceUnknown
		m.r.stats.limited(ResourceDictionaryLimit)
	}
	return o
}

// byRecordRef is the observation of a record's resource, by its ref into the record's table, whose entry decodes from
// encoded.
func (m *resourceMemo) byRecordRef(ref uint32, encoded []byte, t int64) *resourceObservation {
	if o, ok := m.byRef[ref]; ok {
		m.r.stats.memoHits.Add(1)
		return o
	}
	var entry mimirpb.ResourceEntry
	if err := entry.Unmarshal(encoded); err != nil {
		return nil
	}
	o := m.resolve(entry.Attributes, t)
	if m.byRef == nil {
		m.byRef = map[uint32]*resourceObservation{}
	}
	m.byRef[ref] = o
	return o
}

// byResourceEntry is the observation of a decoded series' resource.
func (m *resourceMemo) byResourceEntry(entry *mimirpb.ResourceEntry, t int64) *resourceObservation {
	if o, ok := m.byEntry[entry]; ok {
		m.r.stats.memoHits.Add(1)
		return o
	}
	o := m.resolve(entry.Attributes, t)
	if m.byEntry == nil {
		m.byEntry = map[*mimirpb.ResourceEntry]*resourceObservation{}
	}
	m.byEntry[entry] = o
	return o
}

// byResourceContext is the observation of the resource an appender got, from its typed attributes: without them,
// it's none.
func (m *resourceMemo) byResourceContext(rc *storage.ResourceContext, t int64) *resourceObservation {
	if len(rc.Attributes) == 0 {
		return nil
	}
	if o, ok := m.byContext[rc]; ok {
		m.r.stats.memoHits.Add(1)
		return o
	}
	m.scratch = m.scratch[:0]
	for _, a := range rc.Attributes {
		m.scratch = append(m.scratch, mimirpb.ResourceAttribute{
			Key: a.Key, Type: mimirpb.ResourceAttribute_Type(a.Type), StringValue: a.String, BoolValue: a.Bool, IntValue: a.Int, DoubleValue: a.Double, BytesValue: a.Bytes,
		})
	}
	o := m.resolve(m.scratch, t)
	if m.byContext == nil {
		m.byContext = map[*storage.ResourceContext]*resourceObservation{}
	}
	m.byContext[rc] = o
	return o
}

func (m *resourceMemo) release() {
	for _, o := range m.byRef {
		m.releaseObservation(o)
	}
	for _, o := range m.byEntry {
		m.releaseObservation(o)
	}
	for _, o := range m.byContext {
		m.releaseObservation(o)
	}
	clear(m.byRef)
	clear(m.byEntry)
	clear(m.byContext)
}

func (m *resourceMemo) releaseObservation(o *resourceObservation) {
	if o != nil {
		m.r.identities.release(o.identity)
		m.r.descriptions.release(o.description)
	}
}

// observeResourceLocked records that series has the observation's resource from timestamp on, for a caller holding
// its shard's lock and, through the memo, references to its values. A resource the limits refused is a run of
// unknown resource.
func (e *Engine) observeResourceLocked(t *tenant, series *Series, o *resourceObservation, timestamp int64) {
	if o == nil {
		return
	}
	if series.resourceID == 0 {
		for observed := e.resourcesObservedFrom.Load(); timestamp < observed && !e.resourcesObservedFrom.CompareAndSwap(observed, timestamp); {
			observed = e.resourcesObservedFrom.Load()
		}
	}
	id := o.identity
	if id == 0 {
		id = resourceUnknown
	} else if !o.recorded {
		e.resources.observeIdentity(o.identity, o.description, timestamp, o.isNew)
		o.recorded = true
	}
	if t.nativeResources == nil {
		t.nativeResources = newShardResources(e.resources)
	}
	t.nativeResources.observe(series, id, timestamp, e.metadataBaseMargin())
}

// trimResourcesLocked drops the older runs of the shard's series that end by minTime, and rebases the shard's run
// starts onto minTime once the head's max time nears the end of the offsets' range, for a caller holding the shard's
// lock.
func (e *Engine) trimResourcesLocked(t *tenant, minTime, maxTime int64) {
	m := t.nativeResources
	if m == nil {
		return
	}
	for ref := range m.older {
		if entry, ok := e.lookupLocked(t, ref); ok {
			m.trim(&entry.series, minTime)
		}
	}
	if !m.baseSet || maxTime-m.base < metadataRebaseAfter {
		return
	}
	base := minTime - e.metadataBaseMargin()
	for group := range t.series.groups {
		entries := t.series.groups[group].entries
		for index := range entries {
			if series := &entries[index].series; series.resourceID != 0 {
				series.resourceStart = uint32(max(0, m.base+int64(series.resourceStart)-base))
			}
		}
	}
	m.base = base
}

// SetNativeResources sets whether the series' resources are kept, from the next batch on.
func (e *Engine) SetNativeResources(enabled bool) {
	e.nativeResources.Store(enabled)
}

// SetResourceLimits sets the native resources' soft limits, from the next batch on.
func (e *Engine) SetResourceLimits(limits ResourceLimits) {
	if *e.resources.limits.Load() != limits {
		e.resources.limits.Store(&limits)
	}
}

// ResourceVersion is a series' resource from a time on.
type ResourceVersion struct {
	EffectiveFrom int64
	// The resource's attributes, the identifying ones first, each by key; nil for an unknown resource.
	Identifying, Descriptive []mimirpb.ResourceAttribute
	// Whether the version stands for changes over a limit, with the latest of their values, or none if unknown.
	Coalesced bool
}

// NativeResourcesSeries is a head series' native resources.
type NativeResourcesSeries struct {
	Labels   promlabels.Labels
	Versions []ResourceVersion
}

// versionsLocked appends the series' resource versions in force during [mint, maxt], oldest first, joining its
// identity runs with their identities' versions, for a caller holding its shard's lock.
func (e *Engine) resourceVersionsLocked(m *shardResources, series *Series, mint, maxt int64, into []ResourceVersion) []ResourceVersion {
	var buf [8]resourceRun
	runs := m.runsOf(series, buf[:0])
	r := e.resources
	r.state.mu.Lock()
	defer r.state.mu.Unlock()
	for i, run := range runs {
		end := int64(math.MaxInt64)
		if i+1 < len(runs) {
			end = runs[i+1].from
		}
		if run.from > maxt || end <= mint {
			continue
		}
		identity := run.id &^ resourceUnknown
		if identity == 0 {
			into = append(into, ResourceVersion{EffectiveFrom: run.from, Coalesced: true})
			continue
		}
		identifying := decodeResourceAttributes(r.identities.value(identity), nil)
		versions := r.state.versions[identity]
		if len(versions) == 0 {
			into = append(into, ResourceVersion{EffectiveFrom: run.from, Identifying: identifying, Coalesced: run.id&resourceUnknown != 0})
			continue
		}
		for j, v := range versions {
			// An identity's first version is its description since the identity's start, whichever series' sample
			// the version took its time from.
			from, to := run.from, end
			if j > 0 {
				from = max(from, v.from)
			}
			if j+1 < len(versions) {
				to = min(to, versions[j+1].from)
			}
			if from >= to || from > maxt || to <= mint {
				continue
			}
			into = append(into, ResourceVersion{
				EffectiveFrom: from,
				Identifying:   identifying,
				Descriptive:   decodeResourceAttributes(r.descriptions.value(v.id), nil),
				Coalesced:     run.id&resourceUnknown != 0 || v.id&resourceUnknown != 0,
			})
		}
	}
	return into
}

// NativeResources returns the native resources in force during [mint, maxt] of the head series that match any of
// matcherSets, in label order, at most limit of them with a positive limit, and whether more matched.
func (e *Engine) NativeResources(ctx context.Context, matcherSets [][]*promlabels.Matcher, mint, maxt int64, limit int) ([]NativeResourcesSeries, bool, error) {
	compiled := make([][]compiledMatcher, 0, len(matcherSets))
	for _, matchers := range matcherSets {
		c, err := compilePromMatchers(nil, matchers)
		if err != nil {
			return nil, false, err
		}
		compiled = append(compiled, c)
	}
	type selected struct {
		labels   labels.Labels
		versions []ResourceVersion
	}
	var all []selected
	seen := map[uint64]struct{}{}
	var err error
	e.perShard(func(_ int, t *tenant, _ *chunks.DiskMapper, _ *coldState) {
		if err = ctx.Err(); err != nil || t.nativeResources == nil {
			return
		}
		for _, matchers := range compiled {
			t.series.matching(matchers, func(entry *seriesEntry) bool {
				if entry.series.resourceID == 0 {
					return true
				}
				if _, ok := seen[entry.series.ref]; ok {
					return true
				}
				seen[entry.series.ref] = struct{}{}
				if versions := e.resourceVersionsLocked(t.nativeResources, &entry.series, mint, maxt, nil); len(versions) > 0 {
					all = append(all, selected{labels: entry.labels, versions: versions})
				}
				return true
			})
		}
	})
	if err != nil {
		return nil, false, err
	}
	slices.SortFunc(all, func(a, b selected) int { return labels.Compare(a.labels, b.labels) })
	limited := limit > 0 && len(all) > limit
	if limited {
		all = all[:limit]
	}
	out := make([]NativeResourcesSeries, 0, len(all))
	var builder promlabels.ScratchBuilder
	for _, s := range all {
		out = append(out, NativeResourcesSeries{Labels: toPromLabels(s.labels, &builder), Versions: s.versions})
	}
	return out, limited, nil
}

// NativeResourcesCoverageStart is when the native resources start: the later of the head's min time and the oldest
// time a series' resource was first observed at since the engine opened.
func (e *Engine) NativeResourcesCoverageStart() int64 {
	return max(e.MinTime(), e.resourcesObservedFrom.Load())
}

// ResourceStats are an engine's native resources, as its metrics report them.
type ResourceStats struct {
	// The series with a resource, and their runs: one each, and their older runs.
	Series, Runs int64
	// The dictionaries' values and their bytes, and the identities' versions.
	Identities, Descriptions, Bytes, Versions int64
	// The ranges coalesced over each limit, the values the limits refused, the runs whose start clamped to their
	// shard's base, the batches' memo lookups, and the runs and versions evicted past the cap on them.
	CoalescedIdentityChanges, CoalescedVersions, Refused, Clamped, MemoHits, MemoMisses, Evictions uint64
}

// ResourceStats returns the engine's native resources as its metrics report them.
func (e *Engine) ResourceStats() ResourceStats {
	r := e.resources
	stats := ResourceStats{
		Identities:               r.identities.values.Load(),
		Descriptions:             r.descriptions.values.Load(),
		Bytes:                    r.bytes.Load(),
		CoalescedIdentityChanges: r.stats.coalescedIdentityChanges.Load(),
		CoalescedVersions:        r.stats.coalescedVersions.Load(),
		Refused:                  r.stats.refused.Load(),
		Clamped:                  r.stats.clamped.Load(),
		MemoHits:                 r.stats.memoHits.Load(),
		MemoMisses:               r.stats.memoMisses.Load(),
		Evictions:                r.stats.evictions.Load(),
	}
	r.state.mu.Lock()
	for _, versions := range r.state.versions {
		stats.Versions += int64(len(versions))
	}
	r.state.mu.Unlock()
	for index, state := range e.store.shards {
		state.RLock()
		if t, ok := e.tenantLocked(index); ok && t.nativeResources != nil {
			stats.Series += int64(t.nativeResources.series)
			stats.Runs += int64(t.nativeResources.series + t.nativeResources.runs)
		}
		state.RUnlock()
	}
	return stats
}
