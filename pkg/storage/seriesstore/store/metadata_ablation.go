// SPDX-License-Identifier: AGPL-3.0-only

//go:build native_metadata_ablation

package store

import (
	"hash/maphash"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/metadata"

	"github.com/grafana/mimir/pkg/mimirpb"
)

// The native metadata prototype's ablation (plan 2.4): the engine with each mechanism of Prometheus's #19563 that the
// agreed classification calls a choice, done as #19563 does it, and none of those it calls imposed by the TSDB, which
// are passing metadata with every Append and buffering it until commit, the commit-time sort and batching, the skip's
// stripe filter, the memSeries sidecar and the presence directory. Comparing an incoming value with the series' is
// inherent, so the ablation compares as the engine does, through its memo of a record's values. A transaction is a
// batch's 150 series, as the TSDB arm's batches are, or an appender's commit.
//
// The choices are switches, each on unless a build tag turns it off, in metadata_ablation_switches_*.go:
//   - ablationUnchangedSkip, the append-time unchanged skip: an observation of the series' current value, at or after
//     its start, does nothing. ablation_without_unchanged_skip merges every observation, which a repeated value
//     doesn't change.
//   - ablationTransactionTable, the transaction's table of up to 128 values as they came, resolved only when kept,
//     and resolving a value to one the series already has before the cache. ablation_without_transaction_table
//     resolves each value through the cache as it comes.
//   - ablationValueCache, the engine's cache of 64 shards of 64 values in 4 MiB, which admits a value at its second
//     miss. ablation_without_value_cache makes every resolved value a copy of its own.
//   - ablationMaxVersions, the five-version cap: a series keeps at most this many versions, the oldest evicted, which
//     truncates it. ablation_without_version_cap makes it 65, the default build's 64 older runs and the current one.
//
// A series' state of 48 bytes, out of line and allocated at its first value, is this build itself.

const (
	ablationTableValues       = 128
	ablationTransactionSeries = 150
	// A reference with this bit set is to a resolved value of the transaction's, rather than one of its table.
	ablationResolvedRef = uint32(1) << 31

	ablationCacheShards        = 64
	ablationValuesPerShard     = 64
	ablationRecentMisses       = 64
	ablationCacheBytes         = 4 << 20
	ablationInternMinBytes     = 64
	ablationPooledStringCopies = 256
)

// seriesMetadata is a series' native metric metadata as #19563's memSeries has it: its state out of line, nil until
// its first value.
type seriesMetadata struct {
	native *ablationSeries
}

func (s *seriesMetadata) hasMetricMetadata() bool { return s.native != nil }

// ablationSeries is #19563's nativeSeriesMetadata: the newest value inline and the older ones, oldest first, no two
// neighbours equal, and whether the cap ever evicted one. The shard's lock guards it.
type ablationSeries struct {
	metadata      *metadata.Metadata
	effectiveFrom int64
	older         []ablationPoint
	truncated     bool
}

type ablationPoint struct {
	effectiveFrom int64
	metadata      *metadata.Metadata
}

// mergeLocked merges non-empty observations, in strictly increasing time, into the series' versions, as #19563's
// does, and returns how many the cap evicted. Their values are immutable.
func (n *ablationSeries) mergeLocked(observations []ablationPoint) int {
	evictions := 0
	if n.metadata == nil || observations[0].effectiveFrom >= n.effectiveFrom {
		for _, observation := range observations {
			switch {
			case n.metadata == nil:
				n.effectiveFrom, n.metadata = observation.effectiveFrom, observation.metadata
			case observation.effectiveFrom == n.effectiveFrom:
				n.metadata = observation.metadata
				if last := len(n.older) - 1; last >= 0 && equalAblationMetadata(n.older[last].metadata, n.metadata) {
					n.effectiveFrom = n.older[last].effectiveFrom
					n.older[last] = ablationPoint{}
					n.older = n.older[:last]
				}
			case equalAblationMetadata(observation.metadata, n.metadata):
				// A repeated value does not advance the change point.
			default:
				previous := ablationPoint{effectiveFrom: n.effectiveFrom, metadata: n.metadata}
				if len(n.older) == ablationMaxVersions-1 {
					copy(n.older, n.older[1:])
					n.older[len(n.older)-1] = previous
					evictions++
				} else {
					n.older = append(ensureAblationCapacity(n.older, len(n.older)+1), previous)
				}
				n.effectiveFrom, n.metadata = observation.effectiveFrom, observation.metadata
			}
		}
	} else {
		var existing, retained [ablationMaxVersions]ablationPoint
		count := copy(existing[:], n.older)
		existing[count] = ablationPoint{effectiveFrom: n.effectiveFrom, metadata: n.metadata}
		var versions []ablationPoint
		versions, evictions = mergeOverlappingAblationPoints(existing[:count+1], observations, retained[:0])
		olderCount := len(versions) - 1
		if len(n.older) > olderCount {
			clear(n.older[olderCount:])
			n.older = n.older[:olderCount]
		}
		n.older = ensureAblationCapacity(n.older, olderCount)[:olderCount]
		copy(n.older, versions[:olderCount])
		newest := versions[olderCount]
		n.effectiveFrom, n.metadata = newest.effectiveFrom, newest.metadata
	}
	if len(n.older) == 0 {
		n.older = nil
	}
	if evictions > 0 {
		n.truncated = true
	}
	return evictions
}

// ensureAblationCapacity returns older with room for at least need points, its capacity growing in powers of two.
func ensureAblationCapacity(older []ablationPoint, need int) []ablationPoint {
	if cap(older) >= need {
		return older
	}
	capacity := 1
	for capacity < need {
		capacity *= 2
	}
	grown := make([]ablationPoint, len(older), capacity)
	copy(grown, older)
	return grown
}

// mergeOverlappingAblationPoints merges points in strictly increasing time, the observations winning at equal times
// and equal neighbours coalescing, appends the ones the cap keeps to versions, and returns them and how many it
// evicted.
func mergeOverlappingAblationPoints(existing, observations, versions []ablationPoint) ([]ablationPoint, int) {
	var retained [ablationMaxVersions]ablationPoint
	start, count, evictions := 0, 0, 0
	var lastMetadata *metadata.Metadata
	appendPoint := func(point ablationPoint) {
		if equalAblationMetadata(lastMetadata, point.metadata) {
			return
		}
		lastMetadata = point.metadata
		if count < len(retained) {
			retained[(start+count)%len(retained)] = point
			count++
			return
		}
		retained[start] = point
		start = (start + 1) % len(retained)
		evictions++
	}
	for i, j := 0, 0; i < len(existing) || j < len(observations); {
		switch {
		case i == len(existing):
			appendPoint(observations[j])
			j++
		case j == len(observations):
			appendPoint(existing[i])
			i++
		case existing[i].effectiveFrom < observations[j].effectiveFrom:
			appendPoint(existing[i])
			i++
		case existing[i].effectiveFrom > observations[j].effectiveFrom:
			appendPoint(observations[j])
			j++
		default:
			appendPoint(observations[j])
			i++
			j++
		}
	}
	for i := range count {
		versions = append(versions, retained[(start+i)%len(retained)])
	}
	return versions, evictions
}

func equalAblationMetadata(a, b *metadata.Metadata) bool {
	return a == b || a != nil && b != nil && *a == *b
}

// metricMetadataValueCache is #19563's cache of an engine's values: they own their strings, and a value is shared
// once it missed twice, within the cache's bounds.
type metricMetadataValueCache struct {
	// The values cached, and the bytes of their type, unit and help.
	values, bytes atomic.Int64
	seed          maphash.Seed
	shards        [ablationCacheShards]atomic.Pointer[ablationCacheShard]
}

// ablationCacheShard holds its values, oldest first for eviction, and its recent misses, under its lock.
type ablationCacheShard struct {
	sync.Mutex
	values      map[metadata.Metadata]*metadata.Metadata
	fifo        [ablationValuesPerShard]*metadata.Metadata
	first       int
	count       int
	recent      [ablationRecentMisses]uint64
	recentCount int
	recentNext  int
}

// metricMetadataValues are the engine's metric metadata values: #19563's cache, in this build.
type metricMetadataValues = metricMetadataValueCache

func newMetricMetadataValues() *metricMetadataValues {
	return &metricMetadataValueCache{seed: maphash.MakeSeed()}
}

// reclaim does nothing: the cache bounds itself, and the series' values are theirs.
func (c *metricMetadataValueCache) reclaim() int { return 0 }

// resolve returns an immutable value equal to m, which owns its strings, copied through strs.
func (c *metricMetadataValueCache) resolve(m metadata.Metadata, strs *ablationStrings) *metadata.Metadata {
	if !ablationValueCache {
		return strs.cloneMetadata(m)
	}
	size := int64(len(m.Type) + len(m.Unit) + len(m.Help))
	hash := maphash.Comparable(c.seed, m)
	slot := &c.shards[hash%ablationCacheShards]
	shard := slot.Load()
	if shard == nil {
		shard = &ablationCacheShard{}
		if !slot.CompareAndSwap(nil, shard) {
			shard = slot.Load()
		}
	}
	shard.Lock()
	if existing := shard.values[m]; existing != nil {
		shard.Unlock()
		return existing
	}
	shard.Unlock()

	owned := strs.cloneMetadata(m)
	shard.Lock()
	defer shard.Unlock()
	if existing := shard.values[m]; existing != nil {
		return existing
	}
	// A second miss admits a value.
	if !slices.Contains(shard.recent[:shard.recentCount], hash) {
		shard.recent[shard.recentNext] = hash
		shard.recentNext = (shard.recentNext + 1) % len(shard.recent)
		shard.recentCount = min(shard.recentCount+1, len(shard.recent))
		return owned
	}
	for shard.count >= len(shard.fifo) || !c.reserve(size) {
		if shard.count == 0 {
			return owned
		}
		old := shard.fifo[shard.first]
		shard.fifo[shard.first] = nil
		shard.first = (shard.first + 1) % len(shard.fifo)
		shard.count--
		delete(shard.values, *old)
		c.values.Add(-1)
		c.bytes.Add(-int64(len(old.Type) + len(old.Unit) + len(old.Help)))
	}
	if shard.values == nil {
		shard.values = make(map[metadata.Metadata]*metadata.Metadata)
	}
	// The key owns its strings too, so that it pins nothing of the caller's.
	shard.values[*owned] = owned
	shard.fifo[(shard.first+shard.count)%len(shard.fifo)] = owned
	shard.count++
	c.values.Add(1)
	return owned
}

func (c *metricMetadataValueCache) reserve(size int64) bool {
	if size == 0 {
		return true
	}
	for {
		used := c.bytes.Load()
		if size > ablationCacheBytes-used {
			return false
		}
		if c.bytes.CompareAndSwap(used, used+size) {
			return true
		}
	}
}

// ablationStrings copies each long metadata string once per transaction, as #19563's appender does.
type ablationStrings map[string]string

func (strs *ablationStrings) clone(s string) string {
	if len(s) < ablationInternMinBytes {
		return strings.Clone(s)
	}
	if c, ok := (*strs)[s]; ok {
		return c
	}
	if *strs == nil {
		*strs = make(ablationStrings)
	}
	c := strings.Clone(s)
	(*strs)[c] = c
	return c
}

func (strs *ablationStrings) cloneMetadata(m metadata.Metadata) *metadata.Metadata {
	if len(m.Type) < ablationInternMinBytes && len(m.Unit) < ablationInternMinBytes && len(m.Help) < ablationInternMinBytes {
		return &metadata.Metadata{Type: model.MetricType(strings.Clone(string(m.Type))), Unit: strings.Clone(m.Unit), Help: strings.Clone(m.Help)}
	}
	return &metadata.Metadata{Type: model.MetricType(strs.clone(string(m.Type))), Unit: strs.clone(m.Unit), Help: strs.clone(m.Help)}
}

// shardMetadata counts a shard's series with native metric metadata and their older versions, which are the
// series' own. The shard's lock guards it.
type shardMetadata struct {
	stats        *metricMetadataStats
	series, runs int
}

func newShardMetadata(_ *metricMetadataValues, stats *metricMetadataStats) *shardMetadata {
	return &shardMetadata{stats: stats}
}

// drop gives up series' metadata, as the series leaves the engine's memory.
func (m *shardMetadata) drop(series *Series) {
	if series.native == nil {
		return
	}
	m.runs -= len(series.native.older)
	m.series--
	series.native = nil
}

// metadataMemo is a transaction's values, as #19563's appender has them: up to 128 as they came, each resolved once
// kept, then values resolved as they come, by references, and the long strings it copied. A record's values are also
// found by their symbol references, as the engine's memo finds them. On a batch's paths, whose references are used at
// once, a transaction ends every 150 series; an appender's is its commit.
type metadataMemo struct {
	values *metricMetadataValueCache
	stats  *metricMetadataStats
	// The record's references by their symbol references, and the last one found.
	refs     map[[3]uint32]uint32
	lastRefs [3]uint32
	lastRef  uint32
	lastSet  bool

	table    map[metadata.Metadata]uint32
	raw      []ablationValue
	resolved []*metadata.Metadata
	strings  ablationStrings
	series   int
}

// ablationValue is a value as a batch has it, with the batch's strings, and once resolved, its immutable copy.
type ablationValue struct {
	metadata metadata.Metadata
	owned    *metadata.Metadata
}

func (e *Engine) newMetadataMemo() *metadataMemo {
	return &metadataMemo{values: e.metricMetadata, stats: &e.metricMetadataStats}
}

// byRefs is the reference of a record's series' value, by its type and symbol references.
func (m *metadataMemo) byRefs(key [3]uint32, value mimirpb.SeriesMetadata) uint32 {
	m.nextSeries()
	if m.lastSet && key == m.lastRefs {
		m.stats.memoHits.Add(1)
		return m.lastRef
	}
	if ref, ok := m.refs[key]; ok {
		m.stats.memoHits.Add(1)
		m.lastRefs, m.lastRef = key, ref
		return ref
	}
	m.stats.memoMisses.Add(1)
	var ref uint32
	if kept, ok := metricMetadataOf(value, m.stats); ok {
		ref = m.reference(kept)
	}
	if m.refs == nil {
		m.refs = map[[3]uint32]uint32{}
	}
	m.refs[key] = ref
	m.lastRefs, m.lastRef, m.lastSet = key, ref, true
	return ref
}

// byContent is the reference of a batch's series' own metadata.
func (m *metadataMemo) byContent(value mimirpb.SeriesMetadata) uint32 {
	m.nextSeries()
	kept, ok := metricMetadataOf(value, m.stats)
	if !ok {
		return 0
	}
	return m.reference(kept)
}

// byValue is the reference of an appended value as the native metric metadata keeps it.
func (m *metadataMemo) byValue(value metadata.Metadata) uint32 {
	return m.reference(value)
}

// nextSeries ends the transaction after its 150th series.
func (m *metadataMemo) nextSeries() {
	if m.series == ablationTransactionSeries {
		m.release()
	}
	m.series++
}

// reference is the transaction's reference to v, as #19563's metadataReference: an entry of its table while it has
// room, the value resolved otherwise.
func (m *metadataMemo) reference(v metadata.Metadata) uint32 {
	if ablationTransactionTable {
		if ref, ok := m.table[v]; ok {
			m.stats.memoHits.Add(1)
			return ref
		}
		m.stats.memoMisses.Add(1)
		if len(m.raw) < ablationTableValues {
			if m.table == nil {
				m.table = make(map[metadata.Metadata]uint32, ablationTableValues)
			}
			m.raw = append(m.raw, ablationValue{metadata: v})
			ref := uint32(len(m.raw))
			m.table[v] = ref
			return ref
		}
	}
	m.resolved = append(m.resolved, m.values.resolve(v, &m.strings))
	return ablationResolvedRef | uint32(len(m.resolved)-1)
}

// equal reports whether ref's value is stored, the series' current one: by its pointer when it's the one the
// transaction has, by its strings otherwise.
func (m *metadataMemo) equal(ref uint32, stored *metadata.Metadata) bool {
	if ref&ablationResolvedRef != 0 {
		return equalAblationMetadata(m.resolved[ref&^ablationResolvedRef], stored)
	}
	v := &m.raw[ref-1]
	if v.owned == stored {
		return true
	}
	if v.metadata != *stored {
		return false
	}
	if ablationTransactionTable && v.owned == nil {
		// The transaction resolves the value to the series' own, as #19563's resolveMetadata would: its next series
		// with this one compares by pointer.
		v.owned = stored
	}
	return true
}

// owned is ref's immutable value for series, as #19563's metadataPointer resolves it: one the series already has, or
// the cache's.
func (m *metadataMemo) owned(series *Series, ref uint32) *metadata.Metadata {
	if ref&ablationResolvedRef != 0 {
		return m.resolved[ref&^ablationResolvedRef]
	}
	v := &m.raw[ref-1]
	if v.owned == nil {
		v.owned = m.resolve(series, v.metadata)
	}
	return v.owned
}

func (m *metadataMemo) resolve(series *Series, v metadata.Metadata) *metadata.Metadata {
	if native := series.native; native != nil {
		if *native.metadata == v {
			return native.metadata
		}
		for _, point := range native.older {
			if *point.metadata == v {
				return point.metadata
			}
		}
	}
	return m.values.resolve(v, &m.strings)
}

// release ends the transaction.
func (m *metadataMemo) release() {
	clear(m.refs)
	m.lastSet = false
	clear(m.table)
	clear(m.raw)
	m.raw = m.raw[:0]
	clear(m.resolved)
	m.resolved = m.resolved[:0]
	if len(m.strings) > ablationPooledStringCopies {
		m.strings = nil
	} else {
		clear(m.strings)
	}
	m.series = 0
}

// observeMetadataLocked records that series has ref's value of memo's transaction from timestamp on, for a caller
// holding its shard's lock.
func (e *Engine) observeMetadataLocked(t *tenant, series *Series, memo *metadataMemo, ref uint32, timestamp int64) {
	native := series.native
	if native == nil {
		for observed := e.metadataObservedFrom.Load(); timestamp < observed && !e.metadataObservedFrom.CompareAndSwap(observed, timestamp); {
			observed = e.metadataObservedFrom.Load()
		}
	}
	if ablationUnchangedSkip && native != nil && timestamp >= native.effectiveFrom && memo.equal(ref, native.metadata) {
		return
	}
	m := e.shardMetadataLocked(t)
	observation := [1]ablationPoint{{effectiveFrom: timestamp, metadata: memo.owned(series, ref)}}
	if native == nil {
		native = &ablationSeries{}
		series.native = native
		m.series++
	}
	older := len(native.older)
	if evictions := native.mergeLocked(observation[:]); evictions > 0 {
		m.stats.evictions.Add(uint64(evictions))
	}
	m.runs += len(native.older) - older
}

// trimMetadataLocked does nothing: #19563 keeps a series' versions as long as the series, which its cap bounds.
func (e *Engine) trimMetadataLocked(*tenant, int64, int64) {}

// versionsLocked appends series' versions to into, and reports that none is a coalesced one, and whether the series
// is truncated.
func (e *Engine) versionsLocked(_ *shardMetadata, series *Series, into []MetricMetadataVersion) ([]MetricMetadataVersion, bool, bool) {
	native := series.native
	if native == nil {
		return into, false, false
	}
	for _, point := range native.older {
		into = append(into, MetricMetadataVersion{EffectiveFrom: point.effectiveFrom, Metadata: *point.metadata})
	}
	into = append(into, MetricMetadataVersion{EffectiveFrom: native.effectiveFrom, Metadata: *native.metadata})
	return into, false, native.truncated
}
