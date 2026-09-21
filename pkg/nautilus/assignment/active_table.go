// SPDX-License-Identifier: AGPL-3.0-only

package assignment

import (
	"encoding/json"
	"sort"
	"time"
)

// ActiveTable is a query-optimized view of the entries from a Log
// that are active at a given moment. It exists so consumers in the
// hot path (distributor write routing) can resolve a hash key to a
// partition in O(log N) without scanning the entire log on each
// lookup.
//
// The table is immutable after construction. Each entry covers a
// disjoint, non-overlapping portion of its tenant's 32-bit hash space; if the
// source Log was driven by valid Apply calls, each tenant's entries also tile
// the full space contiguously. Entries are sorted by (TenantID, Range.Lo).
//
// ValidUntil reports the soonest expiration of any included entry:
// after that wall-clock instant, at least one tile is no longer
// active and the table no longer reflects the live tiling. The
// distributor uses ValidUntil to decide when to rebuild from the
// underlying Log (which may also contain a pre-issued successor
// that became active in the meantime).
type ActiveTable struct {
	// entries is the list of entries from the source Log that were
	// active at the wall-clock passed to ActiveTable, sorted by
	// (TenantID, Range.Lo).
	entries []LogEntry
	tenants map[string]activeTenantTable

	// builtAt is the `at` argument passed to NewActiveTable. Lookups
	// for `at` strictly less than builtAt may return entries whose
	// leases hadn't started yet, so callers should rebuild for
	// significantly earlier `at` values.
	builtAt time.Time
}

type activeTenantTable struct {
	start      int
	end        int
	validUntil time.Time
}

// LookupMissDebug describes the ActiveTable state around a key whose
// Lookup returned false. It is intended for low-volume diagnostics on
// routing failures, not for the hot success path.
type LookupMissDebug struct {
	SearchIndex       int      `json:"search_index"`
	HasByHiEntry      bool     `json:"has_by_hi_entry"`
	ByHiEntry         LogEntry `json:"by_hi_entry,omitempty"`
	ByLoIndex         int      `json:"by_lo_index"`
	HasByLoEntry      bool     `json:"has_by_lo_entry"`
	ByLoEntry         LogEntry `json:"by_lo_entry,omitempty"`
	HasNextByLoEntry  bool     `json:"has_next_by_lo_entry"`
	NextByLoEntry     LogEntry `json:"next_by_lo_entry,omitempty"`
	CoveringEntries   int      `json:"covering_entries"`
	CoveringPartition []int32  `json:"covering_partitions,omitempty"`
}

func (d LookupMissDebug) String() string {
	b, err := json.Marshal(d)
	if err != nil {
		return "<lookup miss debug marshal error>"
	}
	return string(b)
}

// ActiveTable returns a new ActiveTable for the entries in l whose
// leases cover wall-clock at, or nil if no entries are active at
// at. The returned table holds its own slice and is safe to use
// concurrently with further mutations of l (including via Apply).
func (l *Log) ActiveTable(at time.Time) *ActiveTable {
	return l.activeTable(at, nil)
}

// ActiveTableForTenant returns a query-optimized view containing only
// tenantID's entries active at at.
func (l *Log) ActiveTableForTenant(tenantID string, at time.Time) *ActiveTable {
	return l.activeTable(at, &tenantID)
}

func (l *Log) activeTable(at time.Time, onlyTenant *string) *ActiveTable {
	// Avoid allocating ActiveAt's slice; we'll do our own pass.
	count := 0
	for _, e := range l.entries {
		if onlyTenant != nil && e.TenantID != *onlyTenant {
			continue
		}
		if e.ActiveAt(at) {
			count++
		}
	}
	if count == 0 {
		return nil
	}

	entries := make([]LogEntry, 0, count)
	for _, e := range l.entries {
		if onlyTenant != nil && e.TenantID != *onlyTenant {
			continue
		}
		if !e.ActiveAt(at) {
			continue
		}
		entries = append(entries, e)
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].TenantID != entries[j].TenantID {
			return entries[i].TenantID < entries[j].TenantID
		}
		return entries[i].Range.Lo < entries[j].Range.Lo
	})
	tenants := make(map[string]activeTenantTable)
	for start := 0; start < len(entries); {
		tenantID := entries[start].TenantID
		end := start + 1
		tenantValidUntil := entries[start].To
		for end < len(entries) && entries[end].TenantID == tenantID {
			if tenantValidUntil.IsZero() || (!entries[end].To.IsZero() && entries[end].To.Before(tenantValidUntil)) {
				tenantValidUntil = entries[end].To
			}
			end++
		}
		tenants[tenantID] = activeTenantTable{start: start, end: end, validUntil: tenantValidUntil}
		start = end
	}
	return &ActiveTable{
		entries: entries,
		tenants: tenants,
		builtAt: at,
	}
}

// Lookup returns the partition ID of the tile containing key, or
// (0, false) if no tile in the table contains it. O(log N).
//
// Lookup does not validate that the table is still fresh; callers
// should compare their wall-clock with ValidUntil before relying on
// the result. The lookup itself is correct regardless: it simply
// returns whatever tile owned key at the table's construction time.
func (t *ActiveTable) Lookup(key uint32) (int32, bool) {
	return t.LookupForTenant("", key)
}

// LookupForTenant returns the partition ID of tenantID's tile containing key.
func (t *ActiveTable) LookupForTenant(tenantID string, key uint32) (int32, bool) {
	bounds, ok := t.tenants[tenantID]
	if !ok {
		return 0, false
	}
	// Find the first tile whose Range.Hi >= key. Because tiles in
	// the table are non-overlapping and sorted by Range.Lo, that
	// tile (if it exists) is the unique candidate.
	idx := bounds.start + sort.Search(bounds.end-bounds.start, func(i int) bool {
		return t.entries[bounds.start+i].Range.Hi >= key
	})
	if idx >= bounds.end {
		return 0, false
	}
	e := &t.entries[idx]
	if key < e.Range.Lo {
		// Hash gap (the source Log's active tiling didn't cover key).
		return 0, false
	}
	return e.PartitionID, true
}

// DebugLookupMiss returns a compact diagnostic view around key after
// Lookup(key) returned false.
func (t *ActiveTable) DebugLookupMiss(key uint32) LookupMissDebug {
	return t.DebugLookupMissForTenant("", key)
}

// DebugLookupMissForTenant returns diagnostics for a tenant-scoped miss.
func (t *ActiveTable) DebugLookupMissForTenant(tenantID string, key uint32) LookupMissDebug {
	out := LookupMissDebug{ByLoIndex: -1}
	bounds, ok := t.tenants[tenantID]
	if !ok {
		return out
	}
	out.SearchIndex = bounds.start + sort.Search(bounds.end-bounds.start, func(i int) bool {
		return t.entries[bounds.start+i].Range.Hi >= key
	})
	if out.SearchIndex < bounds.end {
		out.HasByHiEntry = true
		out.ByHiEntry = t.entries[out.SearchIndex]
	}

	byLo := bounds.start + sort.Search(bounds.end-bounds.start, func(i int) bool {
		return t.entries[bounds.start+i].Range.Lo > key
	}) - 1
	out.ByLoIndex = byLo
	if byLo >= bounds.start {
		out.HasByLoEntry = true
		out.ByLoEntry = t.entries[byLo]
	}
	if byLo+1 >= bounds.start && byLo+1 < bounds.end {
		out.HasNextByLoEntry = true
		out.NextByLoEntry = t.entries[byLo+1]
	}

	seen := map[int32]struct{}{}
	for i := bounds.start; i < bounds.end; i++ {
		e := &t.entries[i]
		if e.Range.Lo > key {
			break
		}
		if !e.Range.Contains(key) {
			continue
		}
		out.CoveringEntries++
		if _, ok := seen[e.PartitionID]; ok {
			continue
		}
		seen[e.PartitionID] = struct{}{}
		out.CoveringPartition = append(out.CoveringPartition, e.PartitionID)
	}
	return out
}

// PartitionsOverlapping returns the distinct partition IDs of all
// tiles whose [Range.Lo, Range.Hi] intersect the inclusive query
// range [lo, hi]. The result is ordered by ascending Range.Lo (the
// table's sort order); a partition that owns multiple overlapping
// tiles appears once.
//
// This is the read-path analogue of Lookup for a metric-name query:
// all series of one metric name hash into a contiguous span of the
// 32-bit space (see mimirpb.MetricNameHashRange), and that span may
// straddle more than one tile, so a single query can resolve to
// several partitions.
func (t *ActiveTable) PartitionsOverlapping(lo, hi uint32) []int32 {
	return t.PartitionsOverlappingForTenant("", lo, hi)
}

// PartitionsOverlappingForTenant is the tenant-scoped form of
// PartitionsOverlapping.
func (t *ActiveTable) PartitionsOverlappingForTenant(tenantID string, lo, hi uint32) []int32 {
	bounds, ok := t.tenants[tenantID]
	if !ok || hi < lo {
		return nil
	}
	// Find the first tile whose Range.Hi >= lo; earlier tiles end
	// strictly before the query range and cannot overlap.
	start := bounds.start + sort.Search(bounds.end-bounds.start, func(i int) bool {
		return t.entries[bounds.start+i].Range.Hi >= lo
	})

	var (
		out  []int32
		seen map[int32]struct{}
	)
	for i := start; i < bounds.end; i++ {
		e := &t.entries[i]
		// Tiles are sorted by Range.Lo, so once a tile begins past
		// the query range nothing later can overlap.
		if e.Range.Lo > hi {
			break
		}
		if seen == nil {
			out = append(out, e.PartitionID)
			seen = map[int32]struct{}{e.PartitionID: {}}
			continue
		}
		if _, ok := seen[e.PartitionID]; ok {
			continue
		}
		seen[e.PartitionID] = struct{}{}
		out = append(out, e.PartitionID)
	}
	return out
}

// AllPartitions returns the distinct partition IDs across every tile
// in the table, ordered by ascending Range.Lo (a partition owning
// multiple tiles appears once). It is the full-fanout case of a read
// query that cannot be narrowed to a metric-name hash range.
func (t *ActiveTable) AllPartitions() []int32 {
	return t.AllPartitionsForTenant("")
}

// AllPartitionsForTenant is the tenant-scoped form of AllPartitions.
func (t *ActiveTable) AllPartitionsForTenant(tenantID string) []int32 {
	bounds, ok := t.tenants[tenantID]
	if !ok {
		return nil
	}
	out := make([]int32, 0, bounds.end-bounds.start)
	seen := make(map[int32]struct{}, bounds.end-bounds.start)
	for i := bounds.start; i < bounds.end; i++ {
		pid := t.entries[i].PartitionID
		if _, ok := seen[pid]; ok {
			continue
		}
		seen[pid] = struct{}{}
		out = append(out, pid)
	}
	return out
}

// ValidUntil returns the soonest expiration across all entries in
// the legacy empty tenant. After this wall-clock the table is stale.
func (t *ActiveTable) ValidUntil() time.Time { return t.ValidUntilForTenant("") }

// ValidUntilForTenant returns the soonest expiration across tenantID's entries.
func (t *ActiveTable) ValidUntilForTenant(tenantID string) time.Time {
	return t.tenants[tenantID].validUntil
}

// BuiltAt returns the wall-clock the table was built for.
func (t *ActiveTable) BuiltAt() time.Time { return t.builtAt }

// Len returns the number of legacy empty-tenant tiles in the table.
func (t *ActiveTable) Len() int { return t.LenForTenant("") }

// LenForTenant returns the number of tenantID's tiles in the table.
func (t *ActiveTable) LenForTenant(tenantID string) int {
	bounds := t.tenants[tenantID]
	return bounds.end - bounds.start
}

// CoversAt reports whether the table is safe to use for wall-clock
// at: built at or before at, and not yet stale.
func (t *ActiveTable) CoversAt(at time.Time) bool {
	return t.CoversTenantAt("", at)
}

// CoversTenantAt reports whether tenantID's table is safe to use at.
func (t *ActiveTable) CoversTenantAt(tenantID string, at time.Time) bool {
	bounds, ok := t.tenants[tenantID]
	return ok && !at.Before(t.builtAt) && (bounds.validUntil.IsZero() || at.Before(bounds.validUntil))
}
