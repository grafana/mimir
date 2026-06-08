// SPDX-License-Identifier: AGPL-3.0-only

package assignment

import (
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
// disjoint, non-overlapping portion of the 32-bit hash space; if
// the source Log was driven by valid Apply calls the entries also
// tile the full space contiguously. Entries are sorted ascending by
// Range.Lo so binary search by Range.Hi resolves the unique
// containing tile.
//
// ValidUntil reports the soonest expiration of any included entry:
// after that wall-clock instant, at least one tile is no longer
// active and the table no longer reflects the live tiling. The
// distributor uses ValidUntil to decide when to rebuild from the
// underlying Log (which may also contain a pre-issued successor
// that became active in the meantime).
type ActiveTable struct {
	// entries is the list of entries from the source Log that were
	// active at the wall-clock passed to NewActiveTable, sorted by
	// Range.Lo.
	entries []LogEntry

	// builtAt is the `at` argument passed to NewActiveTable. Lookups
	// for `at` strictly less than builtAt may return entries whose
	// leases hadn't started yet, so callers should rebuild for
	// significantly earlier `at` values.
	builtAt time.Time

	// validUntil is the minimum To across the entries. Once
	// wall-clock crosses this, at least one tile in the table is no
	// longer active.
	validUntil time.Time
}

// ActiveTable returns a new ActiveTable for the entries in l whose
// leases cover wall-clock at, or nil if no entries are active at
// at. The returned table holds its own slice and is safe to use
// concurrently with further mutations of l (including via Apply).
func (l *Log) ActiveTable(at time.Time) *ActiveTable {
	// Avoid allocating ActiveAt's slice; we'll do our own pass.
	count := 0
	for _, e := range l.entries {
		if e.ActiveAt(at) {
			count++
		}
	}
	if count == 0 {
		return nil
	}

	entries := make([]LogEntry, 0, count)
	var validUntil time.Time
	for _, e := range l.entries {
		if !e.ActiveAt(at) {
			continue
		}
		entries = append(entries, e)
		if validUntil.IsZero() || e.To.Before(validUntil) {
			validUntil = e.To
		}
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Range.Lo < entries[j].Range.Lo
	})
	return &ActiveTable{
		entries:    entries,
		builtAt:    at,
		validUntil: validUntil,
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
	// Find the first tile whose Range.Hi >= key. Because tiles in
	// the table are non-overlapping and sorted by Range.Lo, that
	// tile (if it exists) is the unique candidate.
	idx := sort.Search(len(t.entries), func(i int) bool {
		return t.entries[i].Range.Hi >= key
	})
	if idx >= len(t.entries) {
		return 0, false
	}
	e := &t.entries[idx]
	if key < e.Range.Lo {
		// Hash gap (the source Log's active tiling didn't cover key).
		return 0, false
	}
	return e.PartitionID, true
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
	if len(t.entries) == 0 || hi < lo {
		return nil
	}
	// Find the first tile whose Range.Hi >= lo; earlier tiles end
	// strictly before the query range and cannot overlap.
	start := sort.Search(len(t.entries), func(i int) bool {
		return t.entries[i].Range.Hi >= lo
	})

	var (
		out  []int32
		seen map[int32]struct{}
	)
	for i := start; i < len(t.entries); i++ {
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
	if len(t.entries) == 0 {
		return nil
	}
	out := make([]int32, 0, len(t.entries))
	seen := make(map[int32]struct{}, len(t.entries))
	for i := range t.entries {
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
// the table. After this wall-clock the table is stale.
func (t *ActiveTable) ValidUntil() time.Time { return t.validUntil }

// BuiltAt returns the wall-clock the table was built for.
func (t *ActiveTable) BuiltAt() time.Time { return t.builtAt }

// Len returns the number of tiles in the table.
func (t *ActiveTable) Len() int { return len(t.entries) }

// CoversAt reports whether the table is safe to use for wall-clock
// at: built at or before at, and not yet stale.
func (t *ActiveTable) CoversAt(at time.Time) bool {
	return !at.Before(t.builtAt) && at.Before(t.validUntil)
}
