// SPDX-License-Identifier: AGPL-3.0-only

package assignment

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"sort"
)

// HashRange represents a contiguous range [Lo, Hi] in the 32-bit hash space.
type HashRange struct {
	Lo uint32 `json:"lo"`
	Hi uint32 `json:"hi"`
}

// Contains returns true if key falls within [Lo, Hi].
func (r HashRange) Contains(key uint32) bool {
	return key >= r.Lo && key <= r.Hi
}

// Overlaps reports whether this range intersects the inclusive hash
// range [lo, hi]. Both ranges are inclusive on both ends, so they
// overlap iff each starts at or before the other ends.
func (r HashRange) Overlaps(lo, hi uint32) bool {
	return r.Lo <= hi && r.Hi >= lo
}

// Size returns the number of hash values covered by this range.
func (r HashRange) Size() uint64 {
	return uint64(r.Hi) - uint64(r.Lo) + 1
}

// Entry maps a tenant's hash range to a partition.
type Entry struct {
	TenantID    string    `json:"tenant_id,omitempty"`
	Range       HashRange `json:"range"`
	PartitionID int32     `json:"partition_id"`
}

// Assignment maps each tenant's contiguous, non-overlapping hash ranges to
// partitions. Every tenant represented in Entries independently covers the
// full 32-bit hash space. Entries are sorted by (TenantID, Range.Lo).
type Assignment struct {
	Entries []Entry `json:"entries"`
}

// Lookup returns the partition ID that owns the given hash key.
// It is the legacy empty-tenant wrapper around LookupForTenant.
func (a *Assignment) Lookup(key uint32) (int32, bool) {
	return a.LookupForTenant("", key)
}

// LookupForTenant returns the partition ID that owns key for tenantID.
// Returns (partitionID, true) on success or (0, false) if the assignment
// has no entries for the tenant or the key falls outside its covered range.
func (a *Assignment) LookupForTenant(tenantID string, key uint32) (int32, bool) {
	if len(a.Entries) == 0 {
		return 0, false
	}

	start, end := assignmentTenantBounds(a.Entries, tenantID)
	if start == end {
		return 0, false
	}

	// Binary search: find the last entry for this tenant whose Lo <= key.
	i := start + sort.Search(end-start, func(i int) bool {
		return a.Entries[start+i].Range.Lo > key
	}) - 1
	if i < start {
		return 0, false
	}
	e := &a.Entries[i]
	if key >= e.Range.Lo && key <= e.Range.Hi {
		return e.PartitionID, true
	}
	return 0, false
}

// Validate checks that entries are sorted by (TenantID, Range.Lo) and that
// every represented tenant independently covers [0, math.MaxUint32] without
// gaps or overlaps.
func (a *Assignment) Validate() error {
	if len(a.Entries) == 0 {
		return fmt.Errorf("assignment has no entries")
	}

	first := &a.Entries[0]
	if first.Range.Lo != 0 {
		return fmt.Errorf("tenant %q first entry must start at 0, got %d", first.TenantID, first.Range.Lo)
	}

	for i := 1; i < len(a.Entries); i++ {
		prev := &a.Entries[i-1]
		curr := &a.Entries[i]
		if curr.TenantID < prev.TenantID {
			return fmt.Errorf("entries are not sorted by tenant at indexes %d and %d: %q before %q", i-1, i, prev.TenantID, curr.TenantID)
		}
		if curr.TenantID != prev.TenantID {
			if prev.Range.Hi != math.MaxUint32 {
				return fmt.Errorf("tenant %q last entry must end at %d, got %d", prev.TenantID, uint32(math.MaxUint32), prev.Range.Hi)
			}
			if curr.Range.Lo != 0 {
				return fmt.Errorf("tenant %q first entry must start at 0, got %d", curr.TenantID, curr.Range.Lo)
			}
			continue
		}
		if uint64(curr.Range.Lo) != uint64(prev.Range.Hi)+1 {
			return fmt.Errorf("tenant %q has gap or overlap between entries %d and %d: prev.Hi=%d, curr.Lo=%d", curr.TenantID, i-1, i, prev.Range.Hi, curr.Range.Lo)
		}
	}

	last := &a.Entries[len(a.Entries)-1]
	if last.Range.Hi != math.MaxUint32 {
		return fmt.Errorf("tenant %q last entry must end at %d, got %d", last.TenantID, uint32(math.MaxUint32), last.Range.Hi)
	}
	return nil
}

// EvenSplit creates an assignment that divides the full 32-bit hash space
// evenly across the given partition IDs for the legacy empty tenant.
func EvenSplit(partitionIDs []int32) *Assignment {
	return EvenSplitForTenant("", partitionIDs)
}

// EvenSplitForTenant creates an assignment that divides tenantID's full
// 32-bit hash space evenly across the given partition IDs.
func EvenSplitForTenant(tenantID string, partitionIDs []int32) *Assignment {
	n := len(partitionIDs)
	if n == 0 {
		return &Assignment{}
	}

	entries := make([]Entry, n)
	rangeSize := (uint64(math.MaxUint32) + 1) / uint64(n)

	for i := 0; i < n; i++ {
		lo := uint64(i) * rangeSize
		hi := lo + rangeSize - 1
		if i == n-1 {
			hi = uint64(math.MaxUint32)
		}
		entries[i] = Entry{
			TenantID:    tenantID,
			Range:       HashRange{Lo: uint32(lo), Hi: uint32(hi)},
			PartitionID: partitionIDs[i],
		}
	}

	return &Assignment{Entries: entries}
}

// FineEvenSplit creates an assignment like EvenSplit but with
// slicesPerPartition sub-ranges per partition. This provides the
// granularity needed by the slicer algorithm to make moves on the
// first rebalance round for the legacy empty tenant.
func FineEvenSplit(partitionIDs []int32, slicesPerPartition int) *Assignment {
	return FineEvenSplitForTenant("", partitionIDs, slicesPerPartition)
}

// FineEvenSplitForTenant creates an assignment like EvenSplitForTenant but
// with slicesPerPartition sub-ranges per partition.
func FineEvenSplitForTenant(tenantID string, partitionIDs []int32, slicesPerPartition int) *Assignment {
	n := len(partitionIDs)
	if n == 0 {
		return &Assignment{}
	}
	if slicesPerPartition < 1 {
		slicesPerPartition = 1
	}

	totalSlices := n * slicesPerPartition
	entries := make([]Entry, 0, totalSlices)
	rangeSize := (uint64(math.MaxUint32) + 1) / uint64(totalSlices)

	for i := 0; i < totalSlices; i++ {
		lo := uint64(i) * rangeSize
		hi := lo + rangeSize - 1
		if i == totalSlices-1 {
			hi = uint64(math.MaxUint32)
		}
		entries = append(entries, Entry{
			TenantID:    tenantID,
			Range:       HashRange{Lo: uint32(lo), Hi: uint32(hi)},
			PartitionID: partitionIDs[i/slicesPerPartition],
		})
	}

	return &Assignment{Entries: entries}
}

// Load reads a JSON-encoded assignment from r. This function signature
// is compatible with dskit's runtimeconfig.Loader.
func Load(r io.Reader) (interface{}, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("reading assignment: %w", err)
	}

	var a Assignment
	if err := json.Unmarshal(data, &a); err != nil {
		return nil, fmt.Errorf("parsing assignment JSON: %w", err)
	}

	if err := a.Validate(); err != nil {
		return nil, fmt.Errorf("invalid assignment: %w", err)
	}

	return &a, nil
}

// MarshalJSON marshals the assignment to JSON.
func (a *Assignment) MarshalJSON() ([]byte, error) {
	type alias Assignment
	return json.Marshal((*alias)(a))
}

// WriteJSON writes the assignment as formatted JSON to w.
func (a *Assignment) WriteJSON(w io.Writer) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(a)
}

func assignmentTenantBounds(entries []Entry, tenantID string) (int, int) {
	start := sort.Search(len(entries), func(i int) bool {
		return entries[i].TenantID >= tenantID
	})
	end := start + sort.Search(len(entries)-start, func(i int) bool {
		return entries[start+i].TenantID > tenantID
	})
	return start, end
}
