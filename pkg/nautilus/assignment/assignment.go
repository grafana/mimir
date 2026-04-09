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

// Size returns the number of hash values covered by this range.
func (r HashRange) Size() uint64 {
	return uint64(r.Hi) - uint64(r.Lo) + 1
}

// Entry maps a hash range to a partition.
type Entry struct {
	Range       HashRange `json:"range"`
	PartitionID int32     `json:"partition_id"`
}

// Assignment maps contiguous, non-overlapping hash ranges to partitions,
// covering the full 32-bit hash space. Entries are sorted by Range.Lo
// to enable binary search lookup.
type Assignment struct {
	Entries []Entry `json:"entries"`
}

// Lookup returns the partition ID that owns the given hash key.
// Returns (partitionID, true) on success or (0, false) if the assignment
// is empty or the key somehow falls outside the covered range.
func (a *Assignment) Lookup(key uint32) (int32, bool) {
	if len(a.Entries) == 0 {
		return 0, false
	}

	// Binary search: find the last entry whose Lo <= key.
	i := sort.Search(len(a.Entries), func(i int) bool {
		return a.Entries[i].Range.Lo > key
	}) - 1

	if i < 0 {
		return 0, false
	}

	e := &a.Entries[i]
	if key >= e.Range.Lo && key <= e.Range.Hi {
		return e.PartitionID, true
	}
	return 0, false
}

// Validate checks that entries are sorted, non-overlapping, and cover the
// full 32-bit hash space [0, math.MaxUint32].
func (a *Assignment) Validate() error {
	if len(a.Entries) == 0 {
		return fmt.Errorf("assignment has no entries")
	}

	if a.Entries[0].Range.Lo != 0 {
		return fmt.Errorf("first entry must start at 0, got %d", a.Entries[0].Range.Lo)
	}

	if a.Entries[len(a.Entries)-1].Range.Hi != math.MaxUint32 {
		return fmt.Errorf("last entry must end at %d, got %d", uint32(math.MaxUint32), a.Entries[len(a.Entries)-1].Range.Hi)
	}

	for i := 1; i < len(a.Entries); i++ {
		prev := &a.Entries[i-1]
		curr := &a.Entries[i]
		if curr.Range.Lo != prev.Range.Hi+1 {
			return fmt.Errorf("gap or overlap between entries %d and %d: prev.Hi=%d, curr.Lo=%d", i-1, i, prev.Range.Hi, curr.Range.Lo)
		}
	}

	return nil
}

// EvenSplit creates an assignment that divides the full 32-bit hash space
// evenly across the given partition IDs.
func EvenSplit(partitionIDs []int32) *Assignment {
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
			Range:       HashRange{Lo: uint32(lo), Hi: uint32(hi)},
			PartitionID: partitionIDs[i],
		}
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
