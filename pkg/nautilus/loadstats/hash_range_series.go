// SPDX-License-Identifier: AGPL-3.0-only

package loadstats

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
)

// RangeSeries tracks per-hash-range active-series counts for the hash
// ranges this instance has been told it owns (via SetRanges).
//
// Counts are not maintained incrementally on the write path. Instead
// the producer periodically walks the TSDB head of each tenant,
// tallies the cached secondary hashes per owned range, and pushes the
// result via SetCountsFor. This avoids the atomic-op hot path on every
// series creation/deletion, avoids the 512 KB fixed histogram, and
// self-corrects any accumulated drift from missed/duplicated lifecycle
// callbacks.
//
// The rebalancer fetches counts via Snapshot (through the
// HashRangeStats RPC) and uses them to pick which owned range to move
// off of a hot source partition.
type RangeSeries struct {
	mu     sync.RWMutex
	ranges []assignment.HashRange // sorted by Lo, non-overlapping
	counts []int64                // parallel to ranges
}

// NewRangeSeries returns an empty RangeSeries with no owned ranges.
func NewRangeSeries() *RangeSeries {
	return &RangeSeries{}
}

// SetRanges replaces the current set of owned hash ranges. Counts are
// reset to zero — the next walk will repopulate them. Until that walk
// completes, Snapshot reports zeros for every range.
func (h *RangeSeries) SetRanges(ranges []assignment.HashRange) {
	sorted := make([]assignment.HashRange, len(ranges))
	copy(sorted, ranges)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Lo < sorted[j].Lo })

	h.mu.Lock()
	defer h.mu.Unlock()

	h.ranges = sorted
	h.counts = make([]int64, len(sorted))
}

// SetCountsFor atomically replaces the per-range counts. The provided
// forRanges slice must equal the currently-configured ranges (same
// length, same Lo/Hi at each position); otherwise the update is
// discarded and false is returned. This guards against a walk whose
// input ranges went stale mid-walk because SetRanges fired; a
// subsequent walk will refresh.
func (h *RangeSeries) SetCountsFor(forRanges []assignment.HashRange, counts []int64) bool {
	if len(forRanges) != len(counts) {
		return false
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	if len(forRanges) != len(h.ranges) {
		return false
	}
	for i := range forRanges {
		if forRanges[i] != h.ranges[i] {
			return false
		}
	}

	newCounts := make([]int64, len(counts))
	copy(newCounts, counts)
	h.counts = newCounts
	return true
}

// RangeSeriesSnapshot holds a consistent view of owned ranges and
// their counts.
type RangeSeriesSnapshot struct {
	Ranges []assignment.HashRange
	Counts []int64
}

// Snapshot returns the current owned ranges and their per-range active-
// series counts. Returned slices are copies safe for the caller to
// retain and mutate.
func (h *RangeSeries) Snapshot() RangeSeriesSnapshot {
	h.mu.RLock()
	defer h.mu.RUnlock()

	snap := RangeSeriesSnapshot{
		Ranges: make([]assignment.HashRange, len(h.ranges)),
		Counts: make([]int64, len(h.counts)),
	}
	copy(snap.Ranges, h.ranges)
	copy(snap.Counts, h.counts)
	return snap
}

// HasRanges returns true if any owned ranges are configured.
func (h *RangeSeries) HasRanges() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.ranges) > 0
}

// Ranges returns a copy of the currently-configured owned ranges, in
// the canonical sorted order established by SetRanges.
func (h *RangeSeries) Ranges() []assignment.HashRange {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make([]assignment.HashRange, len(h.ranges))
	copy(out, h.ranges)
	return out
}

// LogSummary logs a one-line summary of all owned hash ranges with
// their active-series counts.
func (h *RangeSeries) LogSummary(logger log.Logger) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.ranges) == 0 {
		return
	}

	var total int64
	lines := make([]string, len(h.ranges))
	for i, r := range h.ranges {
		total += h.counts[i]
		lines[i] = fmt.Sprintf("[%08x-%08x]=%d-series", r.Lo, r.Hi, h.counts[i])
	}

	level.Info(logger).Log(
		"msg", "hash range series summary",
		"num_ranges", len(h.ranges),
		"total_active_series", total,
		"ranges", strings.Join(lines, " "),
	)
}

// CountSeriesByHashRange walks all series in head using the secondary
// hash cached on each TSDB series, looks up which of the provided
// sorted, non-overlapping ranges (if any) contains it, and increments
// counts at that index. The head must have been opened with a
// SecondaryHashFunction that computes the metric-name-locality hash
// for the head's tenant. counts must be the same length as ranges and
// is mutated in place.
//
// If examples is non-nil it must be the same length as ranges. The
// labels of the FIRST series found in each range are written via
// labels.Labels.String() into the matching slot; subsequent series in
// the same range leave the slot untouched. Slots whose range never
// matched any series are left as the zero value. Callers that only
// want counts should pass nil. labels.String() materialises fresh
// Go strings, so the captured examples are safe to retain across
// walks and TSDB head compactions.
//
// Returns the number of series it observed (regardless of whether they
// landed in an owned range).
func CountSeriesByHashRange(ctx context.Context, head *tsdb.Head, ranges []assignment.HashRange, counts []int64, examples []string) (int64, error) {
	wantExamples := examples != nil && len(examples) == len(ranges)
	exampleRefs := make([]chunks.HeadSeriesRef, len(ranges))
	exampleRefsSet := make([]bool, len(ranges))
	var walked int64
	var walkErr error

	head.ForEachSecondaryHash(func(refs []chunks.HeadSeriesRef, hashes []uint32) {
		if walkErr != nil {
			return
		}
		if err := ctx.Err(); err != nil {
			walkErr = err
			return
		}
		for i, hash := range hashes {
			walked++

			// Binary search: find largest i such that ranges[i].Lo <= hash.
			ri := sort.Search(len(ranges), func(i int) bool {
				return ranges[i].Lo > hash
			}) - 1
			if ri >= 0 && ranges[ri].Contains(hash) {
				counts[ri]++
				if wantExamples && examples[ri] == "" && !exampleRefsSet[ri] {
					exampleRefs[ri] = refs[i]
					exampleRefsSet[ri] = true
				}
			}
		}
	})
	if walkErr != nil {
		return walked, walkErr
	}

	if !wantExamples {
		return walked, nil
	}

	if err := loadSeriesExamples(ctx, head, exampleRefs, exampleRefsSet, examples); err != nil {
		return walked, err
	}
	return walked, nil
}

func loadSeriesExamples(ctx context.Context, head *tsdb.Head, exampleRefs []chunks.HeadSeriesRef, exampleRefsSet []bool, examples []string) error {
	idx, err := head.Index()
	if err != nil {
		return err
	}
	defer idx.Close()

	builder := labels.NewScratchBuilder(16)
	for i, ref := range exampleRefs {
		if !exampleRefsSet[i] || examples[i] != "" {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		builder.Reset()
		if err := idx.Series(storage.SeriesRef(ref), &builder, nil); err != nil {
			// Series may be deleted between the secondary-hash walk and
			// this bounded example lookup; treat it as absent.
			if errors.Is(err, storage.ErrNotFound) {
				continue
			}
			return err
		}
		// labels.Labels.String() allocates a fresh Go string, detaching
		// the example from the TSDB intern pool. At most one lookup and
		// allocation is performed per range.
		examples[i] = builder.Labels().String()
	}
	return nil
}
