// SPDX-License-Identifier: AGPL-3.0-only

package assignment

import (
	"testing"
	"time"
)

// BenchmarkLog_Apply_DevSized mirrors the production hot path that
// pegged a single rebalancer core on mimir-dev-15 (300 partitions ×
// 64 slices = 19,200 tile entries, ~100K live log entries from 24h
// of retention). A pprof on the live pod showed 98.7% of CPU in
// (*Log).latestTo, called O(N) times per Apply, each O(M) — total
// O(N*M) ≈ 1.9 billion comparisons per round.
//
// Run with:
//
//	go test -run=^$ -bench=BenchmarkLog_Apply -benchmem ./pkg/nautilus/assignment/
//
// Before the latest-To pre-index, a single Apply call took roughly
// 10s of seconds on a developer laptop (and pegged a single core
// in production). After, it should be in the low-double-digit ms.
// The benchmark exists so that regressions (e.g. someone reverting
// the index or adding a new O(M) lookup inside Apply) jump out in
// CI before they reach a deploy.
func BenchmarkLog_Apply_DevSized(b *testing.B) {
	b.Run("cold_log/300p_64s", func(b *testing.B) {
		benchApply(b, 300, 64, 0)
	})
	b.Run("warm_log_5x/300p_64s", func(b *testing.B) {
		// 5 historical revisions per (range, pid). At
		// LeaseDuration=5m and EntryRetention=24h that's roughly
		// what a couple of hours of healthy operation looks like:
		// fresh leases plus a backlog of expired entries waiting
		// for the next Prune.
		benchApply(b, 300, 64, 5)
	})
	b.Run("warm_log_50x/300p_64s", func(b *testing.B) {
		// 50 historical revisions per chain — the dev-15 shape
		// after the cold-start collapse loop ran for hours and
		// EntryRetention hadn't pruned them yet.
		benchApply(b, 300, 64, 50)
	})
}

// benchApply runs b.N Apply() calls against a log seeded with
// `numHistorical` extra expired entries per (range, pid) chain.
// Each iteration applies the same FineEvenSplit at a fresh `at`,
// simulating a steady-state round where the desired tiling matches
// the current active one (so the second pass is the dominant cost
// — the case that exposes the latestTo hot loop).
func benchApply(b *testing.B, numPartitions, slicesPerPartition, numHistorical int) {
	b.Helper()

	partitions := make([]int32, numPartitions)
	for i := range partitions {
		partitions[i] = int32(i)
	}
	desired := FineEvenSplit(partitions, slicesPerPartition)

	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	lease := 5 * time.Minute
	lookahead := 90 * time.Second

	// Pre-build a baseline log with one fresh lease per (range,
	// pid) plus `numHistorical` expired predecessors. This mirrors
	// the production shape where EntryRetention=24h holds onto
	// expired entries until they're pruned.
	baseline := NewLog()
	baseline.Apply(t0, desired, lease, lookahead)
	if numHistorical > 0 {
		historical := make([]LogEntry, 0, numHistorical*len(desired.Entries))
		// Stack expired predecessors from oldest to newest just
		// before t0, with non-overlapping [From, To) windows so
		// invariants stay intact.
		gap := time.Minute
		for h := 1; h <= numHistorical; h++ {
			expiredTo := t0.Add(-time.Duration(h) * (lease + gap))
			expiredFrom := expiredTo.Add(-lease)
			for _, e := range desired.Entries {
				historical = append(historical, LogEntry{
					Range:       e.Range,
					PartitionID: e.PartitionID,
					From:        expiredFrom,
					To:          expiredTo,
				})
			}
		}
		// Splice the historical entries into the baseline. Order
		// doesn't matter for correctness; Apply re-sorts.
		baseline.entries = append(baseline.entries, historical...)
		sortEntries(baseline.entries)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Each iteration starts from a fresh copy of the baseline
		// so the b.N runs measure steady-state Apply, not an
		// ever-growing log.
		b.StopTimer()
		l := &Log{entries: append([]LogEntry(nil), baseline.entries...)}
		at := t0.Add(time.Duration(i+1) * lookahead) // advance enough to require successors
		b.StartTimer()
		l.Apply(at, desired, lease, lookahead)
	}
}
