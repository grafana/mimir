// SPDX-License-Identifier: AGPL-3.0-only

package readcacheassignment

import (
	"fmt"
	"testing"
	"time"
)

// BenchmarkLog_Apply_DevSized mirrors the tier-2 hot path that
// contributed to the rebalancer single-core saturation on mimir-
// dev-15. Same shape and motivation as the tier-1 benchmark in
// pkg/nautilus/assignment/log_bench_test.go; this benchmark covers
// the (partition -> readcache instance) log instead of the
// (range -> partition) log.
//
// dev-15 ran 300 partitions × 22 readcache instances with 24h
// retention, so the live log peaked around 60K entries (we saw
// `readcache_log_entries=60284` in production warnings). The
// production warm-log_50x case here represents that shape with
// extra historical revisions.
//
// Run with:
//
//	go test -run=^$ -bench=BenchmarkLog_Apply -benchmem ./pkg/nautilus/readcacheassignment/
func BenchmarkLog_Apply_DevSized(b *testing.B) {
	b.Run("cold_log/300p_1owner", func(b *testing.B) {
		benchApply(b, 300, 0)
	})
	b.Run("warm_log_5x/300p_1owner", func(b *testing.B) {
		benchApply(b, 300, 5)
	})
	b.Run("warm_log_50x/300p_1owner", func(b *testing.B) {
		benchApply(b, 300, 50)
	})
}

// benchApply seeds a Log with numPartitions live (partition, instance)
// pairs plus numHistorical expired revisions per chain, then times
// Apply()s of the same steady-state assignment. Each iteration
// works against a fresh copy of the baseline so b.N runs don't
// inflate the working set artificially.
func benchApply(b *testing.B, numPartitions, numHistorical int) {
	b.Helper()

	desired := &Assignment{
		Entries: make([]AssignmentEntry, numPartitions),
	}
	for i := 0; i < numPartitions; i++ {
		desired.Entries[i] = AssignmentEntry{
			PartitionID: int32(i),
			// Single-owner mode: spread partitions across 22
			// fake readcache instances round-robin, matching the
			// dev-15 fleet size.
			InstanceID: fmt.Sprintf("rc-%d", i%22),
		}
	}

	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	lease := 5 * time.Minute
	lookahead := 90 * time.Second

	baseline := NewLog()
	baseline.Apply(t0, desired, lease, lookahead)

	if numHistorical > 0 {
		historical := make([]LogEntry, 0, numHistorical*len(desired.Entries))
		gap := time.Minute
		for h := 1; h <= numHistorical; h++ {
			expiredTo := t0.Add(-time.Duration(h) * (lease + gap))
			expiredFrom := expiredTo.Add(-lease)
			for _, e := range desired.Entries {
				historical = append(historical, LogEntry{
					PartitionID: e.PartitionID,
					InstanceID:  e.InstanceID,
					From:        expiredFrom,
					To:          expiredTo,
				})
			}
		}
		baseline.entries = append(baseline.entries, historical...)
		sortEntries(baseline.entries)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		l := &Log{entries: append([]LogEntry(nil), baseline.entries...)}
		at := t0.Add(time.Duration(i+1) * lookahead)
		b.StartTimer()
		l.Apply(at, desired, lease, lookahead)
	}
}
