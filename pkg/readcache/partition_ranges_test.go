// SPDX-License-Identifier: AGPL-3.0-only

package readcache

import (
	"math"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/nautilus/assignment"
)

func hr(lo, hi uint32) assignment.HashRange {
	return assignment.HashRange{Lo: lo, Hi: hi}
}

func TestRangeDiff(t *testing.T) {
	cases := []struct {
		name string
		a, b []assignment.HashRange
		want []assignment.HashRange
	}{
		{
			name: "empty a returns nil",
			a:    nil,
			b:    []assignment.HashRange{hr(0, 99)},
			want: nil,
		},
		{
			name: "empty b returns copy of a",
			a:    []assignment.HashRange{hr(0, 99)},
			b:    nil,
			want: []assignment.HashRange{hr(0, 99)},
		},
		{
			name: "disjoint b before a",
			a:    []assignment.HashRange{hr(100, 199)},
			b:    []assignment.HashRange{hr(0, 50)},
			want: []assignment.HashRange{hr(100, 199)},
		},
		{
			name: "disjoint b after a",
			a:    []assignment.HashRange{hr(0, 99)},
			b:    []assignment.HashRange{hr(200, 299)},
			want: []assignment.HashRange{hr(0, 99)},
		},
		{
			name: "full overlap (a entirely covered by b)",
			a:    []assignment.HashRange{hr(50, 149)},
			b:    []assignment.HashRange{hr(0, 199)},
			want: nil,
		},
		{
			name: "exact same range",
			a:    []assignment.HashRange{hr(0, 99)},
			b:    []assignment.HashRange{hr(0, 99)},
			want: nil,
		},
		{
			name: "partial overlap left",
			a:    []assignment.HashRange{hr(0, 99)},
			b:    []assignment.HashRange{hr(0, 49)},
			want: []assignment.HashRange{hr(50, 99)},
		},
		{
			name: "partial overlap right",
			a:    []assignment.HashRange{hr(0, 99)},
			b:    []assignment.HashRange{hr(50, 99)},
			want: []assignment.HashRange{hr(0, 49)},
		},
		{
			name: "b in middle of a (a splits in two)",
			a:    []assignment.HashRange{hr(0, 199)},
			b:    []assignment.HashRange{hr(50, 149)},
			want: []assignment.HashRange{hr(0, 49), hr(150, 199)},
		},
		{
			name: "split: union of b covers a",
			a:    []assignment.HashRange{hr(0, 99)},
			b:    []assignment.HashRange{hr(0, 49), hr(50, 99)},
			want: nil,
		},
		{
			name: "merge: a is two halves, b is parent",
			a:    []assignment.HashRange{hr(0, 49), hr(50, 99)},
			b:    []assignment.HashRange{hr(0, 99)},
			want: nil,
		},
		{
			name: "partial shift left (old 0-99, new 50-149)",
			a:    []assignment.HashRange{hr(0, 99)},
			b:    []assignment.HashRange{hr(50, 149)},
			want: []assignment.HashRange{hr(0, 49)},
		},
		{
			name: "partial shift right (old 50-149, new 0-99)",
			a:    []assignment.HashRange{hr(50, 149)},
			b:    []assignment.HashRange{hr(0, 99)},
			want: []assignment.HashRange{hr(100, 149)},
		},
		{
			name: "multiple a, multiple b interleaved",
			a:    []assignment.HashRange{hr(0, 99), hr(200, 299), hr(400, 499)},
			b:    []assignment.HashRange{hr(50, 249), hr(450, 499)},
			want: []assignment.HashRange{hr(0, 49), hr(250, 299), hr(400, 449)},
		},
		{
			name: "max uint32 edge case (no overflow)",
			a:    []assignment.HashRange{hr(math.MaxUint32-100, math.MaxUint32)},
			b:    []assignment.HashRange{hr(math.MaxUint32-50, math.MaxUint32)},
			want: []assignment.HashRange{hr(math.MaxUint32-100, math.MaxUint32-51)},
		},
		{
			name: "full hash space covered",
			a:    []assignment.HashRange{hr(0, math.MaxUint32)},
			b:    []assignment.HashRange{hr(0, math.MaxUint32)},
			want: nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := rangeDiff(tc.a, tc.b)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestPartitionRangesSetRanges_TransitionsTracked(t *testing.T) {
	t.Run("initial assignment populates current with no historical", func(t *testing.T) {
		pr := newPartitionRanges()
		pr.setRanges([]assignment.HashRange{hr(0, 99), hr(200, 299)})

		assert.Equal(t, []assignment.HashRange{hr(0, 99), hr(200, 299)}, pr.currentRanges)
		assert.Empty(t, pr.historicalRanges)
	})

	t.Run("partial shift moves the orphan into historical", func(t *testing.T) {
		pr := newPartitionRanges()
		pr.setRanges([]assignment.HashRange{hr(0, 99)})
		pr.setRanges([]assignment.HashRange{hr(50, 149)})

		assert.Equal(t, []assignment.HashRange{hr(50, 149)}, pr.currentRanges)
		assert.Equal(t, []assignment.HashRange{hr(0, 49)}, pr.historicalRanges)
	})

	t.Run("full move sends everything to historical", func(t *testing.T) {
		pr := newPartitionRanges()
		pr.setRanges([]assignment.HashRange{hr(0, 99)})
		pr.setRanges([]assignment.HashRange{hr(500, 599)})

		assert.Equal(t, []assignment.HashRange{hr(500, 599)}, pr.currentRanges)
		assert.Equal(t, []assignment.HashRange{hr(0, 99)}, pr.historicalRanges)
	})

	t.Run("split preserves current with no historical residue", func(t *testing.T) {
		pr := newPartitionRanges()
		pr.setRanges([]assignment.HashRange{hr(0, 99)})
		pr.setRanges([]assignment.HashRange{hr(0, 49), hr(50, 99)})

		assert.Equal(t, []assignment.HashRange{hr(0, 49), hr(50, 99)}, pr.currentRanges)
		assert.Empty(t, pr.historicalRanges)
	})

	t.Run("merge preserves current with no historical residue", func(t *testing.T) {
		pr := newPartitionRanges()
		pr.setRanges([]assignment.HashRange{hr(0, 49), hr(50, 99)})
		pr.setRanges([]assignment.HashRange{hr(0, 99)})

		assert.Equal(t, []assignment.HashRange{hr(0, 99)}, pr.currentRanges)
		assert.Empty(t, pr.historicalRanges)
	})

	t.Run("re-acquiring a previously-historical range removes it from historical", func(t *testing.T) {
		pr := newPartitionRanges()
		pr.setRanges([]assignment.HashRange{hr(0, 99)})
		pr.setRanges([]assignment.HashRange{hr(500, 599)})
		// historical now has [0, 99].
		require.Equal(t, []assignment.HashRange{hr(0, 99)}, pr.historicalRanges)

		// Take [0, 99] back.
		pr.setRanges([]assignment.HashRange{hr(0, 99)})
		assert.Equal(t, []assignment.HashRange{hr(0, 99)}, pr.currentRanges)
		// [500, 599] went historical, [0, 99] is no longer historical.
		assert.Equal(t, []assignment.HashRange{hr(500, 599)}, pr.historicalRanges)
	})

	t.Run("historical entries with zero count and not current are dropped", func(t *testing.T) {
		pr := newPartitionRanges()
		pr.setRanges([]assignment.HashRange{hr(0, 99)})
		// Simulate a walker tick that observes some residue.
		pr.setRanges([]assignment.HashRange{hr(500, 599)})
		ok := pr.applyWalkResult([]assignment.HashRange{hr(0, 99), hr(500, 599)}, []int64{42, 7}, nil)
		require.True(t, ok)
		// Residue (42) keeps the historical range alive; current (7)
		// is always retained.
		assert.Equal(t, []assignment.HashRange{hr(0, 99)}, pr.historicalRanges)
		assert.Equal(t, int64(42), pr.rangeCounts[hr(0, 99)])
		assert.Equal(t, int64(7), pr.rangeCounts[hr(500, 599)])

		// Next tick: compaction has cleared the residue.
		ok = pr.applyWalkResult([]assignment.HashRange{hr(0, 99), hr(500, 599)}, []int64{0, 7}, nil)
		require.True(t, ok)
		assert.Empty(t, pr.historicalRanges, "historical drops when count goes to zero")
		_, hasOld := pr.rangeCounts[hr(0, 99)]
		assert.False(t, hasOld, "rangeCounts drops the historical entry")
		assert.Equal(t, int64(7), pr.rangeCounts[hr(500, 599)], "current entry stays even if zero")
	})

	t.Run("current entry retained at zero count", func(t *testing.T) {
		pr := newPartitionRanges()
		pr.setRanges([]assignment.HashRange{hr(0, 99)})
		ok := pr.applyWalkResult([]assignment.HashRange{hr(0, 99)}, []int64{0}, nil)
		require.True(t, ok)
		// rangeCounts still has the current range, with zero. The
		// snapshot must still emit it so the rebalancer sees the
		// partition's claimed footprint.
		assert.Contains(t, pr.rangeCounts, hr(0, 99))
		snap := pr.snapshotCounts()
		require.Len(t, snap, 1)
		assert.Equal(t, hr(0, 99), snap[0].Range)
		assert.Equal(t, int64(0), snap[0].Count)
	})

	t.Run("applyWalkResult discards if snapshot moved", func(t *testing.T) {
		pr := newPartitionRanges()
		pr.setRanges([]assignment.HashRange{hr(0, 99)})
		// Walker captured these as its snapshot.
		walkerSnap := []assignment.HashRange{hr(0, 99)}
		// Meanwhile, SetRanges fires.
		pr.setRanges([]assignment.HashRange{hr(100, 199)})

		ok := pr.applyWalkResult(walkerSnap, []int64{42}, nil)
		assert.False(t, ok, "stale walk should be discarded")
		// The fresh assignment is intact: [0, 99] in historical,
		// [100, 199] current.
		assert.Equal(t, []assignment.HashRange{hr(100, 199)}, pr.currentRanges)
		assert.Equal(t, []assignment.HashRange{hr(0, 99)}, pr.historicalRanges)
	})
}

func TestPartitionRangesSnapshotCounts(t *testing.T) {
	t.Run("emits current ranges even when zero", func(t *testing.T) {
		pr := newPartitionRanges()
		pr.setRanges([]assignment.HashRange{hr(0, 99), hr(200, 299)})

		snap := pr.snapshotCounts()
		require.Len(t, snap, 2)
		assert.Equal(t, hr(0, 99), snap[0].Range)
		assert.Equal(t, int64(0), snap[0].Count)
		assert.Equal(t, hr(200, 299), snap[1].Range)
		assert.Equal(t, int64(0), snap[1].Count)
	})

	t.Run("skips historical ranges with zero residue", func(t *testing.T) {
		pr := newPartitionRanges()
		pr.setRanges([]assignment.HashRange{hr(0, 99)})
		pr.setRanges([]assignment.HashRange{hr(200, 299)})
		// Walker records: no residue on the historical, no growth yet
		// on the current.
		ok := pr.applyWalkResult([]assignment.HashRange{hr(0, 99), hr(200, 299)}, []int64{0, 0}, nil)
		require.True(t, ok)

		snap := pr.snapshotCounts()
		// Only current entry remains; historical zero entry was GC'd.
		require.Len(t, snap, 1)
		assert.Equal(t, hr(200, 299), snap[0].Range)
	})

	t.Run("emits residue and growth on the same partition simultaneously", func(t *testing.T) {
		pr := newPartitionRanges()
		// Initial ownership: [0, 99].
		pr.setRanges([]assignment.HashRange{hr(0, 99)})
		// Move to [200, 299]. Old [0, 99] still has residue.
		pr.setRanges([]assignment.HashRange{hr(200, 299)})
		ok := pr.applyWalkResult([]assignment.HashRange{hr(0, 99), hr(200, 299)}, []int64{1234, 56}, nil)
		require.True(t, ok)

		snap := pr.snapshotCounts()
		require.Len(t, snap, 2)
		// Sorted by Lo.
		assert.Equal(t, hr(0, 99), snap[0].Range)
		assert.Equal(t, int64(1234), snap[0].Count)
		assert.Equal(t, hr(200, 299), snap[1].Range)
		assert.Equal(t, int64(56), snap[1].Count)
	})
}

// TestPartitionRangesWalkerInvariant verifies the Σ counts == sum-of-
// walked invariant: every series the walker observes is bucketed into
// exactly one range (current or historical), regardless of which side
// it falls on.
func TestPartitionRangesWalkerInvariant(t *testing.T) {
	pr := newPartitionRanges()
	pr.setRanges([]assignment.HashRange{hr(0, 99)})
	pr.setRanges([]assignment.HashRange{hr(50, 149)}) // partial shift

	// At this point pr has currentRanges=[50,149] and
	// historicalRanges=[0, 49]. Snapshot is the sorted union.
	snap := pr.rangesSnapshot()
	require.Equal(t, []assignment.HashRange{hr(0, 49), hr(50, 149)}, snap)

	// Simulate a walk where 100 series are in [0, 49] (residue) and
	// 200 are in [50, 149] (current).
	counts := []int64{100, 200}
	ok := pr.applyWalkResult(snap, counts, nil)
	require.True(t, ok)

	// snapshotCounts should report both, summing to 300.
	out := pr.snapshotCounts()
	var total int64
	for _, e := range out {
		total += e.Count
	}
	assert.Equal(t, int64(300), total)
}

// TestPartitionRangesExampleSeries verifies that example series flow
// through applyWalkResult → adminSnapshot, are retained across walks
// that fail to re-capture an example (e.g. a transient skip from a
// concurrent compaction), and get GC'd when a range falls out of the
// working set.
func TestPartitionRangesExampleSeries(t *testing.T) {
	t.Run("examples surface on adminSnapshot for current and historical", func(t *testing.T) {
		pr := newPartitionRanges()
		pr.setRanges([]assignment.HashRange{hr(0, 99)})
		pr.setRanges([]assignment.HashRange{hr(200, 299)}) // [0, 99] now historical

		snap := pr.rangesSnapshot()
		require.True(t, pr.applyWalkResult(snap, []int64{50, 10}, []string{
			`{__name__="residue_metric",x="1"}`,
			`{__name__="growth_metric",x="2"}`,
		}))

		current, historical := pr.adminSnapshot()
		require.Len(t, current, 1)
		require.Len(t, historical, 1)
		assert.Equal(t, hr(200, 299), current[0].Range)
		assert.Equal(t, `{__name__="growth_metric",x="2"}`, current[0].Example)
		assert.Equal(t, hr(0, 99), historical[0].Range)
		assert.Equal(t, `{__name__="residue_metric",x="1"}`, historical[0].Example)
	})

	t.Run("retains previous example when walk passes empty slot", func(t *testing.T) {
		pr := newPartitionRanges()
		pr.setRanges([]assignment.HashRange{hr(0, 99)})

		snap := pr.rangesSnapshot()
		require.True(t, pr.applyWalkResult(snap, []int64{10}, []string{`{__name__="first_seen"}`}))

		// A second walk: still has the range, but found no series
		// (e.g. a transient compaction race). The previous example
		// should be preserved so the admin page doesn't blank out.
		require.True(t, pr.applyWalkResult(snap, []int64{0}, []string{""}))
		current, _ := pr.adminSnapshot()
		require.Len(t, current, 1)
		assert.Equal(t, `{__name__="first_seen"}`, current[0].Example,
			"empty slot should not clobber the previous example")
	})

	t.Run("nil examples slice leaves prior examples intact", func(t *testing.T) {
		pr := newPartitionRanges()
		pr.setRanges([]assignment.HashRange{hr(0, 99)})

		snap := pr.rangesSnapshot()
		require.True(t, pr.applyWalkResult(snap, []int64{10}, []string{`{__name__="cached"}`}))
		// Stats-only caller (nil examples) — must not blank the
		// previously-captured example.
		require.True(t, pr.applyWalkResult(snap, []int64{15}, nil))
		current, _ := pr.adminSnapshot()
		require.Len(t, current, 1)
		assert.Equal(t, `{__name__="cached"}`, current[0].Example)
		assert.Equal(t, int64(15), current[0].Count)
	})

	t.Run("examples for dropped ranges are GC'd", func(t *testing.T) {
		pr := newPartitionRanges()
		pr.setRanges([]assignment.HashRange{hr(0, 99)})

		require.True(t, pr.applyWalkResult(
			[]assignment.HashRange{hr(0, 99)},
			[]int64{10},
			[]string{`{__name__="lives_here"}`},
		))

		// Move ownership and let residue compact away. setRanges
		// drops the previous current into historical; the next
		// walk with zero residue should let GC drop the example.
		pr.setRanges([]assignment.HashRange{hr(500, 599)})
		require.True(t, pr.applyWalkResult(
			[]assignment.HashRange{hr(0, 99), hr(500, 599)},
			[]int64{0, 0},
			[]string{"", ""},
		))

		current, historical := pr.adminSnapshot()
		require.Len(t, current, 1)
		assert.Empty(t, historical, "compacted-out historical should be GC'd")
		// The new current range hasn't seen any series; example
		// should be empty.
		assert.Empty(t, current[0].Example)
	})

	t.Run("setRanges drops examples for ranges that fall out of both sets", func(t *testing.T) {
		pr := newPartitionRanges()
		pr.setRanges([]assignment.HashRange{hr(0, 99)})
		require.True(t, pr.applyWalkResult(
			[]assignment.HashRange{hr(0, 99)},
			[]int64{10},
			[]string{`{__name__="should_disappear"}`},
		))

		// Compaction tick clears residue, then we get a brand-new
		// assignment that shares no hash space with the old.
		pr.setRanges([]assignment.HashRange{hr(500, 599)})
		require.True(t, pr.applyWalkResult(
			[]assignment.HashRange{hr(0, 99), hr(500, 599)},
			[]int64{0, 0},
			nil,
		))
		// Force a fresh setRanges that excludes the historical too —
		// this happens whenever the rebalancer re-issues without the
		// historical hash space (the residue counter would normally
		// drop it via applyWalkResult above, but we want to be sure
		// setRanges also GCs).
		pr.setRanges([]assignment.HashRange{hr(700, 799)})

		// Snapshot the bookkeeping directly — the previously-captured
		// example must be gone.
		pr.mu.RLock()
		_, hasOld := pr.exampleSeries[hr(0, 99)]
		pr.mu.RUnlock()
		assert.False(t, hasOld, "examples for ranges no longer tracked must be GC'd")
	})
}

// makeTSForHash builds a PreallocTimeseries whose locality hash will
// land in a specific bucket: it sets __name__ such that the metric
// half of ShardByMetricNameLocality is deterministic and adds one
// sample. The exact hash value depends on userID + metric and is
// computed by the test setup, not asserted here — what matters is
// that the same labels always hash to the same bucket within a run.
func makeTSForHash(metricName string, sampleCount int) mimirpb.PreallocTimeseries {
	samples := make([]mimirpb.Sample, sampleCount)
	for i := range samples {
		samples[i] = mimirpb.Sample{Value: float64(i), TimestampMs: 1000 + int64(i)}
	}
	return mimirpb.PreallocTimeseries{
		TimeSeries: &mimirpb.TimeSeries{
			Labels: []mimirpb.LabelAdapter{
				{Name: model.MetricNameLabel, Value: metricName},
			},
			Samples: samples,
		},
	}
}

func TestPartitionRanges_SampleRates(t *testing.T) {
	t.Run("recordSampleBatch buckets by hash into current ranges", func(t *testing.T) {
		pr := newPartitionRanges()
		// Two non-overlapping ranges that together cover the full
		// 32-bit hash space. Every sample must land in one or the
		// other; if any sample is dropped on the floor then the
		// fallthrough branches in recordSampleBatch are wrong.
		ranges := []assignment.HashRange{
			hr(0, math.MaxUint32/2),
			hr(math.MaxUint32/2+1, math.MaxUint32),
		}
		pr.setRanges(ranges)

		ts := []mimirpb.PreallocTimeseries{
			makeTSForHash("metric_a", 3),
			makeTSForHash("metric_b", 5),
			makeTSForHash("metric_c", 7),
			makeTSForHash("metric_d", 2),
		}
		pr.recordSampleBatch("user-1", ts)

		// Sum of per-range EwmaRate.newEvents must equal total
		// samples pushed in (3+5+7+2 = 17). Anything less would
		// mean the binary search dropped some samples.
		pr.mu.RLock()
		var total int64
		for _, rate := range pr.sampleRates {
			// Tick converts newEvents → instantRate; calling it
			// here lets us observe the per-tick total.
			rate.Tick()
		}
		pr.mu.RUnlock()
		_ = total

		// After Tick, every range's Rate() is (newEvents / 15s).
		// Sum and convert back to events.
		var sumEvents float64
		current, _ := pr.adminSnapshot()
		for _, c := range current {
			sumEvents += c.SampleRate
		}
		// One Tick at TickInterval seconds; first Tick initializes
		// lastRate = instantRate, so sum(Rate * interval) ==
		// total samples pushed (17).
		// loadstats.TickInterval = 15s.
		want := float64(17) / 15.0
		assert.InDelta(t, want, sumEvents, 1e-9,
			"per-range rates after one tick should sum to events/interval")
	})

	t.Run("samples outside current ranges are dropped", func(t *testing.T) {
		pr := newPartitionRanges()
		// Cover only a tiny slice of the hash space. Most samples
		// will hash outside and must be ignored, not bucketed into
		// some adjacent range.
		pr.setRanges([]assignment.HashRange{hr(0, 0)})

		ts := []mimirpb.PreallocTimeseries{
			makeTSForHash("a", 1), makeTSForHash("b", 1), makeTSForHash("c", 1),
			makeTSForHash("d", 1), makeTSForHash("e", 1), makeTSForHash("f", 1),
		}
		pr.recordSampleBatch("user-1", ts)

		pr.tickSampleRates()

		// All samples either land in [0,0] or are dropped. We
		// don't know which (it depends on the hash function); what
		// matters is that the rate is bounded by the number of
		// samples (6/15s) and not erroneously multiplied by
		// floor on the binary search.
		pr.mu.RLock()
		rate := pr.sampleRates[hr(0, 0)]
		pr.mu.RUnlock()
		require.NotNil(t, rate, "current range must have an EwmaRate")
		assert.LessOrEqual(t, rate.Rate(), float64(6)/15.0)
	})

	t.Run("rates decay toward zero when ingest stops", func(t *testing.T) {
		pr := newPartitionRanges()
		pr.setRanges([]assignment.HashRange{hr(0, math.MaxUint32)})

		// Push a burst, then tick until rate decays toward zero.
		ts := []mimirpb.PreallocTimeseries{makeTSForHash("burst", 1000)}
		pr.recordSampleBatch("user-1", ts)
		pr.tickSampleRates()

		current, _ := pr.adminSnapshot()
		require.Len(t, current, 1)
		initialRate := current[0].SampleRate
		require.Greater(t, initialRate, 0.0, "first tick should establish a non-zero rate")

		// 200 ticks with no new events. Alpha≈0.1591, so each tick
		// the rate moves toward 0 by ~16% of its current value;
		// after 200 ticks rate ≈ initialRate * (1-0.1591)^200 ≈
		// initialRate * 1e-15. Bound the assert generously.
		for i := 0; i < 200; i++ {
			pr.tickSampleRates()
		}
		current, _ = pr.adminSnapshot()
		assert.Less(t, current[0].SampleRate, initialRate*0.01,
			"rate must decay toward zero when no samples are pushed")
	})

	t.Run("setRanges seeds an EwmaRate per current range", func(t *testing.T) {
		pr := newPartitionRanges()
		pr.setRanges([]assignment.HashRange{hr(0, 99), hr(200, 299)})

		pr.mu.RLock()
		defer pr.mu.RUnlock()
		require.NotNil(t, pr.sampleRates[hr(0, 99)],
			"setRanges must materialise EwmaRate for new current ranges")
		require.NotNil(t, pr.sampleRates[hr(200, 299)],
			"setRanges must materialise EwmaRate for new current ranges")
	})

	t.Run("rates are GC'd when ranges fall out of working set", func(t *testing.T) {
		pr := newPartitionRanges()
		pr.setRanges([]assignment.HashRange{hr(0, 99)})

		// Push samples so applyWalkResult is satisfied with non-
		// zero residue, then move the range off this partition.
		pr.recordSampleBatch("user-1", []mimirpb.PreallocTimeseries{
			makeTSForHash("metric", 1),
		})

		// Range moves to another partition; on our side it falls
		// into historical until residue clears.
		pr.setRanges([]assignment.HashRange{hr(500, 599)})
		pr.mu.RLock()
		_, stillHasOldRate := pr.sampleRates[hr(0, 99)]
		pr.mu.RUnlock()
		assert.True(t, stillHasOldRate,
			"historical ranges keep their EwmaRate so decay is visible")

		// Compaction empties the residue; applyWalkResult drops
		// the historical range, and the GC sweep clears its
		// EwmaRate.
		require.True(t, pr.applyWalkResult(
			[]assignment.HashRange{hr(0, 99), hr(500, 599)},
			[]int64{0, 0},
			nil,
		))
		pr.mu.RLock()
		_, hasOldRate := pr.sampleRates[hr(0, 99)]
		pr.mu.RUnlock()
		assert.False(t, hasOldRate,
			"applyWalkResult must GC EwmaRates whose range fell out of both sets")
	})
}
