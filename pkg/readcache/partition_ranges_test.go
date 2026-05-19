// SPDX-License-Identifier: AGPL-3.0-only

package readcache

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
