// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
)

// TestSpotlightStore_MaybeSpotlight_Always exercises the sampling
// path with rate=1.0: every call must produce a spotlight, and each
// spotlight gets a unique TraceID even when the (range, partitions)
// tuple repeats.
func TestSpotlightStore_MaybeSpotlight_Always(t *testing.T) {
	store := newSpotlightStore(1, 1.0, time.Minute)
	t0 := time.Unix(1700000000, 0)

	hr := assignment.HashRange{Lo: 100, Hi: 200}
	sp1, ok := store.maybeSpotlight(t0, hr, 5, 9, "phase3-move")
	require.True(t, ok)
	require.Equal(t, "phase3-move", sp1.Reason)
	require.Equal(t, hr, sp1.Range)
	require.Equal(t, int32(5), sp1.FromPartition)
	require.Equal(t, int32(9), sp1.ToPartition)
	require.Equal(t, t0.Add(time.Minute), sp1.ExpiresAt)

	// Re-sampling the same (range, partitions) at a later instant
	// produces a distinct TraceID; the wall-clock component
	// disambiguates the two even when range and partitions match.
	sp2, ok := store.maybeSpotlight(t0.Add(time.Second), hr, 5, 9, "phase3-move")
	require.True(t, ok)
	require.NotEqual(t, sp1.TraceID, sp2.TraceID)
	require.Equal(t, 2, store.len())
}

// TestSpotlightStore_MaybeSpotlight_Never confirms the sampling
// short-circuit: rate=0 never produces an entry and the rng is
// still consumed deterministically (no goroutine leak from
// recursive retry).
func TestSpotlightStore_MaybeSpotlight_Never(t *testing.T) {
	store := newSpotlightStore(1, 0.0, time.Minute)
	for i := 0; i < 100; i++ {
		_, ok := store.maybeSpotlight(time.Now(), assignment.HashRange{Lo: 0, Hi: 100}, 0, 1, "phase3-move")
		require.False(t, ok)
	}
	require.Equal(t, 0, store.len())
}

// TestSpotlightStore_Prune drops only entries whose ExpiresAt is at
// or before `at`. The non-expired entry survives.
func TestSpotlightStore_Prune(t *testing.T) {
	store := newSpotlightStore(1, 1.0, time.Minute)
	t0 := time.Unix(1700000000, 0)

	store.add(Spotlight{TraceID: "expired", Range: assignment.HashRange{Lo: 0, Hi: 9}, StartedAt: t0, ExpiresAt: t0.Add(time.Minute)})
	store.add(Spotlight{TraceID: "fresh", Range: assignment.HashRange{Lo: 10, Hi: 19}, StartedAt: t0, ExpiresAt: t0.Add(10 * time.Minute)})

	store.prune(t0.Add(2 * time.Minute))
	snap := store.snapshot()
	require.Len(t, snap, 1)
	require.Equal(t, "fresh", snap[0].TraceID)
}

// TestSpotlightStore_Snapshot_Concurrent confirms the read path is
// safe against concurrent writes. The contract is "no data race,
// length never negative" — exact contents are racy by design.
func TestSpotlightStore_Snapshot_Concurrent(t *testing.T) {
	store := newSpotlightStore(1, 1.0, time.Minute)
	t0 := time.Unix(1700000000, 0)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			store.add(Spotlight{
				TraceID:   "id-" + string(rune('a'+(i%26))),
				Range:     assignment.HashRange{Lo: uint32(i), Hi: uint32(i + 1)},
				StartedAt: t0,
				ExpiresAt: t0.Add(time.Hour),
			})
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			_ = store.snapshot()
		}
	}()
	wg.Wait()
	require.NotPanics(t, func() { _ = store.snapshot() })
}

// TestRebalancer_GetSpotlightedRanges_Empty returns an empty
// response when no spotlights are active. This is the cold-start
// case downstream consumers must tolerate.
func TestRebalancer_GetSpotlightedRanges_Empty(t *testing.T) {
	r := &Rebalancer{
		spotlights: newSpotlightStore(1, 1.0, time.Minute),
	}
	resp, err := r.GetSpotlightedRanges(context.Background(), &GetSpotlightedRangesRequest{})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Empty(t, resp.Ranges)
}

// TestRebalancer_GetSpotlightedRanges_RoundTrips populates the
// store and validates the proto-side fields end up exactly mirroring
// the in-memory Spotlight values. This catches schema drift between
// the proto and the conversion function inside GetSpotlightedRanges.
func TestRebalancer_GetSpotlightedRanges_RoundTrips(t *testing.T) {
	store := newSpotlightStore(1, 1.0, time.Minute)
	t0 := time.UnixMilli(1700000000123)
	store.add(Spotlight{
		TraceID:       "alpha",
		Range:         assignment.HashRange{Lo: 100, Hi: 200},
		StartedAt:     t0,
		ExpiresAt:     t0.Add(10 * time.Minute),
		FromPartition: 5,
		ToPartition:   9,
		Reason:        "phase3-move",
	})
	store.add(Spotlight{
		TraceID:       "beta",
		Range:         assignment.HashRange{Lo: 300, Hi: 400},
		StartedAt:     t0,
		ExpiresAt:     t0.Add(10 * time.Minute),
		FromPartition: 1,
		ToPartition:   2,
		Reason:        "phase3-move",
	})

	r := &Rebalancer{spotlights: store}
	resp, err := r.GetSpotlightedRanges(context.Background(), &GetSpotlightedRangesRequest{})
	require.NoError(t, err)
	require.Len(t, resp.Ranges, 2)

	sort.Slice(resp.Ranges, func(i, j int) bool { return resp.Ranges[i].TraceId < resp.Ranges[j].TraceId })
	assert.Equal(t, "alpha", resp.Ranges[0].TraceId)
	assert.Equal(t, uint32(100), resp.Ranges[0].Lo)
	assert.Equal(t, uint32(200), resp.Ranges[0].Hi)
	assert.Equal(t, t0.UnixMilli(), resp.Ranges[0].StartedAtUnixMs)
	assert.Equal(t, t0.Add(10*time.Minute).UnixMilli(), resp.Ranges[0].ExpiresAtUnixMs)
	assert.Equal(t, int32(5), resp.Ranges[0].FromPartitionId)
	assert.Equal(t, int32(9), resp.Ranges[0].ToPartitionId)
	assert.Equal(t, "phase3-move", resp.Ranges[0].Reason)

	assert.Equal(t, "beta", resp.Ranges[1].TraceId)
}

// TestRebalancer_GetSpotlightedRanges_NilStore covers the
// defensive-nil branch — many unit tests construct &Rebalancer{...}
// literally without going through New(), and those tests should not
// blow up if anything reaches into GetSpotlightedRanges via the
// gRPC server.
func TestRebalancer_GetSpotlightedRanges_NilStore(t *testing.T) {
	r := &Rebalancer{}
	resp, err := r.GetSpotlightedRanges(context.Background(), &GetSpotlightedRangesRequest{})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Empty(t, resp.Ranges)
}

// TestSpotlightMergeActions samples both same-partition and
// cross-partition merge actions. The downstream observers
// (readcache/distributor) consume the resulting Spotlight entries
// generically: they only care about (range, partitions, reason),
// so the test focuses on what the rebalancer must populate.
func TestSpotlightMergeActions(t *testing.T) {
	r := &Rebalancer{
		spotlights: newSpotlightStore(1, 1.0, 5*time.Minute),
		logger:     log.NewNopLogger(),
	}
	now := time.Unix(1700000000, 0)

	mergeActions := []Action{
		{
			Kind:   ActionMerge,
			Range:  assignment.HashRange{Lo: 100, Hi: 199},
			ToPart: 7,
			Series: 42,
			Detail: "same-partition merge on P7, combined load=0.5",
		},
		{
			Kind:     ActionMerge,
			Range:    assignment.HashRange{Lo: 200, Hi: 299},
			FromPart: 3,
			ToPart:   8,
			Series:   17,
			Detail:   "cross-partition merge P3+P8→P8, combined load=0.4",
		},
		// Non-merge actions interleaved in the slice are ignored —
		// the helper filters by Kind so we never spotlight a move
		// or split with reason=phase2-merge.
		{Kind: ActionMove, Range: assignment.HashRange{Lo: 500, Hi: 599}, FromPart: 1, ToPart: 2},
	}

	r.spotlightMergeActions(now, mergeActions)

	snap := r.spotlights.snapshot()
	require.Len(t, snap, 2, "exactly the two merge actions must be spotlighted")

	byRange := map[uint32]Spotlight{}
	for _, sp := range snap {
		byRange[sp.Range.Lo] = sp
	}

	same := byRange[100]
	assert.Equal(t, "phase2-merge", same.Reason)
	assert.Equal(t, int32(0), same.FromPartition, "same-partition merge keeps FromPartition=0")
	assert.Equal(t, int32(7), same.ToPartition)

	cross := byRange[200]
	assert.Equal(t, "phase2-merge", cross.Reason)
	assert.Equal(t, int32(3), cross.FromPartition, "cross-partition merge must carry donor PID")
	assert.Equal(t, int32(8), cross.ToPartition)
}

// TestSpotlightMergeActions_NilStore confirms the defensive nil
// branch lets harness tests that build a Rebalancer struct literal
// (without a spotlight store) call into runSlicer without panicking.
func TestSpotlightMergeActions_NilStore(t *testing.T) {
	r := &Rebalancer{}
	require.NotPanics(t, func() {
		r.spotlightMergeActions(time.Now(), []Action{
			{Kind: ActionMerge, Range: assignment.HashRange{Lo: 1, Hi: 2}, ToPart: 5, Series: 1},
		})
	})
}

// TestEmitSplitSpotlight populates a single split spotlight with
// rich per-half context and verifies the stored Spotlight carries
// the parent range (so downstream observers see overlap with both
// halves once SetHashRanges propagates).
func TestEmitSplitSpotlight(t *testing.T) {
	r := &Rebalancer{
		spotlights: newSpotlightStore(1, 1.0, 5*time.Minute),
		logger:     log.NewNopLogger(),
	}
	now := time.Unix(1700000000, 0)

	parent := rangeLoad{
		entry:  assignment.Entry{Range: assignment.HashRange{Lo: 1000, Hi: 1999}, PartitionID: 12},
		load:   200.0,
		series: 5000,
	}
	left := assignment.HashRange{Lo: 1000, Hi: 1499}
	right := assignment.HashRange{Lo: 1500, Hi: 1999}

	r.emitSplitSpotlight(now, parent, left, right, 120.0, 80.0, 150.0)

	snap := r.spotlights.snapshot()
	require.Len(t, snap, 1)
	sp := snap[0]
	assert.Equal(t, "phase4-split", sp.Reason)
	assert.Equal(t, parent.entry.Range, sp.Range, "spotlight covers the parent range so observers see both halves")
	assert.Equal(t, int32(0), sp.FromPartition, "split has no donor partition")
	assert.Equal(t, int32(12), sp.ToPartition)
}

// TestEmitSplitSpotlight_NilStore is the symmetric nil-defence
// check for the split helper.
func TestEmitSplitSpotlight_NilStore(t *testing.T) {
	r := &Rebalancer{}
	parent := rangeLoad{entry: assignment.Entry{Range: assignment.HashRange{Lo: 1, Hi: 2}, PartitionID: 5}}
	require.NotPanics(t, func() {
		r.emitSplitSpotlight(time.Now(), parent, assignment.HashRange{Lo: 1, Hi: 1}, assignment.HashRange{Lo: 2, Hi: 2}, 1, 1, 2)
	})
}
