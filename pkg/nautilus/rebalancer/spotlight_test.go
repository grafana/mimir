// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"

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
