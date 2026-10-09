// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	promlabels "github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

func TestContainsHashAgreesWithABinarySearch(t *testing.T) {
	random := rand.New(rand.NewPCG(1, 2))
	for _, shape := range []string{"uniform", "clustered", "small-range", "duplicates"} {
		for _, n := range []int{0, 1, 2, 7, 100, 5000} {
			sorted := make([]uint64, n)
			for i := range sorted {
				switch shape {
				case "uniform":
					sorted[i] = random.Uint64()
				case "clustered":
					sorted[i] = random.Uint64() >> 40
				case "small-range":
					sorted[i] = uint64(random.IntN(50))
				default:
					sorted[i] = uint64(random.IntN(max(n/3, 1))) << 50
				}
			}
			slices.Sort(sorted)
			probes := []uint64{0, 1, ^uint64(0), ^uint64(0) - 1}
			for range 2000 {
				probes = append(probes, random.Uint64(), random.Uint64()>>40, uint64(random.IntN(60)))
			}
			for _, hash := range sorted {
				probes = append(probes, hash, hash+1, hash-1)
			}
			for _, hash := range probes {
				_, want := slices.BinarySearch(sorted, hash)
				require.Equal(t, want, containsHash(sorted, hash), "%s n=%d hash=%d", shape, n, hash)
			}
		}
	}
}

// BenchmarkContainsHash is a lookup in the hashes of a 2h block of a 2M series ingester, against a binary search.
func BenchmarkContainsHash(b *testing.B) {
	random := rand.New(rand.NewPCG(3, 4))
	sorted := make([]uint64, 2_000_000)
	for i := range sorted {
		sorted[i] = random.Uint64()
	}
	slices.Sort(sorted)
	probes := make([]uint64, 1<<16)
	for i := range probes {
		if i%2 == 0 {
			probes[i] = sorted[random.IntN(len(sorted))]
		} else {
			probes[i] = random.Uint64()
		}
	}
	b.Run("interpolated", func(b *testing.B) {
		for i := range b.N {
			containsHash(sorted, probes[i&(len(probes)-1)])
		}
	})
	b.Run("binary", func(b *testing.B) {
		for i := range b.N {
			_, _ = slices.BinarySearch(sorted, probes[i&(len(probes)-1)])
		}
	})
}

// Out-of-order chunks written by appends while the compaction's scan has a shard unlocked between batches are not
// skipped by the watermark it moves.
func TestOutOfOrderCompactionKeepsChunksWrittenBetweenBatches(t *testing.T) {
	const hourMs = int64(3_600_000)
	oldBatch, oldHook := scanBatch, scanGapHook
	t.Cleanup(func() { scanBatch, scanGapHook = oldBatch, oldHook })
	scanBatch = 8

	engine, err := OpenEngine(t.TempDir(), "tenant", EngineOptions{Shards: 1, OutOfOrderTimeWindowMs: 2 * hourMs, SecondaryHashFunction: secondaryHash})
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })

	const seriesCount = 40
	lsets := make([]promlabels.Labels, seriesCount)
	for n := range lsets {
		lsets[n] = promlabels.FromStrings("__name__", "metric", "n", fmt.Sprint(n))
	}
	appendOOO := func(from int64, count int) {
		app := engine.Appender(context.Background())
		for _, lset := range lsets {
			for i := range count {
				_, err := app.Append(0, lset, from+int64(i)*1000, 1)
				require.NoError(t, err)
			}
		}
		require.NoError(t, app.Commit())
	}
	appendOOO(0, 1)
	appendOOO(4*hourMs, 1)
	appendOOO(3*hourMs, 1)

	// A full out-of-order buffer is written out to a chunk by the append that fills it.
	taken := map[uint64]bool{}
	oooCompactedHook = func(ref uint64) { taken[ref] = true }
	t.Cleanup(func() { oooCompactedHook = nil })
	gaps := 0
	scanGapHook = func() {
		// The first gap of the out-of-order compaction's scan, after it took a chunk.
		if len(taken) > 0 {
			gaps++
		}
		if gaps == 1 {
			appendOOO(2*hourMs+1000, outOfOrderCapacity)
		}
	}
	require.NoError(t, engine.Compact(context.Background()))
	require.Positive(t, gaps)

	shard := engine.store.shards[0]
	shard.RLock()
	defer shard.RUnlock()
	var missed []uint64
	shard.tenants["tenant"].series.forEach(func(entry *seriesEntry) {
		it := entry.series.chunks.iter()
		for chunk, more := it.next(); more; chunk, more = it.next() {
			if chunk.OutOfOrder && !taken[uint64(chunk.Ref)] {
				missed = append(missed, uint64(chunk.Ref))
			}
		}
	})
	require.Empty(t, missed, "out-of-order chunks that no block took")
}
