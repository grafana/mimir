// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"
)

// The ref table answers like a Go map through growth, deletions that move refs back into the
// holes, and shrinking.
func TestRefIndexMatchesAMap(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 4))
	var table refIndex
	want := map[uint64]refLocation{}
	var live []uint64
	next := uint64(0)
	for step := range 200_000 {
		switch {
		// Mostly new refs, counting up like the engine's, early on; mostly removals later.
		case len(live) == 0 || rng.IntN(10) < 6-step/50_000*2:
			next++
			ref := next<<refShardBits | 3
			location := refLocation{uint32(rng.IntN(50)), int32(rng.IntN(1 << 20))}
			table.set(ref, location)
			want[ref] = location
			live = append(live, ref)
		case rng.IntN(4) == 0:
			ref := live[rng.IntN(len(live))]
			location := refLocation{uint32(rng.IntN(50)), int32(rng.IntN(1 << 20))}
			table.set(ref, location)
			want[ref] = location
		default:
			at := rng.IntN(len(live))
			ref := live[at]
			live[at] = live[len(live)-1]
			live = live[:len(live)-1]
			table.delete(ref)
			delete(want, ref)
		}
		if step%997 == 0 {
			require.Equal(t, len(want), table.len())
			for ref, location := range want {
				got, ok := table.get(ref)
				require.True(t, ok, ref)
				require.Equal(t, location, got)
			}
			for range 100 {
				_, ok := table.get((next+uint64(rng.IntN(1000))+1)<<refShardBits | 3)
				require.False(t, ok)
			}
		}
	}
	require.Less(t, len(table.slots), 8*max(table.len(), 8)+1, "it gave back the slots of removed refs")
	table.delete(12345 << refShardBits)
	require.Equal(t, len(want), table.len(), "deleting a missing ref changes nothing")
}

// The hash table finds each hash's latest entry, also among hashes sharing their tag or their
// slot, which only the entries' full hashes tell apart.
func TestHashIndexMatchesAMap(t *testing.T) {
	rng := rand.New(rand.NewPCG(5, 6))
	var (
		index   hashIndex
		entries []seriesEntry
		want    = map[uint64]int32{}
	)
	hashes := make([]uint64, 0, 3000)
	for range 1000 {
		hash := rng.Uint64()
		// Same tag, other slot; same slot, other tag.
		hashes = append(hashes, hash, hash^0xffff, hash^(0xffff<<40))
	}
	for range 20_000 {
		hash := hashes[rng.IntN(len(hashes))]
		entries = append(entries, seriesEntry{hash: hash})
		index.set(hash, int32(len(entries)-1), entries)
		want[hash] = int32(len(entries) - 1)
	}
	for _, hash := range hashes {
		got, ok := index.get(hash, entries)
		expected, present := want[hash]
		require.Equal(t, present, ok)
		require.Equal(t, expected, got)
	}
	_, ok := index.get(rng.Uint64(), entries)
	require.False(t, ok)
	require.Equal(t, len(want), index.used)
	require.False(t, tableFull(index.used, len(index.slots)))
}
