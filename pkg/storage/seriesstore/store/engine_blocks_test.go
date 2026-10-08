// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"math/rand/v2"
	"slices"
	"testing"

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
