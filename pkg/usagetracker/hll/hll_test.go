// SPDX-License-Identifier: AGPL-3.0-only

package hll

import (
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHash(t *testing.T) {
	// The first output of SplitMix64 seeded with 0: with no salt, Hash(0) is that same step.
	require.Equal(t, uint64(0xe220a8397b1dcdaf), Hash(0, 0))
	require.NotEqual(t, Hash(1, 0), Hash(1, 1), "salt must change the hash")
}

func TestSketchInsertRank(t *testing.T) {
	const p = 8
	s := NewSketch(p)

	// All bits after the index are 0: the rank is capped at MaxRank.
	s.Insert(0)
	require.Equal(t, MaxRank(p), s[0])

	// The first bit after the index is 1: rank 1.
	s.Insert(0xff<<56 | 1<<55)
	require.Equal(t, uint8(1), s[0xff])

	// A lower rank doesn't replace a higher one.
	s.Insert(1 << 55)
	require.Equal(t, MaxRank(p), s[0])
}

func TestSketchEstimate(t *testing.T) {
	for _, p := range []uint8{MinPrecision, 8, 12, MaxPrecision} {
		// The relative standard error is about 1.04/sqrt(m): allow 5 of them.
		tolerance := 5 * 1.04 / math.Sqrt(float64(uint(1)<<p))
		for _, n := range []int{0, 1, 10, 100, 1000, 10_000, 100_000, 1_000_000} {
			t.Run(fmt.Sprintf("p=%d/n=%d", p, n), func(t *testing.T) {
				s := NewSketch(p)
				values := make([]uint64, n)
				for i := range values {
					values[i] = uint64(i)
				}
				// Insert every value twice: duplicates don't count.
				s.InsertAll(values, 42)
				s.InsertAll(values, 42)

				estimate := s.Estimate()
				if n == 0 {
					require.Zero(t, estimate)
					return
				}
				require.InEpsilon(t, float64(n), estimate, tolerance)
			})
		}
	}
}

func TestSketchMerge(t *testing.T) {
	for _, p := range []uint8{MinPrecision, 8, 12} {
		t.Run(fmt.Sprintf("p=%d", p), func(t *testing.T) {
			r := rand.New(rand.NewSource(int64(p)))
			a, b := NewSketch(p), NewSketch(p)
			for i := range a {
				a[i] = uint8(r.Intn(int(MaxRank(p)) + 1))
				b[i] = uint8(r.Intn(int(MaxRank(p)) + 1))
			}
			expected := NewSketch(p)
			for i := range expected {
				expected[i] = max(a[i], b[i])
			}

			a.Merge(b)
			require.Equal(t, expected, a)
		})
	}

	t.Run("union", func(t *testing.T) {
		const p = 8
		values := make([]uint64, 10_000)
		for i := range values {
			values[i] = uint64(i)
		}
		all, first, second := NewSketch(p), NewSketch(p), NewSketch(p)
		all.InsertAll(values, 1)
		first.InsertAll(values[:6000], 1)
		second.InsertAll(values[4000:], 1)

		first.Merge(second)
		require.Equal(t, all, first)
	})
}

func TestSketchValid(t *testing.T) {
	require.True(t, NewSketch(MinPrecision).Valid())
	require.True(t, NewSketch(MaxPrecision).Valid())
	require.False(t, make(Sketch, 1<<(MinPrecision-1)).Valid(), "too small")
	require.False(t, make(Sketch, 1<<(MaxPrecision+1)).Valid(), "too large")
	require.False(t, make(Sketch, 300).Valid(), "not a power of 2")

	s := NewSketch(8)
	s[3] = MaxRank(8)
	require.True(t, s.Valid())
	s[3] = MaxRank(8) + 1
	require.False(t, s.Valid())
}

func BenchmarkSketchInsertAll(b *testing.B) {
	values := make([]uint64, 1000)
	for i := range values {
		values[i] = rand.Uint64()
	}
	for _, p := range []uint8{8, 12} {
		b.Run(fmt.Sprintf("p=%d", p), func(b *testing.B) {
			s := NewSketch(p)
			for b.Loop() {
				s.InsertAll(values, 42)
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*len(values)), "ns/value")
		})
	}
}

func BenchmarkSketchMerge(b *testing.B) {
	for _, p := range []uint8{8, 12} {
		b.Run(fmt.Sprintf("p=%d", p), func(b *testing.B) {
			s, other := NewSketch(p), NewSketch(p)
			for i := range other {
				other[i] = uint8(rand.Intn(int(MaxRank(p)) + 1))
			}
			for b.Loop() {
				s.Merge(other)
			}
		})
	}
}

func BenchmarkSketchEstimate(b *testing.B) {
	for _, p := range []uint8{8, 12} {
		b.Run(fmt.Sprintf("p=%d", p), func(b *testing.B) {
			s := NewSketch(p)
			for i := range s {
				s[i] = uint8(rand.Intn(int(MaxRank(p)) + 1))
			}
			for b.Loop() {
				_ = s.Estimate()
			}
		})
	}
}
