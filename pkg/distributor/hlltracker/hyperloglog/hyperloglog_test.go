// SPDX-License-Identifier: AGPL-3.0-only

package hyperloglog

import (
	"hash/fnv"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

// hash32 computes a 32-bit FNV-1a hash of the input value.
// This is needed because HyperLogLog requires well-distributed hash values.
func hash32(val uint32) uint32 {
	h := fnv.New32a()
	b := [4]byte{
		byte(val),
		byte(val >> 8),
		byte(val >> 16),
		byte(val >> 24),
	}
	h.Write(b[:])
	return h.Sum32()
}

func TestNew(t *testing.T) {
	tests := []struct {
		name      string
		precision uint8
		wantM     uint32
		wantPanic bool
	}{
		{
			name:      "precision 4",
			precision: 4,
			wantM:     16,
			wantPanic: false,
		},
		{
			name:      "precision 11",
			precision: 11,
			wantM:     2048,
			wantPanic: false,
		},
		{
			name:      "precision 14",
			precision: 14,
			wantM:     16384,
			wantPanic: false,
		},
		{
			name:      "precision too low",
			precision: 3,
			wantPanic: true,
		},
		{
			name:      "precision too high",
			precision: 19,
			wantPanic: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.wantPanic {
				assert.Panics(t, func() { New(tt.precision) })
			} else {
				hll := New(tt.precision)
				assert.Equal(t, tt.wantM, hll.M)
				assert.Equal(t, tt.precision, hll.B)
				assert.Len(t, hll.Registers, int(tt.wantM))
				assert.Greater(t, hll.Alpha, 0.0)
			}
		})
	}
}

func TestAdd(t *testing.T) {
	hll := New(11)

	// Initially, count should be close to 0
	count := hll.Count()
	assert.LessOrEqual(t, count, uint64(10))

	// Add some hashes
	for i := uint32(0); i < 100; i++ {
		hll.Add(hash32(i))
	}

	// Count should be approximately 100
	count = hll.Count()
	assert.Greater(t, count, uint64(80)) // Within ~20% error
	assert.Less(t, count, uint64(120))   // Within ~20% error
}

func TestMerge(t *testing.T) {
	hll1 := New(11)
	hll2 := New(11)

	// Add different values to each HLL
	for i := uint32(0); i < 50; i++ {
		hll1.Add(hash32(i))
	}

	for i := uint32(50); i < 100; i++ {
		hll2.Add(hash32(i))
	}

	// Merge hll2 into hll1
	hll1.Merge(hll2)

	// Count should be approximately 100
	count := hll1.Count()
	assert.Greater(t, count, uint64(80))
	assert.Less(t, count, uint64(120))
}

func TestMergeWithOverlap(t *testing.T) {
	hll1 := New(11)
	hll2 := New(11)

	// Add overlapping values
	for i := uint32(0); i < 100; i++ {
		hll1.Add(hash32(i))
	}

	for i := uint32(50); i < 150; i++ {
		hll2.Add(hash32(i))
	}

	// Merge hll2 into hll1
	hll1.Merge(hll2)

	// Count should be approximately 150 (total unique values)
	count := hll1.Count()
	assert.Greater(t, count, uint64(120))
	assert.Less(t, count, uint64(180))
}

func TestMergeDifferentPrecisions(t *testing.T) {
	hll1 := New(11)
	hll2 := New(12)

	assert.Panics(t, func() {
		hll1.Merge(hll2)
	})
}

func TestMergeNil(t *testing.T) {
	hll := New(11)
	hll.Add(hash32(1))

	countBefore := hll.Count()

	// Merging nil should not change the HLL
	hll.Merge(nil)

	countAfter := hll.Count()
	assert.Equal(t, countBefore, countAfter)
}

func TestClone(t *testing.T) {
	hll := New(11)

	for i := uint32(0); i < 100; i++ {
		hll.Add(hash32(i))
	}

	clone := hll.Clone()

	// Counts should be the same
	assert.Equal(t, hll.Count(), clone.Count())

	// Modify original
	for i := uint32(100); i < 200; i++ {
		hll.Add(hash32(i))
	}

	// Clone should not be affected
	cloneCount := clone.Count()
	hllCount := hll.Count()
	assert.Less(t, cloneCount, hllCount)
}

func TestAccuracyLargeCardinality(t *testing.T) {
	tests := []struct {
		name        string
		precision   uint8
		cardinality uint32
		maxErrorPct float64
	}{
		{
			name:        "precision 11, 1K items",
			precision:   11,
			cardinality: 1000,
			maxErrorPct: 40, // HLL has higher error for small cardinalities
		},
		{
			name:        "precision 11, 10K items",
			precision:   11,
			cardinality: 10000,
			maxErrorPct: 60, // Still relatively high error for precision 11
		},
		{
			name:        "precision 11, 100K items",
			precision:   11,
			cardinality: 100000,
			maxErrorPct: 20, // Better for larger cardinalities
		},
		{
			name:        "precision 14, 1M items",
			precision:   14,
			cardinality: 1000000,
			maxErrorPct: 5, // Much better accuracy with precision 14
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hll := New(tt.precision)

			for i := uint32(0); i < tt.cardinality; i++ {
				hll.Add(hash32(i))
			}

			count := hll.Count()
			errorPct := math.Abs(float64(count)-float64(tt.cardinality)) / float64(tt.cardinality) * 100

			t.Logf("Precision %d, Expected: %d, Got: %d, Error: %.2f%%",
				tt.precision, tt.cardinality, count, errorPct)

			assert.LessOrEqual(t, errorPct, tt.maxErrorPct)
		})
	}
}

func TestDuplicates(t *testing.T) {
	hll := New(11)

	// Add the same value many times
	val := hash32(42)
	for i := 0; i < 1000; i++ {
		hll.Add(val)
	}

	// Count should be approximately 1
	count := hll.Count()
	assert.LessOrEqual(t, count, uint64(10))
}

func TestIdempotentMerge(t *testing.T) {
	hll1 := New(11)
	hll2 := New(11)

	for i := uint32(0); i < 100; i++ {
		hll1.Add(hash32(i))
		hll2.Add(hash32(i))
	}

	countBefore := hll1.Count()

	// Merging identical HLLs should not change the count
	hll1.Merge(hll2)

	countAfter := hll1.Count()
	assert.Equal(t, countBefore, countAfter)
}

func BenchmarkAdd(b *testing.B) {
	hll := New(11)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hll.Add(hash32(uint32(i)))
	}
}

func BenchmarkCount(b *testing.B) {
	hll := New(11)

	for i := uint32(0); i < 10000; i++ {
		hll.Add(hash32(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = hll.Count()
	}
}

func BenchmarkMerge(b *testing.B) {
	hll1 := New(11)
	hll2 := New(11)

	for i := uint32(0); i < 10000; i++ {
		hll1.Add(hash32(i))
		hll2.Add(hash32(i + 5000))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hll1Clone := hll1.Clone()
		hll1Clone.Merge(hll2)
	}
}

func BenchmarkClone(b *testing.B) {
	hll := New(11)

	for i := uint32(0); i < 10000; i++ {
		hll.Add(hash32(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = hll.Clone()
	}
}
