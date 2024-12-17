// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/util/pool/pool_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package pool

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func makeFunc(size int) []int {
	return make([]int, 0, size)
}

func TestBucketedPool_HappyPath(t *testing.T) {

	cases := []struct {
		size        int
		expectedCap int
	}{
		{
			size:        0,
			expectedCap: 0,
		},
		{
			size:        1,
			expectedCap: 1,
		},
		{
			// One less than bucket boundary
			size:        3,
			expectedCap: 4,
		},
		{
			// Same as bucket boundary
			size:        4,
			expectedCap: 4,
		},
		{
			// One more than bucket boundary
			size:        5,
			expectedCap: 8,
		},
		{
			// Two more than bucket boundary
			size:        6,
			expectedCap: 8,
		},
		{
			size:        8,
			expectedCap: 8,
		},
		{
			size:        16,
			expectedCap: 16,
		},
		{
			size:        20,
			expectedCap: 20, // Max size is 19, so we expect to get a slice with the size requested (20), not 32 (the next power of two).
		},
	}

	runTests := func(t *testing.T, returnToPool bool) {
		testPool := NewBucketedPool(19, makeFunc)
		for _, c := range cases {
			ret := testPool.Get(c.size)
			require.Equal(t, c.expectedCap, cap(ret))
			require.Len(t, ret, 0)

			if returnToPool {
				// Add something to the slice, so we can test that the next consumer of the slice receives a slice of length 0.
				if cap(ret) > 0 {
					ret = append(ret, 123)
				}

				testPool.Put(ret)
			}
		}
	}

	t.Run("populated pool", func(t *testing.T) {
		runTests(t, true)
	})

	t.Run("empty pool", func(t *testing.T) {
		runTests(t, false)
	})
}

func TestBucketedPool_SliceNotAlignedToBuckets(t *testing.T) {
	pool := NewBucketedPool(1000, makeFunc)
	pool.Put(make([]int, 0, 5))
	s := pool.Get(6)
	require.Equal(t, 8, cap(s))
	require.Len(t, s, 0)
}

func TestBucketedPool_PutEmptySlice(t *testing.T) {
	pool := NewBucketedPool(1000, makeFunc)
	pool.Put([]int{})
	s := pool.Get(1)
	require.Equal(t, 1, cap(s))
	require.Len(t, s, 0)
}

func TestBucketedPool_PutNilSlice(t *testing.T) {
	pool := NewBucketedPool(1000, makeFunc)
	pool.Put(nil)
	s := pool.Get(1)
	require.Equal(t, 1, cap(s))
	require.Len(t, s, 0)
}

func TestBucketedPool_PutSliceLargerThanMaximum(t *testing.T) {
	pool := NewBucketedPool(100, makeFunc)
	s1 := make([]int, 101)
	pool.Put(s1)
	s2 := pool.Get(101)[:101]
	require.NotSame(t, &s1[0], &s2[0])
	require.Equal(t, 101, cap(s2))
}

func TestBucketedPool_GetSizeCloseToMax(t *testing.T) {
	maxSize := 100000
	pool := NewBucketedPool(uint(maxSize), makeFunc)

	// Request a size that triggers the last bucket boundary.
	s := pool.Get(86401)

	// Check that we still get a slice with the correct size.
	require.Equal(t, 86401, cap(s))
	require.Len(t, s, 0)
}
