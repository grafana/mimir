// SPDX-License-Identifier: AGPL-3.0-only

package pooling

import (
	"testing"

	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"
)

func TestLimitingPool_Unlimited(t *testing.T) {
	setupLimitingPoolFunctionsForTesting(t)

	pool := NewLimitingPool(0)

	// Get a slice from the pool, the current and peak stats should be updated based on the capacity of the slice returned, not the size requested..
	f100, err := pool.GetFPointSlice(100)
	require.NoError(t, err)
	require.Equal(t, 101, cap(f100))
	require.Equal(t, 101, pool.CurrentInMemorySamples)
	require.Equal(t, 101, pool.PeakInMemorySamples)

	// Get another slice from the pool, the current and peak stats should be updated.
	f2, err := pool.GetFPointSlice(2)
	require.NoError(t, err)
	require.Equal(t, 3, cap(f2))
	require.Equal(t, 104, pool.CurrentInMemorySamples)
	require.Equal(t, 104, pool.PeakInMemorySamples)

	// Put a slice back into the pool, the current stat should be updated but peak should be unchanged.
	pool.PutFPointSlice(f100)
	require.Equal(t, 3, pool.CurrentInMemorySamples)
	require.Equal(t, 104, pool.PeakInMemorySamples)

	// Get another slice from the pool that doesn't take us over the previous peak.
	f5, err := pool.GetFPointSlice(5)
	require.NoError(t, err)
	require.Equal(t, 6, cap(f5))
	require.Equal(t, 9, pool.CurrentInMemorySamples)
	require.Equal(t, 104, pool.PeakInMemorySamples)

	// Get another slice from the pool that does take us over the previous peak.
	f200, err := pool.GetFPointSlice(200)
	require.NoError(t, err)
	require.Equal(t, 201, cap(f200))
	require.Equal(t, 210, pool.CurrentInMemorySamples)
	require.Equal(t, 210, pool.PeakInMemorySamples)
}

func TestLimitingPool_Limited(t *testing.T) {
	setupLimitingPoolFunctionsForTesting(t)

	pool := NewLimitingPool(10)

	// Get a slice from the pool beneath the limit.
	f7, err := pool.GetFPointSlice(7)
	require.NoError(t, err)
	require.Equal(t, 8, cap(f7))
	require.Equal(t, 8, pool.CurrentInMemorySamples)
	require.Equal(t, 8, pool.PeakInMemorySamples)

	// Get another slice from the pool beneath the limit.
	f1, err := pool.GetFPointSlice(1)
	require.NoError(t, err)
	require.Equal(t, 2, cap(f1))
	require.Equal(t, 10, pool.CurrentInMemorySamples)
	require.Equal(t, 10, pool.PeakInMemorySamples)

	// Return a slice to the pool.
	pool.PutFPointSlice(f1)
	require.Equal(t, 8, pool.CurrentInMemorySamples)
	require.Equal(t, 10, pool.PeakInMemorySamples)

	// Try to get a slice where the requested size would push us over the limit.
	_, err = pool.GetFPointSlice(3)
	require.ErrorContains(t, err, "the query exceeded the maximum allowed number of in-memory samples (limit: 10 samples) (err-mimir-max-in-memory-samples-per-query)")
	require.Equal(t, 8, pool.CurrentInMemorySamples)
	require.Equal(t, 10, pool.PeakInMemorySamples)

	// Try to get a slice where the requested size is under the limit, but the capacity of the slice returned by the pool is over the limit.
	_, err = pool.GetFPointSlice(2)
	require.ErrorContains(t, err, "the query exceeded the maximum allowed number of in-memory samples (limit: 10 samples) (err-mimir-max-in-memory-samples-per-query)")
	require.Equal(t, 8, pool.CurrentInMemorySamples)
	require.Equal(t, 10, pool.PeakInMemorySamples)

	// Get another slice from the pool that would take us right up to the limit.
	f2, err := pool.GetFPointSlice(1)
	require.NoError(t, err)
	require.Equal(t, 2, cap(f2))
	require.Equal(t, 10, pool.CurrentInMemorySamples)
	require.Equal(t, 10, pool.PeakInMemorySamples)
}

// setupLimitingPoolFunctionsForTesting replaces the global FPoint slice pool used by LimitingPool
// with a fake for testing.
//
// Each returned slice will have capacity = requested size + 1.
func setupLimitingPoolFunctionsForTesting(t *testing.T) {
	originalGet := getFPointSliceForLimitingPool
	originalPut := putFPointSliceForLimitingPool

	getFPointSliceForLimitingPool = func(size int) []promql.FPoint {
		return make([]promql.FPoint, 0, size+1)
	}

	putFPointSliceForLimitingPool = func(_ []promql.FPoint) {
		// Drop slice on the floor - we don't need it.
	}

	t.Cleanup(func() {
		getFPointSliceForLimitingPool = originalGet
		putFPointSliceForLimitingPool = originalPut
	})
}
