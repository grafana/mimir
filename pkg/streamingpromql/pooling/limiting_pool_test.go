// SPDX-License-Identifier: AGPL-3.0-only

package pooling

import (
	"testing"

	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"
)

func TestLimitingPool_Unlimited_FPointSlices(t *testing.T) {
	setupLimitingPoolFunctionsForTesting(t)

	pool := NewLimitingPool(0)

	// Get a slice from the pool, the current and peak stats should be updated based on the capacity of the slice returned, not the size requested.
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

	// Ensure we handle nil slices safely.
	pool.PutFPointSlice(nil)
	require.Equal(t, 210, pool.CurrentInMemorySamples)
	require.Equal(t, 210, pool.PeakInMemorySamples)
}

func TestLimitingPool_Unlimited_HPointSlices(t *testing.T) {
	setupLimitingPoolFunctionsForTesting(t)

	pool := NewLimitingPool(0)

	// Get a slice from the pool, the current and peak stats should be updated based on the capacity of the slice returned, not the size requested.
	// The current and peak in-memory samples stats should be multiplied by the native histogram conversion factor of 10.
	h100, err := pool.GetHPointSlice(100)
	require.NoError(t, err)
	require.Equal(t, 101, cap(h100))
	require.Equal(t, 1010, pool.CurrentInMemorySamples)
	require.Equal(t, 1010, pool.PeakInMemorySamples)

	// Get another slice from the pool, the current and peak stats should be updated.
	h2, err := pool.GetHPointSlice(2)
	require.NoError(t, err)
	require.Equal(t, 3, cap(h2))
	require.Equal(t, 1040, pool.CurrentInMemorySamples)
	require.Equal(t, 1040, pool.PeakInMemorySamples)

	// Put a slice back into the pool, the current stat should be updated but peak should be unchanged.
	pool.PutHPointSlice(h100)
	require.Equal(t, 30, pool.CurrentInMemorySamples)
	require.Equal(t, 1040, pool.PeakInMemorySamples)

	// Get another slice from the pool that doesn't take us over the previous peak.
	h5, err := pool.GetHPointSlice(5)
	require.NoError(t, err)
	require.Equal(t, 6, cap(h5))
	require.Equal(t, 90, pool.CurrentInMemorySamples)
	require.Equal(t, 1040, pool.PeakInMemorySamples)

	// Get another slice from the pool that does take us over the previous peak.
	h200, err := pool.GetHPointSlice(200)
	require.NoError(t, err)
	require.Equal(t, 201, cap(h200))
	require.Equal(t, 2100, pool.CurrentInMemorySamples)
	require.Equal(t, 2100, pool.PeakInMemorySamples)

	// Ensure we handle nil slices safely.
	pool.PutHPointSlice(nil)
	require.Equal(t, 2100, pool.CurrentInMemorySamples)
	require.Equal(t, 2100, pool.PeakInMemorySamples)
}

func TestLimitingPool_Limited_FPointSlices(t *testing.T) {
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
	f1, err = pool.GetFPointSlice(1)
	require.NoError(t, err)
	require.Equal(t, 2, cap(f1))
	require.Equal(t, 10, pool.CurrentInMemorySamples)
	require.Equal(t, 10, pool.PeakInMemorySamples)
}

func TestLimitingPool_Limited_HPointSlices(t *testing.T) {
	setupLimitingPoolFunctionsForTesting(t)

	pool := NewLimitingPool(100)

	// Get a slice from the pool beneath the limit.
	h7, err := pool.GetHPointSlice(7)
	require.NoError(t, err)
	require.Equal(t, 8, cap(h7))
	require.Equal(t, 80, pool.CurrentInMemorySamples)
	require.Equal(t, 80, pool.PeakInMemorySamples)

	// Get another slice from the pool beneath the limit.
	h1, err := pool.GetHPointSlice(1)
	require.NoError(t, err)
	require.Equal(t, 2, cap(h1))
	require.Equal(t, 100, pool.CurrentInMemorySamples)
	require.Equal(t, 100, pool.PeakInMemorySamples)

	// Return a slice to the pool.
	pool.PutHPointSlice(h1)
	require.Equal(t, 80, pool.CurrentInMemorySamples)
	require.Equal(t, 100, pool.PeakInMemorySamples)

	// Try to get a slice where the requested size would push us over the limit.
	_, err = pool.GetHPointSlice(3)
	require.ErrorContains(t, err, "the query exceeded the maximum allowed number of in-memory samples (limit: 100 samples) (err-mimir-max-in-memory-samples-per-query)")
	require.Equal(t, 80, pool.CurrentInMemorySamples)
	require.Equal(t, 100, pool.PeakInMemorySamples)

	// Try to get a slice where the requested size is under the limit, but the capacity of the slice returned by the pool is over the limit.
	_, err = pool.GetHPointSlice(2)
	require.ErrorContains(t, err, "the query exceeded the maximum allowed number of in-memory samples (limit: 100 samples) (err-mimir-max-in-memory-samples-per-query)")
	require.Equal(t, 80, pool.CurrentInMemorySamples)
	require.Equal(t, 100, pool.PeakInMemorySamples)

	// Get another slice from the pool that would take us right up to the limit.
	h1, err = pool.GetHPointSlice(1)
	require.NoError(t, err)
	require.Equal(t, 2, cap(h1))
	require.Equal(t, 100, pool.CurrentInMemorySamples)
	require.Equal(t, 100, pool.PeakInMemorySamples)
}

// setupLimitingPoolFunctionsForTesting replaces the global FPoint slice pool used by LimitingPool
// with a fake for testing.
//
// Each returned slice will have capacity = requested size + 1.
func setupLimitingPoolFunctionsForTesting(t *testing.T) {
	originalGetFPointSlice := getFPointSliceForLimitingPool
	originalPutFPointSlice := putFPointSliceForLimitingPool
	originalGetHPointSlice := getHPointSliceForLimitingPool
	originalPutHPointSlice := putHPointSliceForLimitingPool

	getFPointSliceForLimitingPool = func(size int) []promql.FPoint {
		return make([]promql.FPoint, 0, size+1)
	}

	putFPointSliceForLimitingPool = func(_ []promql.FPoint) {
		// Drop slice on the floor - we don't need it.
	}

	getHPointSliceForLimitingPool = func(size int) []promql.HPoint { return make([]promql.HPoint, 0, size+1) }

	putHPointSliceForLimitingPool = func(_ []promql.HPoint) {
		// Drop slice on the floor - we don't need it.
	}

	t.Cleanup(func() {
		getFPointSliceForLimitingPool = originalGetFPointSlice
		putFPointSliceForLimitingPool = originalPutFPointSlice
		getHPointSliceForLimitingPool = originalGetHPointSlice
		putHPointSliceForLimitingPool = originalPutHPointSlice
	})
}
