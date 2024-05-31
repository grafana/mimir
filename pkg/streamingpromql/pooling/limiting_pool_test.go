// SPDX-License-Identifier: AGPL-3.0-only

package pooling

import (
	"fmt"
	"testing"

	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"
)

func TestLimitingPool_Unlimited(t *testing.T) {
	setupLimitingPoolFunctionsForTesting(t)

	t.Run("[]promql.FPoint", func(t *testing.T) {
		pool := NewLimitingPool(0)
		testUnlimitedPool(t, pool.GetFPointSlice, pool.PutFPointSlice, pool, fPointSize)
	})

	t.Run("[]promql.HPoint", func(t *testing.T) {
		pool := NewLimitingPool(0)
		testUnlimitedPool(t, pool.GetHPointSlice, pool.PutHPointSlice, pool, hPointSize)
	})

	t.Run("promql.Vector", func(t *testing.T) {
		pool := NewLimitingPool(0)
		testUnlimitedPool(t, pool.GetVector, pool.PutVector, pool, vectorSampleSize)
	})

	t.Run("[]float64", func(t *testing.T) {
		pool := NewLimitingPool(0)
		testUnlimitedPool(t, pool.GetFloatSlice, pool.PutFloatSlice, pool, float64Size)
	})

	t.Run("[]bool", func(t *testing.T) {
		pool := NewLimitingPool(0)
		testUnlimitedPool(t, pool.GetBoolSlice, pool.PutBoolSlice, pool, boolSize)
	})
}

func testUnlimitedPool[E any, S ~[]E](t *testing.T, get func(int) (S, error), put func(S), pool *LimitingPool, elementSize uint64) {
	// Get a slice from the pool, the current and peak stats should be updated based on the capacity of the slice returned, not the size requested.
	s100, err := get(100)
	require.NoError(t, err)
	require.Equal(t, 101, cap(s100))
	require.Equal(t, 101*elementSize, pool.CurrentEstimatedMemoryConsumptionBytes)
	require.Equal(t, 101*elementSize, pool.PeakEstimatedMemoryConsumptionBytes)

	// Get another slice from the pool, the current and peak stats should be updated.
	s2, err := get(2)
	require.NoError(t, err)
	require.Equal(t, 3, cap(s2))
	require.Equal(t, 104*elementSize, pool.CurrentEstimatedMemoryConsumptionBytes)
	require.Equal(t, 104*elementSize, pool.PeakEstimatedMemoryConsumptionBytes)

	// Put a slice back into the pool, the current stat should be updated but peak should be unchanged.
	put(s100)
	require.Equal(t, 3*elementSize, pool.CurrentEstimatedMemoryConsumptionBytes)
	require.Equal(t, 104*elementSize, pool.PeakEstimatedMemoryConsumptionBytes)

	// Get another slice from the pool that doesn't take us over the previous peak.
	s5, err := get(5)
	require.NoError(t, err)
	require.Equal(t, 6, cap(s5))
	require.Equal(t, 9*elementSize, pool.CurrentEstimatedMemoryConsumptionBytes)
	require.Equal(t, 104*elementSize, pool.PeakEstimatedMemoryConsumptionBytes)

	// Get another slice from the pool that does take us over the previous peak.
	s200, err := get(200)
	require.NoError(t, err)
	require.Equal(t, 201, cap(s200))
	require.Equal(t, 210*elementSize, pool.CurrentEstimatedMemoryConsumptionBytes)
	require.Equal(t, 210*elementSize, pool.PeakEstimatedMemoryConsumptionBytes)

	// Ensure we handle nil slices safely.
	put(nil)
	require.Equal(t, 210*elementSize, pool.CurrentEstimatedMemoryConsumptionBytes)
	require.Equal(t, 210*elementSize, pool.PeakEstimatedMemoryConsumptionBytes)
}

func TestLimitingPool_Limited(t *testing.T) {
	setupLimitingPoolFunctionsForTesting(t)

	t.Run("[]promql.FPoint", func(t *testing.T) {
		pool := NewLimitingPool(10 * fPointSize)
		testLimitedPool(t, pool.GetFPointSlice, pool.PutFPointSlice, pool, fPointSize)
	})

	t.Run("[]promql.HPoint", func(t *testing.T) {
		pool := NewLimitingPool(10 * hPointSize)
		testLimitedPool(t, pool.GetHPointSlice, pool.PutHPointSlice, pool, hPointSize)
	})

	t.Run("promql.Vector", func(t *testing.T) {
		pool := NewLimitingPool(10 * vectorSampleSize)
		testLimitedPool(t, pool.GetVector, pool.PutVector, pool, vectorSampleSize)
	})

	t.Run("[]float64", func(t *testing.T) {
		pool := NewLimitingPool(10 * float64Size)
		testLimitedPool(t, pool.GetFloatSlice, pool.PutFloatSlice, pool, float64Size)
	})

	t.Run("[]bool", func(t *testing.T) {
		pool := NewLimitingPool(10 * boolSize)
		testLimitedPool(t, pool.GetBoolSlice, pool.PutBoolSlice, pool, boolSize)
	})
}

func testLimitedPool[E any, S ~[]E](t *testing.T, get func(int) (S, error), put func(S), pool *LimitingPool, elementSize uint64) {
	// Get a slice from the pool beneath the limit.
	s7, err := get(7)
	require.NoError(t, err)
	require.Equal(t, 8, cap(s7))
	require.Equal(t, 8*elementSize, pool.CurrentEstimatedMemoryConsumptionBytes)
	require.Equal(t, 8*elementSize, pool.PeakEstimatedMemoryConsumptionBytes)

	// Get another slice from the pool beneath the limit.
	s1, err := get(1)
	require.NoError(t, err)
	require.Equal(t, 2, cap(s1))
	require.Equal(t, 10*elementSize, pool.CurrentEstimatedMemoryConsumptionBytes)
	require.Equal(t, 10*elementSize, pool.PeakEstimatedMemoryConsumptionBytes)

	// Return a slice to the pool.
	put(s1)
	require.Equal(t, 8*elementSize, pool.CurrentEstimatedMemoryConsumptionBytes)
	require.Equal(t, 10*elementSize, pool.PeakEstimatedMemoryConsumptionBytes)

	// Try to get a slice where the requested size would push us over the limit.
	_, err = get(3)
	expectedError := fmt.Sprintf("the query exceeded the maximum allowed estimated amount of memory consumed by a single query (limit: %d bytes) (err-mimir-max-estimated-memory-consumption-per-query)", pool.MaxEstimatedMemoryConsumptionBytes)
	require.ErrorContains(t, err, expectedError)
	require.Equal(t, 8*elementSize, pool.CurrentEstimatedMemoryConsumptionBytes)
	require.Equal(t, 10*elementSize, pool.PeakEstimatedMemoryConsumptionBytes)

	// Try to get a slice where the requested size is under the limit, but the capacity of the slice returned by the pool is over the limit.
	_, err = get(2)
	require.ErrorContains(t, err, expectedError)
	require.Equal(t, 8*elementSize, pool.CurrentEstimatedMemoryConsumptionBytes)
	require.Equal(t, 10*elementSize, pool.PeakEstimatedMemoryConsumptionBytes)

	// Get another slice from the pool that would take us right up to the limit.
	s1, err = get(1)
	require.NoError(t, err)
	require.Equal(t, 2, cap(s1))
	require.Equal(t, 10*elementSize, pool.CurrentEstimatedMemoryConsumptionBytes)
	require.Equal(t, 10*elementSize, pool.PeakEstimatedMemoryConsumptionBytes)
}

func TestLimitingPool_ClearsReturnedSlices(t *testing.T) {
	pool := NewLimitingPool(0)

	// Get a slice, put it back in the pool and get it back again.
	// Make sure all elements are zero or false when we get it back.
	t.Run("[]float64", func(t *testing.T) {
		floatSlice, err := pool.GetFloatSlice(2)
		require.NoError(t, err)
		floatSlice = floatSlice[:2]
		floatSlice[0] = 123
		floatSlice[1] = 456

		pool.PutFloatSlice(floatSlice)

		floatSlice, err = pool.GetFloatSlice(2)
		require.NoError(t, err)
		floatSlice = floatSlice[:2]
		require.Equal(t, []float64{0, 0}, floatSlice)
	})

	t.Run("[]bool", func(t *testing.T) {
		boolSlice, err := pool.GetBoolSlice(2)
		require.NoError(t, err)
		boolSlice = boolSlice[:2]
		boolSlice[0] = false
		boolSlice[1] = true

		pool.PutBoolSlice(boolSlice)

		boolSlice, err = pool.GetBoolSlice(2)
		require.NoError(t, err)
		boolSlice = boolSlice[:2]
		require.Equal(t, []bool{false, false}, boolSlice)
	})
}

// setupLimitingPoolFunctionsForTesting replaces the global slice pools used by LimitingPool
// with fakes for testing.
//
// Each returned slice will have capacity = requested size + 1.
func setupLimitingPoolFunctionsForTesting(t *testing.T) {
	originalFPointSlicePool := fPointSlicePool
	originalHPointSlicePool := hPointSlicePool
	originalVectorPool := vectorPool
	originalFloatSlicePool := float64SlicePool
	originalBoolSlicePool := boolSlicePool

	fPointSlicePool = dummyPoolForLimitingPoolTests[promql.FPoint, []promql.FPoint]{}
	hPointSlicePool = dummyPoolForLimitingPoolTests[promql.HPoint, []promql.HPoint]{}
	vectorPool = dummyPoolForLimitingPoolTests[promql.Sample, promql.Vector]{}
	float64SlicePool = dummyPoolForLimitingPoolTests[float64, []float64]{}
	boolSlicePool = dummyPoolForLimitingPoolTests[bool, []bool]{}

	t.Cleanup(func() {
		fPointSlicePool = originalFPointSlicePool
		hPointSlicePool = originalHPointSlicePool
		vectorPool = originalVectorPool
		float64SlicePool = originalFloatSlicePool
		boolSlicePool = originalBoolSlicePool
	})
}

type dummyPoolForLimitingPoolTests[E any, S ~[]E] struct{}

func (d dummyPoolForLimitingPoolTests[E, S]) Get(size int) S {
	return make(S, 0, size+1)
}

func (d dummyPoolForLimitingPoolTests[E, S]) Put(_ S) {
	// Drop slice on the floor - we don't need it.
}
