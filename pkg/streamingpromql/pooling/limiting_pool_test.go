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
		testUnlimitedPool(t, pool.GetFPointSlice, pool.PutFPointSlice, pool, 1)
	})

	t.Run("[]promql.HPoint", func(t *testing.T) {
		pool := NewLimitingPool(0)
		testUnlimitedPool(t, pool.GetHPointSlice, pool.PutHPointSlice, pool, 10)
	})

	t.Run("promql.Vector", func(t *testing.T) {
		pool := NewLimitingPool(0)
		testUnlimitedPool(t, pool.GetVector, pool.PutVector, pool, 1)
	})

	t.Run("[]float64", func(t *testing.T) {
		pool := NewLimitingPool(0)
		testUnlimitedPoolWithIntrinsicTypeSlice(t, pool.GetFloatSlice, pool.PutFloatSlice, pool)
	})

	t.Run("[]bool", func(t *testing.T) {
		pool := NewLimitingPool(0)
		testUnlimitedPoolWithIntrinsicTypeSlice(t, pool.GetBoolSlice, pool.PutBoolSlice, pool)
	})
}

func testUnlimitedPool[E any, S ~[]E](t *testing.T, get func(int) (S, error), put func(S), pool *LimitingPool, multiplier int) {
	// Get a slice from the pool, the current and peak stats should be updated based on the capacity of the slice returned, not the size requested.
	s100, err := get(100)
	require.NoError(t, err)
	require.Equal(t, 101, cap(s100))
	require.Equal(t, 101*multiplier, pool.CurrentInMemorySamples)
	require.Equal(t, 101*multiplier, pool.PeakInMemorySamples)

	// Get another slice from the pool, the current and peak stats should be updated.
	s2, err := get(2)
	require.NoError(t, err)
	require.Equal(t, 3, cap(s2))
	require.Equal(t, 104*multiplier, pool.CurrentInMemorySamples)
	require.Equal(t, 104*multiplier, pool.PeakInMemorySamples)

	// Put a slice back into the pool, the current stat should be updated but peak should be unchanged.
	put(s100)
	require.Equal(t, 3*multiplier, pool.CurrentInMemorySamples)
	require.Equal(t, 104*multiplier, pool.PeakInMemorySamples)

	// Get another slice from the pool that doesn't take us over the previous peak.
	s5, err := get(5)
	require.NoError(t, err)
	require.Equal(t, 6, cap(s5))
	require.Equal(t, 9*multiplier, pool.CurrentInMemorySamples)
	require.Equal(t, 104*multiplier, pool.PeakInMemorySamples)

	// Get another slice from the pool that does take us over the previous peak.
	s200, err := get(200)
	require.NoError(t, err)
	require.Equal(t, 201, cap(s200))
	require.Equal(t, 210*multiplier, pool.CurrentInMemorySamples)
	require.Equal(t, 210*multiplier, pool.PeakInMemorySamples)

	// Ensure we handle nil slices safely.
	put(nil)
	require.Equal(t, 210*multiplier, pool.CurrentInMemorySamples)
	require.Equal(t, 210*multiplier, pool.PeakInMemorySamples)
}

func testUnlimitedPoolWithIntrinsicTypeSlice[E any](t *testing.T, get func(int) ([]E, error), put func([]E), pool *LimitingPool) {
	// Get a slice from the pool, the current and peak stats should be updated based on the capacity of the slice returned, not the size requested.
	s100, err := get(100)
	require.NoError(t, err)
	require.Equal(t, 101, cap(s100))
	require.Equal(t, 50, pool.CurrentInMemorySamples)
	require.Equal(t, 50, pool.PeakInMemorySamples)

	// Get another slice from the pool, the current and peak stats should be updated.
	s2, err := get(2)
	require.NoError(t, err)
	require.Equal(t, 3, cap(s2))
	require.Equal(t, 51, pool.CurrentInMemorySamples)
	require.Equal(t, 51, pool.PeakInMemorySamples)

	// Put a slice back into the pool, the current stat should be updated but peak should be unchanged.
	put(s100)
	require.Equal(t, 1, pool.CurrentInMemorySamples)
	require.Equal(t, 51, pool.PeakInMemorySamples)

	// Get another slice from the pool that doesn't take us over the previous peak.
	s5, err := get(5)
	require.NoError(t, err)
	require.Equal(t, 6, cap(s5))
	require.Equal(t, 4, pool.CurrentInMemorySamples)
	require.Equal(t, 51, pool.PeakInMemorySamples)

	// Get another slice from the pool that does take us over the previous peak.
	s200, err := get(200)
	require.NoError(t, err)
	require.Equal(t, 201, cap(s200))
	require.Equal(t, 104, pool.CurrentInMemorySamples)
	require.Equal(t, 104, pool.PeakInMemorySamples)

	// Ensure we handle nil slices safely.
	put(nil)
	require.Equal(t, 104, pool.CurrentInMemorySamples)
	require.Equal(t, 104, pool.PeakInMemorySamples)

	// Getting a slice of size 1 should count as at least one sample.
	s1, err := get(1)
	require.NoError(t, err)
	require.Equal(t, 2, cap(s1))
	require.Equal(t, 105, pool.CurrentInMemorySamples)
	require.Equal(t, 105, pool.PeakInMemorySamples)

	// Putting a slice of size 1 should count as one sample.
	put(make([]E, 1))
	require.Equal(t, 104, pool.CurrentInMemorySamples)
	require.Equal(t, 105, pool.PeakInMemorySamples)
}

func TestLimitingPool_Limited_FPointSlices(t *testing.T) {
	setupLimitingPoolFunctionsForTesting(t)

	t.Run("[]promql.FPoint", func(t *testing.T) {
		pool := NewLimitingPool(10)
		testLimitedPool(t, pool.GetFPointSlice, pool.PutFPointSlice, pool, 1)
	})

	t.Run("[]promql.HPoint", func(t *testing.T) {
		pool := NewLimitingPool(100)
		testLimitedPool(t, pool.GetHPointSlice, pool.PutHPointSlice, pool, 10)
	})

	t.Run("promql.Vector", func(t *testing.T) {
		pool := NewLimitingPool(10)
		testLimitedPool(t, pool.GetVector, pool.PutVector, pool, 1)
	})

	t.Run("[]float64", func(t *testing.T) {
		pool := NewLimitingPool(10)
		testLimitedPoolWithIntrinsicTypeSlice(t, pool.GetFloatSlice, pool.PutFloatSlice, pool)
	})

	t.Run("[]bool", func(t *testing.T) {
		pool := NewLimitingPool(10)
		testLimitedPoolWithIntrinsicTypeSlice(t, pool.GetBoolSlice, pool.PutBoolSlice, pool)
	})
}

func testLimitedPool[E any, S ~[]E](t *testing.T, get func(int) (S, error), put func(S), pool *LimitingPool, multiplier int) {
	// Get a slice from the pool beneath the limit.
	s7, err := get(7)
	require.NoError(t, err)
	require.Equal(t, 8, cap(s7))
	require.Equal(t, 8*multiplier, pool.CurrentInMemorySamples)
	require.Equal(t, 8*multiplier, pool.PeakInMemorySamples)

	// Get another slice from the pool beneath the limit.
	s1, err := get(1)
	require.NoError(t, err)
	require.Equal(t, 2, cap(s1))
	require.Equal(t, 10*multiplier, pool.CurrentInMemorySamples)
	require.Equal(t, 10*multiplier, pool.PeakInMemorySamples)

	// Return a slice to the pool.
	put(s1)
	require.Equal(t, 8*multiplier, pool.CurrentInMemorySamples)
	require.Equal(t, 10*multiplier, pool.PeakInMemorySamples)

	// Try to get a slice where the requested size would push us over the limit.
	_, err = get(3)
	expectedError := fmt.Sprintf("the query exceeded the maximum allowed number of in-memory samples (limit: %d samples) (err-mimir-max-in-memory-samples-per-query)", pool.MaxInMemorySamples)
	require.ErrorContains(t, err, expectedError)
	require.Equal(t, 8*multiplier, pool.CurrentInMemorySamples)
	require.Equal(t, 10*multiplier, pool.PeakInMemorySamples)

	// Try to get a slice where the requested size is under the limit, but the capacity of the slice returned by the pool is over the limit.
	_, err = get(2)
	require.ErrorContains(t, err, expectedError)
	require.Equal(t, 8*multiplier, pool.CurrentInMemorySamples)
	require.Equal(t, 10*multiplier, pool.PeakInMemorySamples)

	// Get another slice from the pool that would take us right up to the limit.
	s1, err = get(1)
	require.NoError(t, err)
	require.Equal(t, 2, cap(s1))
	require.Equal(t, 10*multiplier, pool.CurrentInMemorySamples)
	require.Equal(t, 10*multiplier, pool.PeakInMemorySamples)
}

func testLimitedPoolWithIntrinsicTypeSlice[E any](t *testing.T, get func(int) ([]E, error), put func([]E), pool *LimitingPool) {
	// Get a slice from the pool beneath the limit.
	s7, err := get(7)
	require.NoError(t, err)
	require.Equal(t, 8, cap(s7))
	require.Equal(t, 4, pool.CurrentInMemorySamples)
	require.Equal(t, 4, pool.PeakInMemorySamples)

	// Get another slice from the pool beneath the limit.
	s1, err := get(1)
	require.NoError(t, err)
	require.Equal(t, 2, cap(s1))
	require.Equal(t, 5, pool.CurrentInMemorySamples)
	require.Equal(t, 5, pool.PeakInMemorySamples)

	// Return a slice to the pool.
	put(s1)
	require.Equal(t, 4, pool.CurrentInMemorySamples)
	require.Equal(t, 5, pool.PeakInMemorySamples)

	// Try to get a slice where the requested size would push us over the limit.
	_, err = get(14)
	expectedError := fmt.Sprintf("the query exceeded the maximum allowed number of in-memory samples (limit: %d samples) (err-mimir-max-in-memory-samples-per-query)", pool.MaxInMemorySamples)
	require.ErrorContains(t, err, expectedError)
	require.Equal(t, 4, pool.CurrentInMemorySamples)
	require.Equal(t, 5, pool.PeakInMemorySamples)

	// Try to get a slice where the requested size is under the limit, but the capacity of the slice returned by the pool is over the limit.
	_, err = get(13)
	require.ErrorContains(t, err, expectedError)
	require.Equal(t, 4, pool.CurrentInMemorySamples)
	require.Equal(t, 5, pool.PeakInMemorySamples)

	// Get another slice from the pool that would take us right up to the limit.
	s1, err = get(11)
	require.NoError(t, err)
	require.Equal(t, 12, cap(s1))
	require.Equal(t, 10, pool.CurrentInMemorySamples)
	require.Equal(t, 10, pool.PeakInMemorySamples)
}

// setupLimitingPoolFunctionsForTesting replaces the global slice pools used by LimitingPool
// with fakes for testing.
//
// Each returned slice will have capacity = requested size + 1.
func setupLimitingPoolFunctionsForTesting(t *testing.T) {
	originalFPointSlicePool := fPointSlicePool
	originalHPointSlicePool := hPointSlicePool
	originalVectorPool := vectorPool
	originalFloatSlicePool := floatSlicePool
	originalBoolSlicePool := boolSlicePool

	fPointSlicePool = dummyPoolForLimitingPoolTests[promql.FPoint, []promql.FPoint]{}
	hPointSlicePool = dummyPoolForLimitingPoolTests[promql.HPoint, []promql.HPoint]{}
	vectorPool = dummyPoolForLimitingPoolTests[promql.Sample, promql.Vector]{}
	floatSlicePool = dummyPoolForLimitingPoolTests[float64, []float64]{}
	boolSlicePool = dummyPoolForLimitingPoolTests[bool, []bool]{}

	t.Cleanup(func() {
		fPointSlicePool = originalFPointSlicePool
		hPointSlicePool = originalHPointSlicePool
		vectorPool = originalVectorPool
		floatSlicePool = originalFloatSlicePool
		boolSlicePool = originalBoolSlicePool
	})
}

type dummyPoolForLimitingPoolTests[E any, S ~[]E] struct{}

func (d dummyPoolForLimitingPoolTests[E, S]) Get(size int) S {
	return make(S, 0, size+1)
}

func (d dummyPoolForLimitingPoolTests[E, S]) Put(s S) {
	// Drop slice on the floor - we don't need it.
}
