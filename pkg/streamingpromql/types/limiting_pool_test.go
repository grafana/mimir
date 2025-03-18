// SPDX-License-Identifier: AGPL-3.0-only

package types

import (
	"fmt"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/util/pool"
)

const rejectedQueriesMetricName = "rejected_queries"

func TestLimitingBucketedPool_Unlimited(t *testing.T) {
	reg, metric := createRejectedMetric()
	tracker := limiting.NewMemoryConsumptionTracker(0, metric)

	p := NewLimitingBucketedPool(
		pool.NewBucketedPool(1024, func(size int) []promql.FPoint { return make([]promql.FPoint, 0, size) }),
		FPointSize,
		false,
		nil,
	)

	// Get a slice from the pool, the current and peak stats should be updated based on the capacity of the slice returned, not the size requested.
	s100, err := p.Get(100, tracker)
	require.NoError(t, err)
	require.Equal(t, 128, cap(s100))
	require.Equal(t, 128*FPointSize, tracker.CurrentEstimatedMemoryConsumptionBytes)
	require.Equal(t, 128*FPointSize, tracker.PeakEstimatedMemoryConsumptionBytes)

	// Get another slice from the pool, the current and peak stats should be updated.
	s2, err := p.Get(2, tracker)
	require.NoError(t, err)
	require.Equal(t, 2, cap(s2))
	require.Equal(t, 130*FPointSize, tracker.CurrentEstimatedMemoryConsumptionBytes)
	require.Equal(t, 130*FPointSize, tracker.PeakEstimatedMemoryConsumptionBytes)

	// Put a slice back into the pool, the current stat should be updated but peak should be unchanged.
	p.Put(s100, tracker)
	require.Equal(t, 2*FPointSize, tracker.CurrentEstimatedMemoryConsumptionBytes)
	require.Equal(t, 130*FPointSize, tracker.PeakEstimatedMemoryConsumptionBytes)

	// Get another slice from the pool that doesn't take us over the previous peak.
	s5, err := p.Get(5, tracker)
	require.NoError(t, err)
	require.Equal(t, 8, cap(s5))
	require.Equal(t, 10*FPointSize, tracker.CurrentEstimatedMemoryConsumptionBytes)
	require.Equal(t, 130*FPointSize, tracker.PeakEstimatedMemoryConsumptionBytes)

	// Get another slice from the pool that does take us over the previous peak.
	s200, err := p.Get(200, tracker)
	require.NoError(t, err)
	require.Equal(t, 256, cap(s200))
	require.Equal(t, 266*FPointSize, tracker.CurrentEstimatedMemoryConsumptionBytes)
	require.Equal(t, 266*FPointSize, tracker.PeakEstimatedMemoryConsumptionBytes)

	// Ensure we handle nil slices safely.
	p.Put(nil, tracker)
	require.Equal(t, 266*FPointSize, tracker.CurrentEstimatedMemoryConsumptionBytes)
	require.Equal(t, 266*FPointSize, tracker.PeakEstimatedMemoryConsumptionBytes)

	assertRejectedQueryCount(t, reg, 0)
}

func TestLimitingPool_Limited(t *testing.T) {
	reg, metric := createRejectedMetric()
	limit := 11 * FPointSize
	tracker := limiting.NewMemoryConsumptionTracker(limit, metric)

	p := NewLimitingBucketedPool(
		pool.NewBucketedPool(1024, func(size int) []promql.FPoint { return make([]promql.FPoint, 0, size) }),
		FPointSize,
		false,
		nil,
	)

	// Get a slice from the pool beneath the limit.
	s7, err := p.Get(7, tracker)
	require.NoError(t, err)
	require.Equal(t, 8, cap(s7))
	require.Equal(t, 8*FPointSize, tracker.CurrentEstimatedMemoryConsumptionBytes)
	require.Equal(t, 8*FPointSize, tracker.PeakEstimatedMemoryConsumptionBytes)
	assertRejectedQueryCount(t, reg, 0)

	// Get another slice from the pool beneath the limit.
	s1, err := p.Get(1, tracker)
	require.NoError(t, err)
	require.Equal(t, 1, cap(s1))
	require.Equal(t, 9*FPointSize, tracker.CurrentEstimatedMemoryConsumptionBytes)
	require.Equal(t, 9*FPointSize, tracker.PeakEstimatedMemoryConsumptionBytes)
	assertRejectedQueryCount(t, reg, 0)

	// Return a slice to the pool.
	p.Put(s1, tracker)
	require.Equal(t, 8*FPointSize, tracker.CurrentEstimatedMemoryConsumptionBytes)
	require.Equal(t, 9*FPointSize, tracker.PeakEstimatedMemoryConsumptionBytes)
	assertRejectedQueryCount(t, reg, 0)

	// Try to get a slice where the requested size would push us over the limit.
	_, err = p.Get(4, tracker)
	expectedError := fmt.Sprintf("the query exceeded the maximum allowed estimated amount of memory consumed by a single query (limit: %d bytes) (err-mimir-max-estimated-memory-consumption-per-query)", limit)
	require.ErrorContains(t, err, expectedError)
	require.Equal(t, 8*FPointSize, tracker.CurrentEstimatedMemoryConsumptionBytes)
	require.Equal(t, 9*FPointSize, tracker.PeakEstimatedMemoryConsumptionBytes)
	assertRejectedQueryCount(t, reg, 1)

	// Try to get a slice where the requested size is under the limit, but the capacity of the slice returned by the pool is over the limit.
	// (We expect the pool to be configured with a factor of 2, so a slice of size 3 will be rounded up to 4 elements.)
	_, err = p.Get(3, tracker)
	require.ErrorContains(t, err, expectedError)
	require.Equal(t, 8*FPointSize, tracker.CurrentEstimatedMemoryConsumptionBytes)
	require.Equal(t, 9*FPointSize, tracker.PeakEstimatedMemoryConsumptionBytes)

	// Make sure we don't increment the rejection count a second time for the same query.
	assertRejectedQueryCount(t, reg, 1)

	// Keep getting more slices from the pool up to the limit of 11 to make sure the failed allocations weren't counted.
	for i := 0; i < 3; i++ {
		s1, err = p.Get(1, tracker)
		require.NoError(t, err)
		require.Equal(t, 1, cap(s1))
		require.Equal(t, uint64(9+i)*FPointSize, tracker.CurrentEstimatedMemoryConsumptionBytes)
		require.Equal(t, uint64(9+i)*FPointSize, tracker.PeakEstimatedMemoryConsumptionBytes)
	}

	// Try to get another slice while we're already at the limit.
	_, err = p.Get(1, tracker)
	require.ErrorContains(t, err, expectedError)
	require.Equal(t, 11*FPointSize, tracker.CurrentEstimatedMemoryConsumptionBytes)
	require.Equal(t, 11*FPointSize, tracker.PeakEstimatedMemoryConsumptionBytes)
	assertRejectedQueryCount(t, reg, 1)
}

func TestLimitingPool_ClearsReturnedSlices(t *testing.T) {
	tracker := limiting.NewMemoryConsumptionTracker(0, nil)

	// Get a slice, put it back in the pool and get it back again.
	// Make sure all elements are zero or false when we get it back.
	t.Run("[]float64", func(t *testing.T) {
		s, err := Float64SlicePool.Get(2, tracker)
		require.NoError(t, err)
		s = s[:2]
		s[0] = 123
		s[1] = 456

		Float64SlicePool.Put(s, tracker)

		s, err = Float64SlicePool.Get(2, tracker)
		require.NoError(t, err)
		s = s[:2]
		require.Equal(t, []float64{0, 0}, s)
	})

	t.Run("[]bool", func(t *testing.T) {
		s, err := BoolSlicePool.Get(2, tracker)
		require.NoError(t, err)
		s = s[:2]
		s[0] = false
		s[1] = true

		BoolSlicePool.Put(s, tracker)

		s, err = BoolSlicePool.Get(2, tracker)
		require.NoError(t, err)
		s = s[:2]
		require.Equal(t, []bool{false, false}, s)
	})

	t.Run("[]*histogram.FloatHistogram", func(t *testing.T) {
		s, err := HistogramSlicePool.Get(2, tracker)
		require.NoError(t, err)
		s = s[:2]
		s[0] = &histogram.FloatHistogram{Count: 1}
		s[1] = &histogram.FloatHistogram{Count: 2}

		HistogramSlicePool.Put(s, tracker)

		s, err = HistogramSlicePool.Get(2, tracker)
		require.NoError(t, err)
		s = s[:2]
		require.Equal(t, []*histogram.FloatHistogram{nil, nil}, s)
	})
}

func TestLimitingPool_Mangling(t *testing.T) {
	currentEnableManglingReturnedSlices := EnableManglingReturnedSlices
	defer func() {
		// Ensure we reset this back to the default state given it applies globally.
		EnableManglingReturnedSlices = currentEnableManglingReturnedSlices
	}()

	_, metric := createRejectedMetric()
	tracker := limiting.NewMemoryConsumptionTracker(0, metric)

	p := NewLimitingBucketedPool(
		pool.NewBucketedPool(1024, func(size int) []int { return make([]int, 0, size) }),
		1,
		false,
		func(_ int) int { return 123 },
	)

	// Test with mangling disabled.
	EnableManglingReturnedSlices = false
	s, err := p.Get(4, tracker)
	require.NoError(t, err)
	s = append(s, 1000, 2000, 3000, 4000)

	p.Put(s, tracker)
	require.Equal(t, []int{1000, 2000, 3000, 4000}, s, "returned slice should not be mangled when mangling is disabled")

	// Test with mangling enabled.
	EnableManglingReturnedSlices = true
	s, err = p.Get(4, tracker)
	require.NoError(t, err)
	s = append(s, 1000, 2000, 3000, 4000)

	p.Put(s, tracker)
	require.Equal(t, []int{123, 123, 123, 123}, s, "returned slice should be mangled when mangling is enabled")
}

func TestLimitingBucketedPool_MaxExpectedPointsPerSeriesConstantIsPowerOfTwo(t *testing.T) {
	// Although not strictly required (as the code should handle MaxExpectedPointsPerSeries not being a power of two correctly),
	// it is best that we keep it as one for now.
	require.True(t, pool.IsPowerOfTwo(MaxExpectedPointsPerSeries), "MaxExpectedPointsPerSeries must be a power of two")
}

func assertRejectedQueryCount(t *testing.T, reg *prometheus.Registry, expectedRejectionCount int) {
	expected := fmt.Sprintf(`
		# TYPE %s counter
		%s %v
	`, rejectedQueriesMetricName, rejectedQueriesMetricName, expectedRejectionCount)
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expected), rejectedQueriesMetricName))
}

func createRejectedMetric() (*prometheus.Registry, prometheus.Counter) {
	reg := prometheus.NewPedanticRegistry()
	metric := promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: rejectedQueriesMetricName,
	})

	return reg, metric
}
