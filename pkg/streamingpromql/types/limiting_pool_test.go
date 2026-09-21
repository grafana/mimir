// SPDX-License-Identifier: AGPL-3.0-only

package types

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"unsafe"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/util/limiter"
	"github.com/grafana/mimir/pkg/util/pool"
)

const rejectedQueriesMetricName = "rejected_queries"

func TestLimitingBucketedPool_Unlimited(t *testing.T) {
	reg, metric := createRejectedMetric()
	tracker := limiter.NewMemoryConsumptionTracker(context.Background(), 0, metric, "")

	p := NewLimitingBucketedPool(
		pool.NewBucketedPool(1024, func(size int) []promql.FPoint { return make([]promql.FPoint, 0, size) }),
		limiter.FPointSlices,
		FPointSize,
		false,
		atomic.NewBool(true),
		func(point promql.FPoint) promql.FPoint { return point },
		nil,
	)

	// Get a slice from the pool, the current and peak stats should be updated based on the capacity of the slice returned, not the size requested.
	s100, err := p.Get(100, tracker)
	require.NoError(t, err)
	require.Equal(t, 128, cap(s100))
	require.Equal(t, 128*FPointSize, tracker.CurrentEstimatedMemoryConsumptionBytes())
	require.Equal(t, 128*FPointSize, tracker.PeakEstimatedMemoryConsumptionBytes())

	// Get another slice from the pool, the current and peak stats should be updated.
	s2, err := p.Get(2, tracker)
	require.NoError(t, err)
	require.Equal(t, 2, cap(s2))
	require.Equal(t, 130*FPointSize, tracker.CurrentEstimatedMemoryConsumptionBytes())
	require.Equal(t, 130*FPointSize, tracker.PeakEstimatedMemoryConsumptionBytes())

	// Put a slice back into the pool, the current stat should be updated but peak should be unchanged.
	p.Put(&s100, tracker)
	require.Equal(t, 2*FPointSize, tracker.CurrentEstimatedMemoryConsumptionBytes())
	require.Equal(t, 130*FPointSize, tracker.PeakEstimatedMemoryConsumptionBytes())

	// Get another slice from the pool that doesn't take us over the previous peak.
	s5, err := p.Get(5, tracker)
	require.NoError(t, err)
	require.Equal(t, 8, cap(s5))
	require.Equal(t, 10*FPointSize, tracker.CurrentEstimatedMemoryConsumptionBytes())
	require.Equal(t, 130*FPointSize, tracker.PeakEstimatedMemoryConsumptionBytes())

	// Get another slice from the pool that does take us over the previous peak.
	s200, err := p.Get(200, tracker)
	require.NoError(t, err)
	require.Equal(t, 256, cap(s200))
	require.Equal(t, 266*FPointSize, tracker.CurrentEstimatedMemoryConsumptionBytes())
	require.Equal(t, 266*FPointSize, tracker.PeakEstimatedMemoryConsumptionBytes())

	// Ensure we handle nil slices safely.
	p.Put(nil, tracker)
	require.Equal(t, 266*FPointSize, tracker.CurrentEstimatedMemoryConsumptionBytes())
	require.Equal(t, 266*FPointSize, tracker.PeakEstimatedMemoryConsumptionBytes())

	assertRejectedQueryCount(t, reg, 0)
}

func TestLimitingPool_Limited(t *testing.T) {
	reg, metric := createRejectedMetric()
	limit := 11 * FPointSize
	tracker := limiter.NewMemoryConsumptionTracker(context.Background(), limit, metric, "")

	p := NewLimitingBucketedPool(
		pool.NewBucketedPool(1024, func(size int) []promql.FPoint { return make([]promql.FPoint, 0, size) }),
		limiter.FPointSlices,
		FPointSize,
		false,
		atomic.NewBool(true),
		func(point promql.FPoint) promql.FPoint { return point },
		nil,
	)

	// Get a slice from the pool beneath the limit.
	s7, err := p.Get(7, tracker)
	require.NoError(t, err)
	require.Equal(t, 8, cap(s7))
	require.Equal(t, 8*FPointSize, tracker.CurrentEstimatedMemoryConsumptionBytes())
	require.Equal(t, 8*FPointSize, tracker.PeakEstimatedMemoryConsumptionBytes())
	assertRejectedQueryCount(t, reg, 0)

	// Get another slice from the pool beneath the limit.
	s1, err := p.Get(1, tracker)
	require.NoError(t, err)
	require.Equal(t, 1, cap(s1))
	require.Equal(t, 9*FPointSize, tracker.CurrentEstimatedMemoryConsumptionBytes())
	require.Equal(t, 9*FPointSize, tracker.PeakEstimatedMemoryConsumptionBytes())
	assertRejectedQueryCount(t, reg, 0)

	// Return a slice to the pool.
	p.Put(&s1, tracker)
	require.Equal(t, 8*FPointSize, tracker.CurrentEstimatedMemoryConsumptionBytes())
	require.Equal(t, 9*FPointSize, tracker.PeakEstimatedMemoryConsumptionBytes())
	assertRejectedQueryCount(t, reg, 0)

	// Try to get a slice where the requested size would push us over the limit.
	_, err = p.Get(4, tracker)
	expectedError := fmt.Sprintf("the query exceeded the maximum allowed estimated amount of memory consumed by a single query (limit: %d bytes) (err-mimir-max-estimated-memory-consumption-per-query)", limit)
	require.ErrorContains(t, err, expectedError)
	require.Equal(t, 8*FPointSize, tracker.CurrentEstimatedMemoryConsumptionBytes())
	require.Equal(t, 9*FPointSize, tracker.PeakEstimatedMemoryConsumptionBytes())
	assertRejectedQueryCount(t, reg, 1)

	// Try to get a slice where the requested size is under the limit, but the capacity of the slice returned by the pool is over the limit.
	// (We expect the pool to be configured with a factor of 2, so a slice of size 3 will be rounded up to 4 elements.)
	_, err = p.Get(3, tracker)
	require.ErrorContains(t, err, expectedError)
	require.Equal(t, 8*FPointSize, tracker.CurrentEstimatedMemoryConsumptionBytes())
	require.Equal(t, 9*FPointSize, tracker.PeakEstimatedMemoryConsumptionBytes())

	// Make sure we don't increment the rejection count a second time for the same query.
	assertRejectedQueryCount(t, reg, 1)

	// Keep getting more slices from the pool up to the limit of 11 to make sure the failed allocations weren't counted.
	for i := 0; i < 3; i++ {
		s1, err = p.Get(1, tracker)
		require.NoError(t, err)
		require.Equal(t, 1, cap(s1))
		require.Equal(t, uint64(9+i)*FPointSize, tracker.CurrentEstimatedMemoryConsumptionBytes())
		require.Equal(t, uint64(9+i)*FPointSize, tracker.PeakEstimatedMemoryConsumptionBytes())
	}

	// Try to get another slice while we're already at the limit.
	_, err = p.Get(1, tracker)
	require.ErrorContains(t, err, expectedError)
	require.Equal(t, 11*FPointSize, tracker.CurrentEstimatedMemoryConsumptionBytes())
	require.Equal(t, 11*FPointSize, tracker.PeakEstimatedMemoryConsumptionBytes())
	assertRejectedQueryCount(t, reg, 1)
}

func TestLimitingPool_ClearsReturnedSlices(t *testing.T) {
	tracker := limiter.NewUnlimitedMemoryConsumptionTracker(context.Background())

	// Get a slice, put it back in the pool and get it back again.
	// Make sure all elements are zero or false when we get it back.
	t.Run("[]float64", func(t *testing.T) {
		s, err := Float64SlicePool.Get(2, tracker)
		require.NoError(t, err)
		s = s[:2]
		s[0] = 123
		s[1] = 456

		Float64SlicePool.Put(&s, tracker)
		require.Nil(t, s, "reference to slice should be cleared on Put")

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

		BoolSlicePool.Put(&s, tracker)
		require.Nil(t, s, "reference to slice should be cleared on Put")

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

		HistogramSlicePool.Put(&s, tracker)
		require.Nil(t, s, "reference to slice should be cleared on Put")

		s, err = HistogramSlicePool.Get(2, tracker)
		require.NoError(t, err)
		s = s[:2]
		require.Equal(t, []*histogram.FloatHistogram{nil, nil}, s)
	})
}

func TestLimitingPool_Mangling(t *testing.T) {
	_, metric := createRejectedMetric()

	t.Run("with mangling function", func(t *testing.T) {
		tracker := limiter.NewMemoryConsumptionTracker(context.Background(), 0, metric, "")

		p := NewLimitingBucketedPool(
			pool.NewBucketedPool(1024, func(size int) []int { return make([]int, 0, size) }),
			limiter.IntSlices,
			1,
			false,
			atomic.NewBool(false),
			func(_ int) int { return 123 },

			func(s []int, _ *limiter.MemoryConsumptionTracker) {
				for idx, i := range s {
					require.NotEqualf(t, 123, i, "Put() hook should be called before mangling, but element at index %d was mangled already", idx)
				}
			},
		)

		// Test with mangling disabled.
		s, err := p.Get(4, tracker)
		require.NoError(t, err)
		s = append(s, 1000, 2000, 3000, 4000)
		sCopy := s // Take another reference to s, so that we can check that it was mangled. (Put will set s to nil when it is called below, so we need to take a copy.)

		p.Put(&s, tracker)
		require.Equal(t, []int{1000, 2000, 3000, 4000}, sCopy, "returned slice should not be mangled when mangling is disabled")
		require.Nil(t, s, "provided slice should be nil-ed out")

		// Test with mangling enabled.
		p.mangleOnPut.Store(true)
		s, err = p.Get(4, tracker)
		require.NoError(t, err)
		s = append(s, 1000, 2000, 3000, 4000)
		sCopy = s

		p.Put(&s, tracker)
		require.Equal(t, []int{123, 123, 123, 123}, sCopy, "returned slice should be mangled when mangling is enabled")
		require.Nil(t, s, "provided slice should be nil-ed out")
	})

	t.Run("without mangling function but 'clear on get' set", func(t *testing.T) {
		tracker := limiter.NewMemoryConsumptionTracker(context.Background(), 0, metric, "")

		p := NewLimitingBucketedPool(
			pool.NewBucketedPool(1024, func(size int) []int { return make([]int, 0, size) }),
			limiter.IntSlices,
			1,
			true,
			atomic.NewBool(false),
			func(_ int) int { return 123 },
			nil,
		)

		// Test with mangling disabled.
		s, err := p.Get(4, tracker)
		require.NoError(t, err)
		s = append(s, 1000, 2000, 3000, 4000)
		sCopy := s // Take another reference to s, so that we can check that it was mangled. (Put will set s to nil when it is called below, so we need to take a copy.)

		p.Put(&s, tracker)
		require.Equal(t, []int{1000, 2000, 3000, 4000}, sCopy, "returned slice should not be cleared when mangling is disabled")
		require.Nil(t, s, "provided slice should be nil-ed out")

		// Test with mangling enabled.
		p.mangleOnPut.Store(true)
		s, err = p.Get(4, tracker)
		require.NoError(t, err)
		s = append(s, 1000, 2000, 3000, 4000)
		sCopy = s

		p.Put(&s, tracker)
		require.Equal(t, []int{0, 0, 0, 0}, sCopy, "returned slice should be cleared when mangling is enabled")
		require.Nil(t, s, "provided slice should be nil-ed out")
	})
}

// TestLimitingBucketedPool_ReturnedSliceSafety covers how a slice returned to a pool could go wrong
// if a query aborts partway through (for example a recovered evaluation panic), checking each is
// prevented or made visible rather than silently corrupting a later query that reuses the memory:
//   - double return: the same backing slice is returned to the pool twice.
//   - leak: a slice is taken from the pool but never returned.
//   - use-after-return: a slice is read or written after being returned to the pool.
func TestLimitingBucketedPool_ReturnedSliceSafety(t *testing.T) {
	// The use-after-return case relies on returned slices being mangled (enabled package-wide in
	// init_test.go). Assert it so the test fails loudly rather than passing for the wrong reason.
	require.True(t, EnableManglingReturnedSlices.Load(), "these tests rely on slice mangling being enabled")

	t.Run("double return is detected, not silently corrupting the pool", func(t *testing.T) {
		_, metric := createRejectedMetric()
		tracker := limiter.NewMemoryConsumptionTracker(context.Background(), 0, metric, "double return test")

		s, err := FPointSlicePool.Get(4, tracker)
		require.NoError(t, err)
		s = append(s, promql.FPoint{T: 1, F: 1}, promql.FPoint{T: 2, F: 2}, promql.FPoint{T: 3, F: 3}, promql.FPoint{T: 4, F: 4})
		require.Greater(t, tracker.CurrentEstimatedMemoryConsumptionBytes(), uint64(0))

		// Keep a second reference to the backing slice, simulating a stale reference that outlives the
		// return (Put nils out the reference it is given, so we cannot rely on s).
		staleRef := s

		FPointSlicePool.Put(&s, tracker)
		require.Nil(t, s, "reference should be cleared on Put")
		require.Equal(t, uint64(0), tracker.CurrentEstimatedMemoryConsumptionBytes())

		// Returning the slice again must be caught by the memory tracker guard, which runs before the
		// slice reaches the pool, so the pool never hands it to two callers. It panics rather than
		// silently under-counting memory.
		var recovered any
		func() {
			defer func() { recovered = recover() }()
			FPointSlicePool.Put(&staleRef, tracker)
		}()
		require.NotNil(t, recovered, "returning a slice twice should panic")
		require.Contains(t, fmt.Sprint(recovered), "returned to a pool more than once")
	})

	t.Run("leaked slice is accounted and never reused by a later query", func(t *testing.T) {
		_, metric := createRejectedMetric()
		tracker := limiter.NewMemoryConsumptionTracker(context.Background(), 0, metric, "leak test")

		// Take a slice and never return it, as would happen if a query aborted before cleanup.
		leaked, err := Float64SlicePool.Get(4, tracker)
		require.NoError(t, err)
		leaked = leaked[:4]

		// The leak stays accounted in the per-query tracker, so it is detectable: pedantic Query.Close()
		// panics when consumption is non-zero after a query finishes.
		leakedBytes := tracker.CurrentEstimatedMemoryConsumptionBytes()
		require.Greater(t, leakedBytes, uint64(0))

		// The leaked slice is never returned, so a later Get cannot receive it and it cannot leak into
		// another query; the GC reclaims it once unreferenced.
		reused, err := Float64SlicePool.Get(4, tracker)
		require.NoError(t, err)
		require.NotSame(t, unsafe.SliceData(leaked), unsafe.SliceData(reused), "a leaked slice must not be handed out again")
		require.Equal(t, 2*leakedBytes, tracker.CurrentEstimatedMemoryConsumptionBytes(), "both slices remain accounted")

		Float64SlicePool.Put(&reused, tracker)
	})

	// A returned slice is poisoned so reading it after return sees obviously-wrong data, not plausible
	// stale values. Pools not cleared on Get (FPointSlicePool) overwrite with sentinels via a mangling
	// function; pools cleared on Get (Float64SlicePool) zero the contents. Both run only while
	// EnableManglingReturnedSlices is set (asserted above).
	t.Run("use-after-return is made visible", func(t *testing.T) {
		t.Run("mangling pool overwrites contents with sentinels", func(t *testing.T) {
			_, metric := createRejectedMetric()
			tracker := limiter.NewMemoryConsumptionTracker(context.Background(), 0, metric, "use-after-return mangle test")

			s, err := FPointSlicePool.Get(4, tracker)
			require.NoError(t, err)
			s = append(s, promql.FPoint{T: 1, F: 1}, promql.FPoint{T: 2, F: 2}, promql.FPoint{T: 3, F: 3}, promql.FPoint{T: 4, F: 4})

			staleRef := s // Reference retained past the return, then read below.
			FPointSlicePool.Put(&s, tracker)

			for i, p := range staleRef {
				require.Equalf(t, mangleInt64(0), p.T, "element %d timestamp should be mangled after return", i)
				require.Equalf(t, mangleFloat64(0), p.F, "element %d value should be mangled after return", i)
			}
		})

		t.Run("clear-on-get pool zeroes contents", func(t *testing.T) {
			_, metric := createRejectedMetric()
			tracker := limiter.NewMemoryConsumptionTracker(context.Background(), 0, metric, "use-after-return clear-on-get test")

			s, err := Float64SlicePool.Get(4, tracker)
			require.NoError(t, err)
			s = s[:4]
			s[0], s[1], s[2], s[3] = 10, 20, 30, 40

			staleRef := s // Reference retained past the return, then read below.
			Float64SlicePool.Put(&s, tracker)

			require.Equal(t, []float64{0, 0, 0, 0}, staleRef, "clear-on-get pool should zero returned contents so a stale read sees cleared data")
		})
	})
}

func TestLimitingBucketedPool_AppendToSlice(t *testing.T) {
	tracker := limiter.NewUnlimitedMemoryConsumptionTracker(context.Background())
	onPutHookSlices := [][]promql.FPoint{}
	p := NewLimitingBucketedPool(
		pool.NewBucketedPool(1024, func(size int) []promql.FPoint { return make([]promql.FPoint, 0, size) }),
		limiter.FPointSlices,
		FPointSize,
		false,
		atomic.NewBool(true),
		func(point promql.FPoint) promql.FPoint { return point },
		func(s []promql.FPoint, _ *limiter.MemoryConsumptionTracker) {
			onPutHookSlices = append(onPutHookSlices, s)
		},
	)

	s1, err := p.Get(2, tracker)
	require.NoError(t, err)
	require.Equal(t, 2, cap(s1))
	require.Equal(t, 2*FPointSize, tracker.CurrentEstimatedMemoryConsumptionBytes())

	s2, err := p.AppendToSlice(s1, tracker, promql.FPoint{T: 1, F: 1.0}, promql.FPoint{T: 2, F: 2.0})
	require.NoError(t, err)
	require.Len(t, s2, 2)
	require.Same(t, unsafe.SliceData(s1), unsafe.SliceData(s2))
	require.Equal(t, 2*FPointSize, tracker.CurrentEstimatedMemoryConsumptionBytes())

	s3, err := p.AppendToSlice(s2, tracker, promql.FPoint{T: 3, F: 3.0}, promql.FPoint{T: 4, F: 4.0}, promql.FPoint{T: 5, F: 5.0})
	require.NoError(t, err)
	require.Len(t, s3, 5)
	require.Equal(t, 8, cap(s3))
	require.NotSame(t, unsafe.SliceData(s2), unsafe.SliceData(s3))
	require.Equal(t, 8*FPointSize, tracker.CurrentEstimatedMemoryConsumptionBytes())
	require.Equal(t, []promql.FPoint{{T: 1, F: 1.0}, {T: 2, F: 2.0}, {T: 3, F: 3.0}, {T: 4, F: 4.0}, {T: 5, F: 5.0}}, s3)
	require.Len(t, onPutHookSlices, 1)
	require.Equal(t, onPutHookSlices[0], []promql.FPoint{{T: 0, F: 0}, {T: 0, F: 0}})

	// Get another slice from the pool.
	// This is likely (but not guaranteed) to get the s2 slice that was returned to the pool when AppendToSlice() was
	// called but s2's capacity was exceeded.
	s4, err := p.Get(2, tracker)
	require.NoError(t, err)
	require.Equal(t, 2, cap(s4))
	// Check the first element is empty (i.e. old data has been cleared).
	require.Equal(t, []promql.FPoint{{T: 0, F: 0}}, s4[:1])
	p.Put(&s4, tracker)

	p.Put(&s3, tracker)
	require.Equal(t, uint64(0), tracker.CurrentEstimatedMemoryConsumptionBytes())
}

func TestLimitingBucketedPool_AppendToSlice_Error(t *testing.T) {
	_, metric := createRejectedMetric()

	tracker := limiter.NewMemoryConsumptionTracker(context.Background(), 2*FPointSize, metric, "")
	p := NewLimitingBucketedPool(
		pool.NewBucketedPool(1024, func(size int) []promql.FPoint { return make([]promql.FPoint, 0, size) }),
		limiter.FPointSlices,
		FPointSize,
		false,
		atomic.NewBool(true),
		func(point promql.FPoint) promql.FPoint { return point },
		nil,
	)

	s, err := p.Get(2, tracker)
	require.NoError(t, err)
	require.Equal(t, 2, cap(s))
	require.Equal(t, 2*FPointSize, tracker.CurrentEstimatedMemoryConsumptionBytes())

	s, err = p.AppendToSlice(s, tracker, promql.FPoint{T: 1, F: 1.0}, promql.FPoint{T: 2, F: 2.0})
	require.NoError(t, err)
	require.Equal(t, 2*FPointSize, tracker.CurrentEstimatedMemoryConsumptionBytes())

	s, err = p.AppendToSlice(s, tracker, promql.FPoint{T: 1, F: 1.0}, promql.FPoint{T: 2, F: 2.0})
	require.Error(t, err)
	require.Nil(t, s)
	require.Equal(t, uint64(0), tracker.CurrentEstimatedMemoryConsumptionBytes())
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

func BenchmarkLimitingPool_GetPut(b *testing.B) {
	for _, mangle := range []bool{false, true} {
		b.Run(fmt.Sprintf("unlimited pool, mangle=%t", mangle), func(b *testing.B) {
			p := NewLimitingBucketedPool(
				pool.NewBucketedPool(1024, func(size int) []promql.FPoint { return make([]promql.FPoint, 0, size) }),
				limiter.FPointSlices,
				FPointSize,
				false,
				atomic.NewBool(mangle),
				mangleFPoint,
				nil,
			)

			tracker := limiter.NewUnlimitedMemoryConsumptionTracker(context.Background())

			for b.Loop() {
				s, err := p.Get(128, tracker)
				if err != nil {
					require.NoError(b, err)
				}

				p.Put(&s, tracker)
			}
		})
	}
}
