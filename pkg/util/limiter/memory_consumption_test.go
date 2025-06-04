// SPDX-License-Identifier: AGPL-3.0-only

package limiter

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

const rejectedQueriesMetricName = "rejected_queries"

func TestFromContextWithFallback(t *testing.T) {
	t.Run("does not exist", func(t *testing.T) {
		ctx := context.Background()
		tracker := MemoryTrackerFromContextWithFallback(ctx)
		require.NotNil(t, tracker)
		require.Equal(t, uint64(0), tracker.CurrentEstimatedMemoryConsumptionBytes())
	})

	t.Run("exists", func(t *testing.T) {
		ctx := context.Background()
		existing := NewMemoryConsumptionTracker(0, nil, "")
		require.NoError(t, existing.IncreaseMemoryConsumption(uint64(512)))

		ctx = context.WithValue(ctx, memoryConsumptionTracker, existing)
		stored := MemoryTrackerFromContextWithFallback(ctx)
		require.Equal(t, existing, stored)
		require.Equal(t, uint64(512), stored.CurrentEstimatedMemoryConsumptionBytes())
	})
}

func TestAddToContext(t *testing.T) {
	ctx := context.Background()
	existing := NewMemoryConsumptionTracker(0, nil, "")
	require.NoError(t, existing.IncreaseMemoryConsumption(uint64(512)))

	ctx = AddMemoryTrackerToContext(ctx, existing)
	stored := ctx.Value(memoryConsumptionTracker).(*MemoryConsumptionTracker)
	require.Equal(t, existing, stored)
	require.Equal(t, uint64(512), stored.CurrentEstimatedMemoryConsumptionBytes())
}

func TestMemoryConsumptionTracker_Unlimited(t *testing.T) {
	reg, metric := createRejectedMetric()
	tracker := NewMemoryConsumptionTracker(0, metric, "foo + bar")

	require.NoError(t, tracker.IncreaseMemoryConsumption(128))
	require.Equal(t, uint64(128), tracker.CurrentEstimatedMemoryConsumptionBytes())
	require.Equal(t, uint64(128), tracker.PeakEstimatedMemoryConsumptionBytes())

	// Add some more memory consumption. The current and peak stats should be updated.
	require.NoError(t, tracker.IncreaseMemoryConsumption(2))
	require.Equal(t, uint64(130), tracker.CurrentEstimatedMemoryConsumptionBytes())
	require.Equal(t, uint64(130), tracker.PeakEstimatedMemoryConsumptionBytes())

	// Reduce memory consumption. The current consumption should be updated, but the peak should be unchanged.
	tracker.DecreaseMemoryConsumption(128)
	require.Equal(t, uint64(2), tracker.CurrentEstimatedMemoryConsumptionBytes())
	require.Equal(t, uint64(130), tracker.PeakEstimatedMemoryConsumptionBytes())

	// Add some more memory consumption that doesn't take us over the previous peak.
	require.NoError(t, tracker.IncreaseMemoryConsumption(8))
	require.Equal(t, uint64(10), tracker.CurrentEstimatedMemoryConsumptionBytes())
	require.Equal(t, uint64(130), tracker.PeakEstimatedMemoryConsumptionBytes())

	// Add some more memory consumption that takes us over the previous peak.
	require.NoError(t, tracker.IncreaseMemoryConsumption(121))
	require.Equal(t, uint64(131), tracker.CurrentEstimatedMemoryConsumptionBytes())
	require.Equal(t, uint64(131), tracker.PeakEstimatedMemoryConsumptionBytes())

	assertRejectedQueriesCount(t, reg, 0)

	// Test reducing memory consumption to a negative value panics
	require.PanicsWithValue(t, `Estimated memory consumption of this query is negative. This indicates something has been returned to a pool more than once, which is a bug. The affected query is: foo + bar`, func() { tracker.DecreaseMemoryConsumption(150) })
}

func TestMemoryConsumptionTracker_Limited(t *testing.T) {
	reg, metric := createRejectedMetric()
	tracker := NewMemoryConsumptionTracker(11, metric, "foo + bar")

	// Add some memory consumption beneath the limit.
	require.NoError(t, tracker.IncreaseMemoryConsumption(8))
	require.Equal(t, uint64(8), tracker.CurrentEstimatedMemoryConsumptionBytes())
	require.Equal(t, uint64(8), tracker.PeakEstimatedMemoryConsumptionBytes())
	assertRejectedQueriesCount(t, reg, 0)

	// Add some more memory consumption beneath the limit.
	require.NoError(t, tracker.IncreaseMemoryConsumption(1))
	require.Equal(t, uint64(9), tracker.CurrentEstimatedMemoryConsumptionBytes())
	require.Equal(t, uint64(9), tracker.PeakEstimatedMemoryConsumptionBytes())
	assertRejectedQueriesCount(t, reg, 0)

	// Reduce memory consumption.
	tracker.DecreaseMemoryConsumption(1)
	require.Equal(t, uint64(8), tracker.CurrentEstimatedMemoryConsumptionBytes())
	require.Equal(t, uint64(9), tracker.PeakEstimatedMemoryConsumptionBytes())
	assertRejectedQueriesCount(t, reg, 0)

	// Try to add some more memory consumption where we would go over the limit.
	const expectedError = "the query exceeded the maximum allowed estimated amount of memory consumed by a single query (limit: 11 bytes) (err-mimir-max-estimated-memory-consumption-per-query)"
	require.ErrorContains(t, tracker.IncreaseMemoryConsumption(4), expectedError)
	require.Equal(t, uint64(8), tracker.CurrentEstimatedMemoryConsumptionBytes())
	require.Equal(t, uint64(9), tracker.PeakEstimatedMemoryConsumptionBytes())
	assertRejectedQueriesCount(t, reg, 1)

	// Make sure we don't increment the rejection count a second time for the same query.
	require.ErrorContains(t, tracker.IncreaseMemoryConsumption(4), expectedError)
	require.Equal(t, uint64(8), tracker.CurrentEstimatedMemoryConsumptionBytes())
	require.Equal(t, uint64(9), tracker.PeakEstimatedMemoryConsumptionBytes())
	assertRejectedQueriesCount(t, reg, 1)

	// Keep adding more memory consumption up to the limit to make sure the failed increases weren't counted.
	for i := 0; i < 3; i++ {
		require.NoError(t, tracker.IncreaseMemoryConsumption(1))
		require.Equal(t, uint64(9+i), tracker.CurrentEstimatedMemoryConsumptionBytes())
		require.Equal(t, uint64(9+i), tracker.PeakEstimatedMemoryConsumptionBytes())
	}

	// Try to add some more memory consumption when we're already at the limit.
	require.ErrorContains(t, tracker.IncreaseMemoryConsumption(1), expectedError)
	require.Equal(t, uint64(11), tracker.CurrentEstimatedMemoryConsumptionBytes())
	require.Equal(t, uint64(11), tracker.PeakEstimatedMemoryConsumptionBytes())
	assertRejectedQueriesCount(t, reg, 1)

	// Test reducing memory consumption to a negative value panics
	require.PanicsWithValue(t, `Estimated memory consumption of this query is negative. This indicates something has been returned to a pool more than once, which is a bug. The affected query is: foo + bar`, func() { tracker.DecreaseMemoryConsumption(150) })
}

func assertRejectedQueriesCount(t *testing.T, reg *prometheus.Registry, expectedRejectionCount int) {
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

func BenchmarkMemoryConsumptionTracker(b *testing.B) {
	// Set to a very high limit since we don't want to benchmark actually hitting the
	// limit since this should be rare. Instead, we want to benchmark having to check the
	// limit.
	const memoryLimit = 1024 * 1024 * 1024

	b.Run("no limits single threaded", func(b *testing.B) {
		l := NewMemoryConsumptionTracker(0, nil, "")
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = l.IncreaseMemoryConsumption(uint64(b.N))
			l.DecreaseMemoryConsumption(uint64(b.N))
		}

		require.Equal(b, uint64(0), l.CurrentEstimatedMemoryConsumptionBytes())
	})

	b.Run("with limits single threaded", func(b *testing.B) {
		counter := promauto.With(nil).NewCounter(prometheus.CounterOpts{
			Name: "cortex_test_rejections_total",
		})

		l := NewMemoryConsumptionTracker(memoryLimit, counter, "")
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := l.IncreaseMemoryConsumption(uint64(b.N)); err == nil {
				l.DecreaseMemoryConsumption(uint64(b.N))
			}
		}

		require.Equal(b, uint64(0), l.CurrentEstimatedMemoryConsumptionBytes())
	})

	b.Run("no limits multiple threads", func(b *testing.B) {
		l := NewMemoryConsumptionTracker(0, nil, "")
		wg := sync.WaitGroup{}
		run := atomic.NewBool(true)

		wg.Add(1)
		go func() {
			defer wg.Done()

			for run.Load() {
				_ = l.IncreaseMemoryConsumption(1024)
				l.DecreaseMemoryConsumption(1024)
			}
		}()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = l.IncreaseMemoryConsumption(uint64(b.N))
			l.DecreaseMemoryConsumption(uint64(b.N))
		}

		run.Store(false)
		wg.Wait()

		require.Equal(b, uint64(0), l.CurrentEstimatedMemoryConsumptionBytes())
	})

	b.Run("with limits multiple threads", func(b *testing.B) {
		counter := promauto.With(nil).NewCounter(prometheus.CounterOpts{
			Name: "cortex_test_rejections_total",
		})

		l := NewMemoryConsumptionTracker(memoryLimit, counter, "")
		wg := sync.WaitGroup{}
		run := atomic.NewBool(true)

		wg.Add(1)
		go func() {
			defer wg.Done()

			for run.Load() {
				if err := l.IncreaseMemoryConsumption(1024); err == nil {
					l.DecreaseMemoryConsumption(1024)
				}
			}
		}()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := l.IncreaseMemoryConsumption(uint64(b.N)); err == nil {
				l.DecreaseMemoryConsumption(uint64(b.N))
			}
		}

		run.Store(false)
		wg.Wait()

		require.Equal(b, uint64(0), l.CurrentEstimatedMemoryConsumptionBytes())
	})

}
