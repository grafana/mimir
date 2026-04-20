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
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/atomic"
)

const rejectedQueriesMetricName = "rejected_queries"

func TestMemoryConsumptionTrackerFromContext(t *testing.T) {
	t.Run("does not exist", func(t *testing.T) {
		ctx := context.Background()
		tracker, err := MemoryConsumptionTrackerFromContext(ctx)
		require.Nil(t, tracker)
		require.ErrorIs(t, err, errNoMemoryConsumptionTrackerInContext)
	})

	t.Run("exists", func(t *testing.T) {
		ctx := context.Background()
		existing := NewUnlimitedMemoryConsumptionTracker(ctx)
		require.NoError(t, existing.IncreaseMemoryConsumption(uint64(512), IngesterChunks))

		ctx = context.WithValue(ctx, memoryConsumptionTracker, existing)
		stored, err := MemoryConsumptionTrackerFromContext(ctx)
		require.NoError(t, err)
		require.Equal(t, existing, stored)
		require.Equal(t, uint64(512), stored.CurrentEstimatedMemoryConsumptionBytes())
		require.Equal(t, uint64(512), stored.CurrentEstimatedMemoryConsumptionBytesBySource(IngesterChunks))
	})
}

func TestAddToContext(t *testing.T) {
	ctx := context.Background()
	existing := NewUnlimitedMemoryConsumptionTracker(ctx)
	require.NoError(t, existing.IncreaseMemoryConsumption(uint64(512), IngesterChunks))

	ctx = AddMemoryTrackerToContext(ctx, existing)
	stored := ctx.Value(memoryConsumptionTracker).(*MemoryConsumptionTracker)
	require.Equal(t, existing, stored)
	require.Equal(t, uint64(512), stored.CurrentEstimatedMemoryConsumptionBytes())
	require.Equal(t, uint64(512), stored.CurrentEstimatedMemoryConsumptionBytesBySource(IngesterChunks))
}

func TestMemoryConsumptionTracker_Unlimited(t *testing.T) {
	reg, metric := createRejectedMetric()
	tracker := NewMemoryConsumptionTracker(context.Background(), 0, metric, "foo + bar")

	require.NoError(t, tracker.IncreaseMemoryConsumption(128, IngesterChunks))
	require.Equal(t, uint64(128), tracker.CurrentEstimatedMemoryConsumptionBytes())
	require.Equal(t, uint64(128), tracker.CurrentEstimatedMemoryConsumptionBytesBySource(IngesterChunks))
	require.Equal(t, uint64(128), tracker.PeakEstimatedMemoryConsumptionBytes())

	// Add some more memory consumption. The current and peak stats should be updated.
	require.NoError(t, tracker.IncreaseMemoryConsumption(2, StoreGatewayChunks))
	require.Equal(t, uint64(130), tracker.CurrentEstimatedMemoryConsumptionBytes())
	require.Equal(t, uint64(2), tracker.CurrentEstimatedMemoryConsumptionBytesBySource(StoreGatewayChunks))
	require.Equal(t, uint64(130), tracker.PeakEstimatedMemoryConsumptionBytes())

	// Reduce memory consumption. The current consumption should be updated, but the peak should be unchanged.
	tracker.DecreaseMemoryConsumption(128, IngesterChunks)
	require.Equal(t, uint64(2), tracker.CurrentEstimatedMemoryConsumptionBytes())
	require.Equal(t, uint64(0), tracker.CurrentEstimatedMemoryConsumptionBytesBySource(IngesterChunks))
	require.Equal(t, uint64(130), tracker.PeakEstimatedMemoryConsumptionBytes())

	// Add some more memory consumption that doesn't take us over the previous peak.
	require.NoError(t, tracker.IncreaseMemoryConsumption(8, FPointSlices))
	require.Equal(t, uint64(10), tracker.CurrentEstimatedMemoryConsumptionBytes())
	require.Equal(t, uint64(8), tracker.CurrentEstimatedMemoryConsumptionBytesBySource(FPointSlices))
	require.Equal(t, uint64(130), tracker.PeakEstimatedMemoryConsumptionBytes())

	// Add some more memory consumption that takes us over the previous peak.
	require.NoError(t, tracker.IncreaseMemoryConsumption(121, HPointSlices))
	require.Equal(t, uint64(131), tracker.CurrentEstimatedMemoryConsumptionBytes())
	require.Equal(t, uint64(121), tracker.CurrentEstimatedMemoryConsumptionBytesBySource(HPointSlices))
	require.Equal(t, uint64(131), tracker.PeakEstimatedMemoryConsumptionBytes())

	assertRejectedQueriesCount(t, reg, 0)

	// Test reducing memory consumption to a negative value panics
	require.PanicsWithValue(t, `Estimated memory consumption of all instances of []promql.FPoint in this query is 8 bytes when trying to return 9 bytes. This indicates something has been returned to a pool more than once, which is a bug. The affected query is: foo + bar`, func() { tracker.DecreaseMemoryConsumption(9, FPointSlices) })
	require.PanicsWithValue(t, `Estimated memory consumption of all instances of []promql.HPoint in this query is 121 bytes when trying to return 130 bytes. This indicates something has been returned to a pool more than once, which is a bug. The affected query is: foo + bar`, func() { tracker.DecreaseMemoryConsumption(130, HPointSlices) })
}

func TestMemoryConsumptionTracker_Limited(t *testing.T) {
	reg, metric := createRejectedMetric()
	tracker := NewMemoryConsumptionTracker(context.Background(), 11, metric, "foo + bar")

	// Add some memory consumption beneath the limit.
	require.NoError(t, tracker.IncreaseMemoryConsumption(8, IngesterChunks))
	require.Equal(t, uint64(8), tracker.CurrentEstimatedMemoryConsumptionBytes())
	require.Equal(t, uint64(8), tracker.PeakEstimatedMemoryConsumptionBytes())
	assertRejectedQueriesCount(t, reg, 0)

	// Add some more memory consumption beneath the limit.
	require.NoError(t, tracker.IncreaseMemoryConsumption(1, StoreGatewayChunks))
	require.Equal(t, uint64(9), tracker.CurrentEstimatedMemoryConsumptionBytes())
	require.Equal(t, uint64(9), tracker.PeakEstimatedMemoryConsumptionBytes())
	assertRejectedQueriesCount(t, reg, 0)

	// Reduce memory consumption.
	tracker.DecreaseMemoryConsumption(1, StoreGatewayChunks)
	require.Equal(t, uint64(8), tracker.CurrentEstimatedMemoryConsumptionBytes())
	require.Equal(t, uint64(9), tracker.PeakEstimatedMemoryConsumptionBytes())
	assertRejectedQueriesCount(t, reg, 0)

	// Try to add some more memory consumption where we would go over the limit.
	const expectedError = "the query exceeded the maximum allowed estimated amount of memory consumed by a single query (limit: 11 bytes) (err-mimir-max-estimated-memory-consumption-per-query)"
	require.ErrorContains(t, tracker.IncreaseMemoryConsumption(4, StoreGatewayChunks), expectedError)
	require.Equal(t, uint64(8), tracker.CurrentEstimatedMemoryConsumptionBytes())
	require.Equal(t, uint64(9), tracker.PeakEstimatedMemoryConsumptionBytes())
	assertRejectedQueriesCount(t, reg, 1)

	// Make sure we don't increment the rejection count a second time for the same query.
	require.ErrorContains(t, tracker.IncreaseMemoryConsumption(4, IngesterChunks), expectedError)
	require.Equal(t, uint64(8), tracker.CurrentEstimatedMemoryConsumptionBytes())
	require.Equal(t, uint64(9), tracker.PeakEstimatedMemoryConsumptionBytes())
	assertRejectedQueriesCount(t, reg, 1)

	// Keep adding more memory consumption up to the limit to make sure the failed increases weren't counted.
	for i := 0; i < 3; i++ {
		require.NoError(t, tracker.IncreaseMemoryConsumption(1, FPointSlices))
		require.Equal(t, uint64(9+i), tracker.CurrentEstimatedMemoryConsumptionBytes())
		require.Equal(t, uint64(9+i), tracker.PeakEstimatedMemoryConsumptionBytes())
	}

	// Try to add some more memory consumption when we're already at the limit.
	require.ErrorContains(t, tracker.IncreaseMemoryConsumption(1, FPointSlices), expectedError)
	require.Equal(t, uint64(11), tracker.CurrentEstimatedMemoryConsumptionBytes())
	require.Equal(t, uint64(11), tracker.PeakEstimatedMemoryConsumptionBytes())
	assertRejectedQueriesCount(t, reg, 1)

	// Test reducing memory consumption to a negative value panics
	require.PanicsWithValue(t, `Estimated memory consumption of all instances of []promql.FPoint in this query is 3 bytes when trying to return 150 bytes. This indicates something has been returned to a pool more than once, which is a bug. The affected query is: foo + bar`, func() { tracker.DecreaseMemoryConsumption(150, FPointSlices) })
	require.PanicsWithValue(t, `Estimated memory consumption of all instances of []promql.HPoint in this query is 0 bytes when trying to return 150 bytes. This indicates something has been returned to a pool more than once, which is a bug. The affected query is: foo + bar`, func() { tracker.DecreaseMemoryConsumption(150, HPointSlices) })
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
	const source = IngesterChunks

	b.Run("no limits single threaded", func(b *testing.B) {
		l := NewUnlimitedMemoryConsumptionTracker(context.Background())
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = l.IncreaseMemoryConsumption(uint64(b.N), source)
			l.DecreaseMemoryConsumption(uint64(b.N), source)
		}

		require.Equal(b, uint64(0), l.CurrentEstimatedMemoryConsumptionBytes())
	})

	b.Run("with limits single threaded", func(b *testing.B) {
		counter := promauto.With(nil).NewCounter(prometheus.CounterOpts{
			Name: "cortex_test_rejections_total",
		})

		l := NewMemoryConsumptionTracker(context.Background(), memoryLimit, counter, "")
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := l.IncreaseMemoryConsumption(uint64(b.N), source); err == nil {
				l.DecreaseMemoryConsumption(uint64(b.N), source)
			}
		}

		require.Equal(b, uint64(0), l.CurrentEstimatedMemoryConsumptionBytes())
	})

	b.Run("no limits multiple threads", func(b *testing.B) {
		l := NewUnlimitedMemoryConsumptionTracker(context.Background())
		wg := sync.WaitGroup{}
		run := atomic.NewBool(true)

		wg.Add(1)
		go func() {
			defer wg.Done()

			for run.Load() {
				_ = l.IncreaseMemoryConsumption(1024, source)
				l.DecreaseMemoryConsumption(1024, source)
			}
		}()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = l.IncreaseMemoryConsumption(uint64(b.N), source)
			l.DecreaseMemoryConsumption(uint64(b.N), source)
		}

		run.Store(false)
		wg.Wait()

		require.Equal(b, uint64(0), l.CurrentEstimatedMemoryConsumptionBytes())
	})

	b.Run("with limits multiple threads", func(b *testing.B) {
		counter := promauto.With(nil).NewCounter(prometheus.CounterOpts{
			Name: "cortex_test_rejections_total",
		})

		l := NewMemoryConsumptionTracker(context.Background(), memoryLimit, counter, "")
		wg := sync.WaitGroup{}
		run := atomic.NewBool(true)

		wg.Add(1)
		go func() {
			defer wg.Done()

			for run.Load() {
				if err := l.IncreaseMemoryConsumption(1024, source); err == nil {
					l.DecreaseMemoryConsumption(1024, source)
				}
			}
		}()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := l.IncreaseMemoryConsumption(uint64(b.N), source); err == nil {
				l.DecreaseMemoryConsumption(uint64(b.N), source)
			}
		}

		run.Store(false)
		wg.Wait()

		require.Equal(b, uint64(0), l.CurrentEstimatedMemoryConsumptionBytes())
	})
}

func TestMemoryConsumptionTrackerTracker_Aggregation(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	tt := NewInflightMemoryConsumptionTracker(reg)

	tracker1Limit := 100
	tracker2Limit := 200
	tracker1 := tt.NewMemoryConsumptionTracker(context.Background(), uint64(tracker1Limit), nil, "query1")
	tracker2 := tt.NewMemoryConsumptionTracker(context.Background(), uint64(tracker2Limit), nil, "query2")

	// tracker1: add 30 ingester + 20 store-gateway = 50, remove 10 ingester -> current=40, peak=50
	tracker1Ingester := 30
	tracker1StoreGateway := 20
	tracker1Decrease := 10
	tracker1Current := tracker1Ingester + tracker1StoreGateway - tracker1Decrease // 40
	tracker1Peak := tracker1Ingester + tracker1StoreGateway                       // 50
	require.NoError(t, tracker1.IncreaseMemoryConsumption(uint64(tracker1Ingester), IngesterChunks))
	require.NoError(t, tracker1.IncreaseMemoryConsumption(uint64(tracker1StoreGateway), StoreGatewayChunks))
	tracker1.DecreaseMemoryConsumption(uint64(tracker1Decrease), IngesterChunks)

	// tracker2: add 60 ingester -> current=60, peak=60
	tracker2Ingester := 60
	tracker2Current := tracker2Ingester // 60
	tracker2Peak := tracker2Ingester    // 60
	require.NoError(t, tracker2.IncreaseMemoryConsumption(uint64(tracker2Ingester), IngesterChunks))

	assertTrackerTrackerMetrics(t, reg, float64(tracker1Limit+tracker2Limit), float64(tracker1Current+tracker2Current), float64(tracker1Peak+tracker2Peak), 2)

	tt.Deregister(tracker1)
	assertTrackerTrackerMetrics(t, reg, float64(tracker2Limit), float64(tracker2Current), float64(tracker2Peak), 1)

	tt.Deregister(tracker2)
	assertTrackerTrackerMetrics(t, reg, 0, 0, 0, 0)

	// idempotent
	tt.Deregister(tracker2)
	assertTrackerTrackerMetrics(t, reg, 0, 0, 0, 0)
}

func TestMemoryConsumptionTrackerTracker_DeregisterNonManagedTracker(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	tt := NewInflightMemoryConsumptionTracker(reg)
	nonManagedTracker := NewMemoryConsumptionTracker(context.Background(), 100, nil, "query3")
	require.Panics(t, func() { tt.Deregister(nonManagedTracker) })
}

func assertTrackerTrackerMetrics(t *testing.T, reg prometheus.Gatherer, maxBytes, currentBytes, peakBytes float64, sampled int) {
	t.Helper()
	expected := fmt.Sprintf(`
		# HELP cortex_querier_inflight_query_current_estimated_memory_consumption_bytes Total current estimated memory consumption across all in-flight queries.
		# TYPE cortex_querier_inflight_query_current_estimated_memory_consumption_bytes gauge
		cortex_querier_inflight_query_current_estimated_memory_consumption_bytes %v
		# HELP cortex_querier_inflight_query_max_estimated_memory_consumption_limit_bytes Total of the max estimated memory consumption limit across all in-flight queries.
		# TYPE cortex_querier_inflight_query_max_estimated_memory_consumption_limit_bytes gauge
		cortex_querier_inflight_query_max_estimated_memory_consumption_limit_bytes %v
		# HELP cortex_querier_inflight_query_peak_estimated_memory_consumption_bytes Total peak estimated memory consumption across all in-flight queries.
		# TYPE cortex_querier_inflight_query_peak_estimated_memory_consumption_bytes gauge
		cortex_querier_inflight_query_peak_estimated_memory_consumption_bytes %v
		# HELP cortex_querier_inflight_query_sampled_count Number of in-flight memory consumption trackers accumulated during the last metrics collection.
		# TYPE cortex_querier_inflight_query_sampled_count gauge
		cortex_querier_inflight_query_sampled_count %v
	`, currentBytes, maxBytes, peakBytes, sampled)
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expected),
		"cortex_querier_inflight_query_max_estimated_memory_consumption_limit_bytes",
		"cortex_querier_inflight_query_current_estimated_memory_consumption_bytes",
		"cortex_querier_inflight_query_peak_estimated_memory_consumption_bytes",
		"cortex_querier_inflight_query_sampled_count",
	))
}

func TestMemoryConsumptionSourceNames(t *testing.T) {
	for i := range memoryConsumptionSourceCount {
		require.NotEqual(t, unknownMemorySource, i.String(), "source %d should have a String() representation", i)
	}
}

func TestMemoryConsumptionTracker_NegativeMemoryConsumptionPanicWithTracing(t *testing.T) {
	traceID, err := trace.TraceIDFromHex("00000000000000010000000000000002")
	require.NoError(t, err)
	spanID, err := trace.SpanIDFromHex("0000000000000003")
	require.NoError(t, err)

	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
	})

	ctx := trace.ContextWithSpanContext(context.Background(), sc)

	tracker := NewMemoryConsumptionTracker(ctx, 0, nil, "foo + bar")

	require.PanicsWithValue(t, `Estimated memory consumption of all instances of ingester chunks in this query is 0 bytes when trying to return 10 bytes. This indicates something has been returned to a pool more than once, which is a bug. The affected query is: foo + bar (trace ID: 00000000000000010000000000000002)`, func() {
		tracker.DecreaseMemoryConsumption(10, IngesterChunks)
	})
}
