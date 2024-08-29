// SPDX-License-Identifier: AGPL-3.0-only

package limiting

import (
	"fmt"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

const rejectedQueriesMetricName = "rejected_queries"

func TestMemoryConsumptionTracker_Unlimited(t *testing.T) {
	reg, metric := createRejectedMetric()
	tracker := NewMemoryConsumptionTracker(0, metric)

	require.NoError(t, tracker.IncreaseMemoryConsumption(128))
	require.Equal(t, uint64(128), tracker.CurrentEstimatedMemoryConsumptionBytes)
	require.Equal(t, uint64(128), tracker.PeakEstimatedMemoryConsumptionBytes)

	// Add some more memory consumption. The current and peak stats should be updated.
	require.NoError(t, tracker.IncreaseMemoryConsumption(2))
	require.Equal(t, uint64(130), tracker.CurrentEstimatedMemoryConsumptionBytes)
	require.Equal(t, uint64(130), tracker.PeakEstimatedMemoryConsumptionBytes)

	// Reduce memory consumption. The current consumption should be updated, but the peak should be unchanged.
	tracker.DecreaseMemoryConsumption(128)
	require.Equal(t, uint64(2), tracker.CurrentEstimatedMemoryConsumptionBytes)
	require.Equal(t, uint64(130), tracker.PeakEstimatedMemoryConsumptionBytes)

	// Add some more memory consumption that doesn't take us over the previous peak.
	require.NoError(t, tracker.IncreaseMemoryConsumption(8))
	require.Equal(t, uint64(10), tracker.CurrentEstimatedMemoryConsumptionBytes)
	require.Equal(t, uint64(130), tracker.PeakEstimatedMemoryConsumptionBytes)

	// Add some more memory consumption that takes us over the previous peak.
	require.NoError(t, tracker.IncreaseMemoryConsumption(121))
	require.Equal(t, uint64(131), tracker.CurrentEstimatedMemoryConsumptionBytes)
	require.Equal(t, uint64(131), tracker.PeakEstimatedMemoryConsumptionBytes)

	assertRejectedQueriesCount(t, reg, 0)

	// Test reducing memory consumption to a negative panics
	require.Panics(t, func() { tracker.DecreaseMemoryConsumption(150) })
}

func TestMemoryConsumptionTracker_Limited(t *testing.T) {
	reg, metric := createRejectedMetric()
	tracker := NewMemoryConsumptionTracker(11, metric)

	// Add some memory consumption beneath the limit.
	require.NoError(t, tracker.IncreaseMemoryConsumption(8))
	require.Equal(t, uint64(8), tracker.CurrentEstimatedMemoryConsumptionBytes)
	require.Equal(t, uint64(8), tracker.PeakEstimatedMemoryConsumptionBytes)
	assertRejectedQueriesCount(t, reg, 0)

	// Add some more memory consumption beneath the limit.
	require.NoError(t, tracker.IncreaseMemoryConsumption(1))
	require.Equal(t, uint64(9), tracker.CurrentEstimatedMemoryConsumptionBytes)
	require.Equal(t, uint64(9), tracker.PeakEstimatedMemoryConsumptionBytes)
	assertRejectedQueriesCount(t, reg, 0)

	// Reduce memory consumption.
	tracker.DecreaseMemoryConsumption(1)
	require.Equal(t, uint64(8), tracker.CurrentEstimatedMemoryConsumptionBytes)
	require.Equal(t, uint64(9), tracker.PeakEstimatedMemoryConsumptionBytes)
	assertRejectedQueriesCount(t, reg, 0)

	// Try to add some more memory consumption where we would go over the limit.
	const expectedError = "the query exceeded the maximum allowed estimated amount of memory consumed by a single query (limit: 11 bytes) (err-mimir-max-estimated-memory-consumption-per-query)"
	require.ErrorContains(t, tracker.IncreaseMemoryConsumption(4), expectedError)
	require.Equal(t, uint64(8), tracker.CurrentEstimatedMemoryConsumptionBytes)
	require.Equal(t, uint64(9), tracker.PeakEstimatedMemoryConsumptionBytes)
	assertRejectedQueriesCount(t, reg, 1)

	// Make sure we don't increment the rejection count a second time for the same query.
	require.ErrorContains(t, tracker.IncreaseMemoryConsumption(4), expectedError)
	require.Equal(t, uint64(8), tracker.CurrentEstimatedMemoryConsumptionBytes)
	require.Equal(t, uint64(9), tracker.PeakEstimatedMemoryConsumptionBytes)
	assertRejectedQueriesCount(t, reg, 1)

	// Keep adding more memory consumption up to the limit to make sure the failed increases weren't counted.
	for i := 0; i < 3; i++ {
		require.NoError(t, tracker.IncreaseMemoryConsumption(1))
		require.Equal(t, uint64(9+i), tracker.CurrentEstimatedMemoryConsumptionBytes)
		require.Equal(t, uint64(9+i), tracker.PeakEstimatedMemoryConsumptionBytes)
	}

	// Try to add some more memory consumption when we're already at the limit.
	require.ErrorContains(t, tracker.IncreaseMemoryConsumption(1), expectedError)
	require.Equal(t, uint64(11), tracker.CurrentEstimatedMemoryConsumptionBytes)
	require.Equal(t, uint64(11), tracker.PeakEstimatedMemoryConsumptionBytes)
	assertRejectedQueriesCount(t, reg, 1)

	// Test reducing memory consumption to a negative panics
	require.Panics(t, func() { tracker.DecreaseMemoryConsumption(150) })
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
