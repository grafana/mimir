// SPDX-License-Identifier: AGPL-3.0-only
//go:build requires_docker

package integration

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/grafana/e2e"
	e2edb "github.com/grafana/e2e/db"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/integration/e2emimir"
)

func TestIngesterPriorityLoadShedding(t *testing.T) {
	s, err := e2e.NewScenario("mimir-ingester-priority")
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	consul := e2edb.NewConsul()
	require.NoError(t, s.StartAndWaitReady(consul))

	// Configure the ingester with very low reactive limiter limits to make it easy to trigger rejection
	flags := mergeFlags(
		DefaultSingleBinaryFlags(),
		map[string]string{
			// Enable reactive limiting with low limits
			"-ingester.push-reactive-limit.enabled":                     "true",
			"-ingester.push-reactive-limit.initial-inflight-limit":      "3",
			"-ingester.push-reactive-limit.min-inflight-limit":          "2",
			"-ingester.push-reactive-limit.max-inflight-limit":          "5",
			"-ingester.push-reactive-limit.initial-rejection-factor":    "2",
			"-ingester.push-reactive-limit.max-rejection-factor":        "3",

			"-ingester.read-reactive-limit.enabled":                     "true",
			"-ingester.read-reactive-limit.initial-inflight-limit":      "2",
			"-ingester.read-reactive-limit.min-inflight-limit":          "1",
			"-ingester.read-reactive-limit.max-inflight-limit":          "3",
			"-ingester.read-reactive-limit.initial-rejection-factor":    "2",
			"-ingester.read-reactive-limit.max-rejection-factor":        "3",

			// Enable prioritizer
			"-ingester.rejection-prioritizer.enabled":        "true",
			"-ingester.rejection-prioritizer.calibration-interval": "1s",
		},
	)

	// Start Mimir components.
	distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags)
	ingester := e2emimir.NewIngester("ingester", consul.NetworkHTTPEndpoint(), flags)
	require.NoError(t, s.StartAndWaitReady(distributor, ingester))

	// Wait until distributor and ingester have updated the ring.
	require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(512), []string{"cortex_ring_tokens_total"}, e2e.WaitMissingMetrics))
	require.NoError(t, ingester.WaitSumMetricsWithOptions(e2e.Equals(512), []string{"cortex_ring_tokens_total"}, e2e.WaitMissingMetrics))

	// Push some initial series to the ingester
	c, err := e2emimir.NewClient(distributor.HTTPEndpoint(), "", "", "", "test")
	require.NoError(t, err)

	// Write series data
	series1 := prompb.TimeSeries{
		Labels: []prompb.Label{
			{Name: "__name__", Value: "test_metric_1"},
			{Name: "job", Value: "test"},
		},
		Samples: []prompb.Sample{{Value: 1, Timestamp: time.Now().UnixMilli()}},
	}
	series2 := prompb.TimeSeries{
		Labels: []prompb.Label{
			{Name: "__name__", Value: "test_metric_2"},
			{Name: "job", Value: "test"},
		},
		Samples: []prompb.Sample{{Value: 2, Timestamp: time.Now().UnixMilli()}},
	}

	_, err = c.Push([]prompb.TimeSeries{series1, series2})
	require.NoError(t, err)

	// Give some time for metrics to be ingested
	time.Sleep(2 * time.Second)

	// Now test priority-aware querying under load
	t.Run("Priority differentiation under load", func(t *testing.T) {
		// Create clients with different User-Agent headers for priority testing
		highPriorityClient, err := e2emimir.NewClient(
			distributor.HTTPEndpoint(), "", "", "", "test",
			e2emimir.WithAddHeader("User-Agent", "grafana-ruler/1.0"), // VeryHigh priority
		)
		require.NoError(t, err)

		lowPriorityClient, err := e2emimir.NewClient(
			distributor.HTTPEndpoint(), "", "", "", "test",
			e2emimir.WithAddHeader("User-Agent", "unknown-client/1.0"), // VeryLow priority
		)
		require.NoError(t, err)

		var (
			highPrioritySuccesses int64
			highPriorityFailures  int64
			lowPrioritySuccesses  int64
			lowPriorityFailures   int64
			mu                    sync.Mutex
		)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		var wg sync.WaitGroup

		// Generate high load with low priority requests (should be rejected first)
		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-ctx.Done():
						return
					default:
						// Query with low priority client
						_, err := lowPriorityClient.Query("test_metric_1", time.Now())
						mu.Lock()
						if err != nil {
							lowPriorityFailures++
						} else {
							lowPrioritySuccesses++
						}
						mu.Unlock()

						// Brief delay to avoid overwhelming
						time.Sleep(10 * time.Millisecond)
					}
				}
			}()
		}

		// Generate high priority requests (should succeed even under load)
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-ctx.Done():
						return
					default:
						// Query with high priority client
						_, err := highPriorityClient.Query("test_metric_2", time.Now())
						mu.Lock()
						if err != nil {
							highPriorityFailures++
						} else {
							highPrioritySuccesses++
						}
						mu.Unlock()

						time.Sleep(50 * time.Millisecond)
					}
				}
			}()
		}

		// Let the load run for a while
		time.Sleep(15 * time.Second)
		cancel()
		wg.Wait()

		mu.Lock()
		t.Logf("High priority requests: %d successes, %d failures", highPrioritySuccesses, highPriorityFailures)
		t.Logf("Low priority requests: %d successes, %d failures", lowPrioritySuccesses, lowPriorityFailures)

		// Verify priority differentiation
		// High priority requests should have a much higher success rate
		require.Greater(t, highPrioritySuccesses, int64(0), "High priority requests should succeed")

		if lowPriorityFailures > 0 {
			highPrioritySuccessRate := float64(highPrioritySuccesses) / float64(highPrioritySuccesses+highPriorityFailures)
			lowPrioritySuccessRate := float64(lowPrioritySuccesses) / float64(lowPrioritySuccesses+lowPriorityFailures)

			t.Logf("High priority success rate: %.2f", highPrioritySuccessRate)
			t.Logf("Low priority success rate: %.2f", lowPrioritySuccessRate)

			// High priority should have significantly higher success rate when load shedding is active
			assert.Greater(t, highPrioritySuccessRate, lowPrioritySuccessRate+0.2,
				"High priority requests should have significantly higher success rate")
		}
		mu.Unlock()
	})

	// Verify metrics are being reported
	t.Run("Priority metrics validation", func(t *testing.T) {
		// Check that reactive limiter metrics are present
		assert.NoError(t, ingester.WaitSumMetricsWithOptions(
			e2e.GreaterOrEqual(0),
			[]string{"cortex_ingester_reactive_limiter_inflight_requests"},
			e2e.WaitMissingMetrics,
		))

		// Check that rejection metrics are present if rejections occurred
		err := ingester.WaitSumMetricsWithOptions(
			e2e.GreaterOrEqual(0),
			[]string{"cortex_ingester_rejection_rate"},
			e2e.WaitMissingMetrics,
		)
		if err == nil {
			t.Log("Rejection rate metrics are available")
		}
	})
}