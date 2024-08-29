// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/integration/querier_sharding_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.
//go:build requires_docker

package integration

import (
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/grafana/e2e"
	e2ecache "github.com/grafana/e2e/cache"
	e2edb "github.com/grafana/e2e/db"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/integration/e2emimir"
)

type querierShardingTestConfig struct {
	shuffleShardingEnabled bool
	querySchedulerEnabled  bool
	sendHistograms         bool
	querierResponseFormat  string
}

func TestQuerySharding(t *testing.T) {
	for _, shuffleShardingEnabled := range []bool{false, true} {
		for _, querySchedulerEnabled := range []bool{false, true} {
			for _, sendHistograms := range []bool{false, true} {
				for _, querierResponseFormat := range []string{"json", "protobuf"} {
					if sendHistograms && querierResponseFormat == "json" {
						// histograms over json are not supported
						continue
					}
					testName := fmt.Sprintf("shuffle shard=%v/query scheduler=%v/histograms=%v/format=%v",
						shuffleShardingEnabled, querySchedulerEnabled, sendHistograms, querierResponseFormat,
					)
					cfg := querierShardingTestConfig{
						shuffleShardingEnabled: shuffleShardingEnabled,
						querySchedulerEnabled:  querySchedulerEnabled,
						sendHistograms:         sendHistograms,
						querierResponseFormat:  querierResponseFormat,
					}
					t.Run(testName, func(t *testing.T) { runQuerierShardingTest(t, cfg) })
				}
			}
		}
	}
}

func runQuerierShardingTest(t *testing.T, cfg querierShardingTestConfig) {
	// Going too high starts hitting file descriptor limit, since we run all queriers concurrently.
	const numQueries = 100

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	memcached := e2ecache.NewMemcached()
	consul := e2edb.NewConsul()
	require.NoError(t, s.StartAndWaitReady(consul, memcached))

	flags := mergeFlags(BlocksStorageFlags(), BlocksStorageS3Flags(), map[string]string{
		"-query-frontend.cache-results":                     "true",
		"-query-frontend.results-cache.backend":             "memcached",
		"-query-frontend.results-cache.memcached.addresses": "dns+" + memcached.NetworkEndpoint(e2ecache.MemcachedPort),
		"-query-frontend.results-cache.compression":         "snappy",
		"-querier.max-outstanding-requests-per-tenant":      strconv.Itoa(numQueries), // To avoid getting errors.
	})

	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(minio))

	if cfg.shuffleShardingEnabled {
		// Use only single querier for each user.
		flags["-query-frontend.max-queriers-per-tenant"] = "1"
	}

	// Start the query-scheduler if enabled.
	var queryScheduler *e2emimir.MimirService
	if cfg.querySchedulerEnabled {
		queryScheduler = e2emimir.NewQueryScheduler("query-scheduler", flags)
		require.NoError(t, s.StartAndWaitReady(queryScheduler))
		flags["-query-frontend.scheduler-address"] = queryScheduler.NetworkGRPCEndpoint()
		flags["-querier.scheduler-address"] = queryScheduler.NetworkGRPCEndpoint()
	}

	// Start the query-frontend.
	queryFrontend := e2emimir.NewQueryFrontend("query-frontend", consul.NetworkHTTPEndpoint(), flags)
	require.NoError(t, s.Start(queryFrontend))

	if !cfg.querySchedulerEnabled {
		flags["-querier.frontend-address"] = queryFrontend.NetworkGRPCEndpoint()
	}

	// Start all other services.
	ingester := e2emimir.NewIngester("ingester", consul.NetworkHTTPEndpoint(), flags)
	distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags)
	querier1 := e2emimir.NewQuerier("querier-1", consul.NetworkHTTPEndpoint(), flags)
	querier2 := e2emimir.NewQuerier("querier-2", consul.NetworkHTTPEndpoint(), flags)

	require.NoError(t, s.StartAndWaitReady(querier1, querier2, ingester, distributor))
	require.NoError(t, s.WaitReady(queryFrontend))

	// Wait until distributor and queriers have updated the ring.
	// The distributor should have 512 tokens for the ingester ring and 1 for the distributor ring
	require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(512+1), "cortex_ring_tokens_total"))
	require.NoError(t, querier1.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))
	require.NoError(t, querier2.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))

	// Push a series for each user to Mimir.
	now := time.Now()

	distClient, err := e2emimir.NewClient(distributor.HTTPEndpoint(), "", "", "", userID)
	require.NoError(t, err)

	var genSeries generateSeriesFunc
	if !cfg.sendHistograms {
		genSeries = generateFloatSeries
	} else {
		genSeries = generateHistogramSeries
	}
	series, expectedVector, _ := genSeries("series_1", now)

	res, err := distClient.Push(series)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	// Send both queriers a single query, so that they both initialize their cortex_querier_request_duration_seconds metrics.
	for _, q := range []*e2emimir.MimirService{querier1, querier2} {
		c, err := e2emimir.NewClient("", q.HTTPEndpoint(), "", "", userID)
		require.NoError(t, err)

		_, err = c.Query("series_1", now)
		require.NoError(t, err)
	}

	// Wait until both workers connect to the query-frontend or query-scheduler, each with the minimum 4 connections.
	if cfg.querySchedulerEnabled {
		require.NoError(t, queryScheduler.WaitSumMetrics(e2e.Equals(8), "cortex_query_scheduler_connected_querier_clients"))
	} else {
		require.NoError(t, queryFrontend.WaitSumMetrics(e2e.Equals(8), "cortex_query_frontend_connected_clients"))
	}

	wg := sync.WaitGroup{}

	// Run all queries concurrently to get better distribution of requests between queriers.
	for i := 0; i < numQueries; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			c, err := e2emimir.NewClient("", queryFrontend.HTTPEndpoint(), "", "", userID)
			require.NoError(t, err)

			result, err := c.Query("series_1", now)
			require.NoError(t, err)
			require.Equal(t, model.ValVector, result.Type())
			assert.Equal(t, expectedVector, result.(model.Vector))
		}()
	}

	wg.Wait()

	require.NoError(t, queryFrontend.WaitSumMetrics(e2e.Equals(numQueries), "cortex_query_frontend_queries_total"))

	// Verify that only single querier handled all the queries when sharding is enabled, otherwise queries have been fairly distributed across queriers.
	q1Values, err := querier1.SumMetrics([]string{"cortex_querier_request_duration_seconds"}, e2e.WithMetricCount)
	require.NoError(t, err)
	require.Len(t, q1Values, 1)

	q2Values, err := querier2.SumMetrics([]string{"cortex_querier_request_duration_seconds"}, e2e.WithMetricCount)
	require.NoError(t, err)
	require.Len(t, q2Values, 1)

	total := q1Values[0] + q2Values[0]
	diff := q1Values[0] - q2Values[0]
	if diff < 0 {
		diff = -diff
	}

	require.Equal(t, float64(numQueries), total-2) // Remove 2 requests used for metrics initialization.

	if cfg.shuffleShardingEnabled {
		require.Equal(t, float64(numQueries), diff)
	} else {
		// Both queriers should have roughly equal number of requests, with possible delta. 50% delta is
		// picked to be small enough so that load between queriers would not be wildly different (allow a
		// max difference of 25 queries vs 75 queries) but tolerant of the variability of doing something
		// probabilistic like this with such a small sample size (only 100 queries).
		require.InDelta(t, 0, diff, numQueries*0.50)
	}

	// Ensure no service-specific metrics prefix is used by the wrong service.
	assertServiceMetricsPrefixes(t, Distributor, distributor)
	assertServiceMetricsPrefixes(t, Ingester, ingester)
	assertServiceMetricsPrefixes(t, Querier, querier1)
	assertServiceMetricsPrefixes(t, Querier, querier2)
	assertServiceMetricsPrefixes(t, QueryFrontend, queryFrontend)
	assertServiceMetricsPrefixes(t, QueryScheduler, queryScheduler)
}
