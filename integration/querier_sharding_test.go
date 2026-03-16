// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/integration/querier_sharding_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.
//go:build requires_docker

package integration

import (
	"fmt"
	"math"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/grafana/e2e"
	e2ecache "github.com/grafana/e2e/cache"
	e2edb "github.com/grafana/e2e/db"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/integration/e2emimir"
)

type querierShardingTestConfig struct {
	shuffleShardingEnabled bool
	sendHistograms         bool
	querierResponseFormat  string
	enableRemoteExecution  bool
}

func TestQuerySharding(t *testing.T) {
	for _, shuffleShardingEnabled := range []bool{false, true} {
		for _, sendHistograms := range []bool{false, true} {
			testName := fmt.Sprintf("shuffle shard=%v/histograms=%v", shuffleShardingEnabled, sendHistograms)

			var formats []string

			if sendHistograms {
				// Histograms over JSON are not supported.
				formats = []string{"protobuf"}
			} else {
				formats = []string{"json", "protobuf"}
			}

			for _, querierResponseFormat := range formats {
				t.Run(testName+fmt.Sprintf("/format=%v", querierResponseFormat), func(t *testing.T) {
					cfg := querierShardingTestConfig{
						shuffleShardingEnabled: shuffleShardingEnabled,
						sendHistograms:         sendHistograms,
						querierResponseFormat:  querierResponseFormat,
					}

					runQuerierShardingTest(t, cfg)
				})
			}

			t.Run(testName+"/remote execution", func(t *testing.T) {
				cfg := querierShardingTestConfig{
					shuffleShardingEnabled: shuffleShardingEnabled,
					sendHistograms:         sendHistograms,
					enableRemoteExecution:  true,
				}

				runQuerierShardingTest(t, cfg)
			})
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
		"-query-frontend.cache-results":                        "true",
		"-query-frontend.results-cache.backend":                "memcached",
		"-query-frontend.results-cache.memcached.addresses":    "dns+" + memcached.NetworkEndpoint(e2ecache.MemcachedPort),
		"-query-frontend.results-cache.compression":            "snappy",
		"-query-scheduler.max-outstanding-requests-per-tenant": strconv.Itoa(numQueries), // To avoid getting errors.
		"-query-frontend.enable-remote-execution":              strconv.FormatBool(cfg.enableRemoteExecution),
		"-query-frontend.use-mimir-query-engine-for-sharding":  strconv.FormatBool(cfg.enableRemoteExecution),
	})

	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(minio))

	if cfg.shuffleShardingEnabled {
		// Use only single querier for each user.
		flags["-query-frontend.max-queriers-per-tenant"] = "1"
	}

	// Start the query-scheduler.
	queryScheduler := e2emimir.NewQueryScheduler("query-scheduler", flags)
	require.NoError(t, s.StartAndWaitReady(queryScheduler))
	flags["-query-frontend.scheduler-address"] = queryScheduler.NetworkGRPCEndpoint()
	flags["-querier.scheduler-address"] = queryScheduler.NetworkGRPCEndpoint()

	// Start the query-frontend.
	queryFrontend := e2emimir.NewQueryFrontend("query-frontend", consul.NetworkHTTPEndpoint(), flags)
	require.NoError(t, s.Start(queryFrontend))

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

	// Wait until the query-frontend has updated the querier ring.
	require.NoError(t, queryFrontend.WaitSumMetricsWithOptions(e2e.Equals(2), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "querier"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

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

		_, _, err = c.Query("series_1", now)
		require.NoError(t, err)
	}

	// Wait until both workers connect to the query-scheduler, each with the minimum 4 connections.
	require.NoError(t, queryScheduler.WaitSumMetrics(e2e.Equals(8), "cortex_query_scheduler_connected_querier_clients"))

	wg := sync.WaitGroup{}

	// Run all queries concurrently to get better distribution of requests between queriers.
	for i := 0; i < numQueries; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			c, err := e2emimir.NewClient("", queryFrontend.HTTPEndpoint(), "", "", userID)
			require.NoError(t, err)

			result, _, err := c.Query("series_1", now)
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

	q1Count := q1Values[0] - 1 // -1: Remove request used for metrics initialization.
	q2Count := q2Values[0] - 1

	total := q1Count + q2Count
	diff := q1Count - q2Count
	if diff < 0 {
		diff = -diff
	}

	require.Equal(t, float64(numQueries), total)

	if cfg.shuffleShardingEnabled {
		require.Equalf(t, float64(numQueries), diff, "expected all queries to be handled by single querier, but one querier got %v requests and the other got %v requests", q1Count, q2Count)
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

// TestQuerySharding_ResultConsistency verifies that sharded queries (via query-frontend)
// produce the same results as unsharded queries (direct to querier) for functions like
// rate(), sum(), avg(), etc. that operate over range vectors.
func TestQuerySharding_ResultConsistency(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	memcached := e2ecache.NewMemcached()
	consul := e2edb.NewConsul()
	require.NoError(t, s.StartAndWaitReady(consul, memcached))

	flags := mergeFlags(BlocksStorageFlags(), BlocksStorageS3Flags(), map[string]string{
		"-query-frontend.cache-results":                       "false",
		"-query-frontend.parallelize-shardable-queries":       "true",
		"-query-frontend.query-sharding-total-shards":         "0", // Disable sharding by default.
		"-query-frontend.results-cache.backend":               "memcached",
		"-query-frontend.results-cache.memcached.addresses":   "dns+" + memcached.NetworkEndpoint(e2ecache.MemcachedPort),
		"-query-frontend.enable-remote-execution":             "true",
		"-query-frontend.use-mimir-query-engine-for-sharding": "true",
	})

	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(minio))

	// Enable query sharding for a specific tenant via runtime config.
	runtimeConfig := "runtime-config.yaml"
	require.NoError(t, writeFileToSharedDir(s, runtimeConfig, []byte(`
overrides:
  sharded-tenant:
    query_sharding_total_shards: 8
`)))
	flags["-runtime-config.file"] = filepath.Join(e2e.ContainerSharedDir, runtimeConfig)

	// Start query-scheduler.
	queryScheduler := e2emimir.NewQueryScheduler("query-scheduler", flags)
	require.NoError(t, s.StartAndWaitReady(queryScheduler))
	flags["-query-frontend.scheduler-address"] = queryScheduler.NetworkGRPCEndpoint()
	flags["-querier.scheduler-address"] = queryScheduler.NetworkGRPCEndpoint()

	// Start services.
	queryFrontend := e2emimir.NewQueryFrontend("query-frontend", consul.NetworkHTTPEndpoint(), flags)
	require.NoError(t, s.Start(queryFrontend))

	ingester := e2emimir.NewIngester("ingester", consul.NetworkHTTPEndpoint(), flags)
	distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags)
	querier1 := e2emimir.NewQuerier("querier-1", consul.NetworkHTTPEndpoint(), flags)
	querier2 := e2emimir.NewQuerier("querier-2", consul.NetworkHTTPEndpoint(), flags)

	require.NoError(t, s.StartAndWaitReady(querier1, querier2, ingester, distributor))
	require.NoError(t, s.WaitReady(queryFrontend))

	require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(512+1), "cortex_ring_tokens_total"))
	require.NoError(t, querier1.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))
	require.NoError(t, querier2.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))

	now := time.Now()
	numSamples := 20
	numSeries := 10

	// Push multiple counter-like series with samples spanning 20 minutes (one per minute).
	// Build all series upfront and push in a single batch per tenant to avoid timestamp-too-old rejections.
	for _, tenant := range []string{userID, "sharded-tenant"} {
		writeClient, err := e2emimir.NewClient(distributor.HTTPEndpoint(), "", "", "", tenant)
		require.NoError(t, err)

		var allSeries []prompb.TimeSeries
		for seriesIdx := 0; seriesIdx < numSeries; seriesIdx++ {
			samples := make([]prompb.Sample, numSamples)
			for i := 0; i < numSamples; i++ {
				// Monotonically increasing values (counter-like).
				samples[i] = prompb.Sample{
					Value:     float64((seriesIdx+1)*100 + i),
					Timestamp: now.Add(time.Duration(i-numSamples+1) * time.Minute).UnixMilli(),
				}
			}

			allSeries = append(allSeries, prompb.TimeSeries{
				Labels: []prompb.Label{
					{Name: model.MetricNameLabel, Value: "test_counter"},
					{Name: "instance", Value: fmt.Sprintf("instance_%d", seriesIdx)},
					{Name: "group", Value: fmt.Sprintf("group_%d", seriesIdx%3)},
				},
				Samples: samples,
			})
		}

		res, err := writeClient.Push(allSeries)
		require.NoError(t, err)
		require.Equal(t, 200, res.StatusCode)
	}

	// Create clients:
	// - unshardedClient queries the querier directly (no sharding).
	// - shardedClient queries the query-frontend with sharding enabled (via per-tenant override).
	unshardedClient, err := e2emimir.NewClient("", querier1.HTTPEndpoint(), "", "", userID)
	require.NoError(t, err)
	shardedClient, err := e2emimir.NewClient("", queryFrontend.HTTPEndpoint(), "", "", "sharded-tenant")
	require.NoError(t, err)

	queries := []struct {
		name  string
		query string
	}{
		{"sum_rate", `sum(rate(test_counter[5m]))`},
		{"avg_rate", `avg(rate(test_counter[5m]))`},
	}

	queryStart := now.Add(-15 * time.Minute)
	queryEnd := now.Add(-1 * time.Minute)
	queryStep := time.Minute

	for _, tc := range queries {
		t.Run(tc.name+"/instant", func(t *testing.T) {
			unshardedResult, unshardedResultAnnos, err := unshardedClient.Query(tc.query, queryEnd)
			require.NoError(t, err)
			require.NotNil(t, unshardedResult)

			shardedResult, shardedResultAnnos, err := shardedClient.Query(tc.query, queryEnd)
			require.NoError(t, err)
			require.NotNil(t, shardedResult)

			requireModelValueApproxEqual(t, unshardedResult, shardedResult)
			require.Equal(t, unshardedResultAnnos, shardedResultAnnos)
			require.Equal(t, 1, len(unshardedResultAnnos))
		})

		t.Run(tc.name+"/range", func(t *testing.T) {
			unshardedResult, unshardedResultAnnos, err := unshardedClient.QueryRange(tc.query, queryStart, queryEnd, queryStep)
			require.NoError(t, err)
			require.NotNil(t, unshardedResult)

			shardedResult, shardedResultAnnos, err := shardedClient.QueryRange(tc.query, queryStart, queryEnd, queryStep)
			require.NoError(t, err)
			require.NotNil(t, shardedResult)

			requireModelValueApproxEqual(t, unshardedResult, shardedResult)
			require.Equal(t, unshardedResultAnnos, shardedResultAnnos)
			require.Equal(t, 1, len(unshardedResultAnnos))
		})
	}
}

// requireModelValueApproxEqual compares two model.Value results allowing for small
// floating-point precision differences that can arise from sharded vs unsharded evaluation.
func requireModelValueApproxEqual(t *testing.T, expected, actual model.Value) {
	t.Helper()
	require.Equal(t, expected.Type(), actual.Type(), "result types differ")

	const epsilon = 1e-12

	switch exp := expected.(type) {
	case model.Vector:
		act := actual.(model.Vector)
		require.Equal(t, len(exp), len(act), "vector lengths differ")
		for i := range exp {
			require.Equal(t, exp[i].Metric, act[i].Metric, "sample %d: metric labels differ", i)
			require.Equal(t, exp[i].Timestamp, act[i].Timestamp, "sample %d: timestamps differ", i)
			if math.IsNaN(float64(exp[i].Value)) {
				require.True(t, math.IsNaN(float64(act[i].Value)), "sample %d: expected NaN", i)
			} else {
				require.InEpsilon(t, float64(exp[i].Value), float64(act[i].Value), epsilon,
					"sample %d: values differ beyond tolerance", i)
			}
		}
	case *model.Scalar:
		act := actual.(*model.Scalar)
		require.Equal(t, exp.Timestamp, act.Timestamp)
		require.InEpsilon(t, float64(exp.Value), float64(act.Value), epsilon)
	case model.Matrix:
		act := actual.(model.Matrix)
		require.Equal(t, len(exp), len(act), "matrix series count differs")
		for i := range exp {
			require.Equal(t, exp[i].Metric, act[i].Metric, "series %d: metric labels differ", i)
			require.Equal(t, len(exp[i].Values), len(act[i].Values), "series %d: sample count differs", i)
			for j := range exp[i].Values {
				require.Equal(t, exp[i].Values[j].Timestamp, act[i].Values[j].Timestamp,
					"series %d sample %d: timestamps differ", i, j)
				if math.IsNaN(float64(exp[i].Values[j].Value)) {
					require.True(t, math.IsNaN(float64(act[i].Values[j].Value)),
						"series %d sample %d: expected NaN", i, j)
				} else {
					require.InEpsilon(t, float64(exp[i].Values[j].Value), float64(act[i].Values[j].Value), epsilon,
						"series %d sample %d: values differ beyond tolerance", i, j)
				}
			}
		}
	default:
		require.Equal(t, expected, actual, "unsupported model.Value type")
	}
}
