// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/integration/querier_sharding_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package integration

import (
	"encoding/json"
	"fmt"
	"net/http"
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
	"github.com/grafana/mimir/tools/querytee"
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

		_, _, _, err = c.Query("series_1", now)
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

			result, _, _, err := c.Query("series_1", now)
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

	consul := e2edb.NewConsul()
	require.NoError(t, s.StartAndWaitReady(consul))

	flags := mergeFlags(BlocksStorageFlags(), BlocksStorageS3Flags(), map[string]string{
		// Results caching is deliberately disabled (also the default): the sharded and unsharded requests
		// share tenant, query, and time range, so a shared results cache could serve one path's response to
		// the other and make the comparison pass even if sharded evaluation were wrong.
		"-query-frontend.cache-results":                       "false",
		"-query-frontend.parallelize-shardable-queries":       "true",
		"-query-frontend.query-sharding-total-shards":         "0", // Disable sharding by default.
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

	// Wait until the query-frontend has updated the querier ring.
	require.NoError(t, queryFrontend.WaitSumMetricsWithOptions(e2e.Equals(2), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "querier"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	// Wait until both queriers connect to the query-scheduler, each with the minimum 4 connections.
	// Without this, sharded queries issued through the query-frontend can race the querier-to-scheduler
	// worker connections and time out while no querier is connected yet.
	require.NoError(t, queryScheduler.WaitSumMetrics(e2e.Equals(8), "cortex_query_scheduler_connected_querier_clients"))

	now := time.Now()
	numSamples := 20
	numSeries := 10

	// Push multiple counter-like series with samples spanning 20 minutes (one per minute).
	// Build all series upfront and push in a single batch to avoid timestamp-too-old rejections.
	writeClient, err := e2emimir.NewClient(distributor.HTTPEndpoint(), "", "", "", "sharded-tenant")
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

	// Both clients query the same tenant and data through the query-frontend, so the only difference is
	// whether the request is sharded. unshardedClient disables sharding per-request via the internal
	// Sharding-Control header (querying the querier directly is avoided since those HTTP endpoints are
	// being phased out).
	unshardedClient, err := e2emimir.NewClient("", queryFrontend.HTTPEndpoint(), "", "", "sharded-tenant", e2emimir.WithAddHeader("Sharding-Control", "0"))
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

	// Reuse query-tee's response comparator (the same logic query-tee uses to compare two backends) rather
	// than hand-rolling one. It compares the raw JSON responses of both paths: sample values (with float
	// tolerance and NaN/Inf/StaleNaN handling), result type, and warning/info annotations. Skip options are
	// left at zero so no samples are excluded from the comparison.
	comparator := querytee.NewSamplesComparator(querytee.SampleComparisonOptions{
		Tolerance:        1e-6, // Matches promqltest's defaultEpsilon.
		UseRelativeError: true,
	})

	// runQuery issues a single raw query and asserts how it affected the query-frontend's sharding-rewrite
	// counter: an unsharded request (Sharding-Control: 0) must leave it unchanged, while a sharded request
	// must increase it. Measuring the delta per request keeps the sharded and unsharded paths separate, so we
	// verify each independently instead of only their combined total (which could hide, e.g., the
	// Sharding-Control header being ignored). Only the direction of the change is checked, so this doesn't
	// assume a fixed number of rewrites per query (e.g. it tolerates query splitting).
	runQuery := func(t *testing.T, expectSharded bool, do func() (*http.Response, []byte, error)) []byte {
		t.Helper()
		const rewritesMetric = "cortex_frontend_query_sharding_rewrites_succeeded_total"

		before, err := queryFrontend.SumMetrics([]string{rewritesMetric})
		require.NoError(t, err)

		res, body, reqErr := do()
		resp := requireSuccessfulQueryResponse(t, res, body, reqErr)

		if expectSharded {
			require.NoError(t, queryFrontend.WaitSumMetrics(e2e.Greater(before[0]), rewritesMetric))
		} else {
			require.NoError(t, queryFrontend.WaitSumMetrics(e2e.Equals(before[0]), rewritesMetric))
		}
		return resp
	}

	for _, tc := range queries {
		t.Run(tc.name+"/instant", func(t *testing.T) {
			unshardedResp := runQuery(t, false, func() (*http.Response, []byte, error) { return unshardedClient.QueryRawAt(tc.query, queryEnd) })
			shardedResp := runQuery(t, true, func() (*http.Response, []byte, error) { return shardedClient.QueryRawAt(tc.query, queryEnd) })

			_, err := comparator.Compare(unshardedResp, shardedResp, queryEnd)
			require.NoError(t, err)
			requireSingleInfoAnnotation(t, unshardedResp)
		})

		t.Run(tc.name+"/range", func(t *testing.T) {
			unshardedResp := runQuery(t, false, func() (*http.Response, []byte, error) {
				return unshardedClient.QueryRangeRaw(tc.query, queryStart, queryEnd, queryStep)
			})
			shardedResp := runQuery(t, true, func() (*http.Response, []byte, error) {
				return shardedClient.QueryRangeRaw(tc.query, queryStart, queryEnd, queryStep)
			})

			_, err := comparator.Compare(unshardedResp, shardedResp, queryEnd)
			require.NoError(t, err)
			requireSingleInfoAnnotation(t, unshardedResp)
		})
	}
}

// requireSuccessfulQueryResponse asserts a raw query response was returned with HTTP 200 and returns its body
// for further comparison.
func requireSuccessfulQueryResponse(t *testing.T, res *http.Response, body []byte, err error) []byte {
	t.Helper()
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, res.StatusCode, "unexpected status code, body: %s", body)
	return body
}

// requireSingleInfoAnnotation asserts the raw query response carries exactly one info-level annotation. The
// comparator only checks that both responses' annotations match each other, so this guards that annotations
// are actually present (rate() over the counter data emits one info annotation).
func requireSingleInfoAnnotation(t *testing.T, body []byte) {
	t.Helper()
	var resp querytee.SamplesResponse
	require.NoError(t, json.Unmarshal(body, &resp))
	require.Len(t, resp.Infos, 1)
}
