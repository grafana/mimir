// SPDX-License-Identifier: AGPL-3.0-only
//go:build requires_docker

package integration

import (
	"fmt"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/grafana/e2e"
	e2ecache "github.com/grafana/e2e/cache"
	e2edb "github.com/grafana/e2e/db"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/integration/e2emimir"
	"github.com/grafana/mimir/pkg/cardinality"
)

func TestActiveSeriesWithQuerySharding(t *testing.T) {
	for _, tc := range []struct {
		querySchedulerEnabled bool
		shardingEnabled       bool
	}{
		{true, false},
		{true, true},
		{false, true},
	} {
		config := queryFrontendTestConfig{
			queryStatsEnabled:           true,
			shardActiveSeriesQueries:    tc.shardingEnabled,
			querySchedulerEnabled:       tc.querySchedulerEnabled,
			querySchedulerDiscoveryMode: "ring",
			setup: func(t *testing.T, s *e2e.Scenario) (string, map[string]string) {
				flags := mergeFlags(BlocksStorageFlags(), BlocksStorageS3Flags(),
					map[string]string{
						"-querier.response-streaming-enabled": "true",
					},
				)
				minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
				require.NoError(t, s.StartAndWaitReady(minio))

				return "", flags
			},
		}

		testName := fmt.Sprintf("query scheduler=%v/query sharding=%v",
			tc.querySchedulerEnabled, tc.shardingEnabled,
		)
		t.Run("active series/"+testName, func(t *testing.T) {
			runTestActiveSeriesWithQueryShardingHTTPTest(t, config)
		})

		t.Run("active native histograms/"+testName, func(t *testing.T) {
			runTestActiveNativeHistogramMetricsWithQueryShardingHTTPTest(t, config)
		})
	}
}

const (
	metricName = "test_metric"
)

func runTestActiveSeriesWithQueryShardingHTTPTest(t *testing.T, cfg queryFrontendTestConfig) {
	numSeries := 100
	s, c := setupComponentsForActiveSeriesTest(t, cfg, numSeries)
	defer s.Close()

	// Query active series.
	for _, options := range [][]e2emimir.ActiveSeriesOption{
		{e2emimir.WithRequestMethod(http.MethodGet)},
		{e2emimir.WithRequestMethod(http.MethodPost)},
		{e2emimir.WithRequestMethod(http.MethodGet), e2emimir.WithEnableCompression()},
		{e2emimir.WithRequestMethod(http.MethodPost), e2emimir.WithEnableCompression()},
		{e2emimir.WithQueryShards(1)},
		{e2emimir.WithQueryShards(12)},
	} {
		response, err := c.ActiveSeries(metricName, options...)
		require.NoError(t, err)
		require.Len(t, response.Data, numSeries)
	}

	var err error
	_, err = c.ActiveSeries(metricName, e2emimir.WithQueryShards(512))
	if cfg.shardActiveSeriesQueries {
		require.Error(t, err)
		require.Contains(t, err.Error(), "shard count 512 exceeds allowed maximum (128)")
	} else {
		require.NoError(t, err)
	}
}

func runTestActiveNativeHistogramMetricsWithQueryShardingHTTPTest(t *testing.T, cfg queryFrontendTestConfig) {
	numSeries := 100
	s, c := setupComponentsForActiveSeriesTest(t, cfg, numSeries)
	defer s.Close()

	// Query active series.
	for _, options := range [][]e2emimir.ActiveSeriesOption{
		{e2emimir.WithRequestMethod(http.MethodGet)},
		{e2emimir.WithRequestMethod(http.MethodPost)},
		{e2emimir.WithRequestMethod(http.MethodGet), e2emimir.WithEnableCompression()},
		{e2emimir.WithRequestMethod(http.MethodPost), e2emimir.WithEnableCompression()},
		{e2emimir.WithQueryShards(1)},
		{e2emimir.WithQueryShards(12)},
	} {
		response, err := c.ActiveNativeHistogramMetrics(metricName, options...)
		require.NoError(t, err)
		require.Len(t, response.Data, 1)
		require.Equal(t, response.Data[0], cardinality.ActiveMetricWithBucketCount{
			Metric:         metricName,
			SeriesCount:    uint64(numSeries / 2),     // Only half of the series are native histograms.
			BucketCount:    uint64(numSeries / 2 * 8), // We add up the bucket count of all histograms.
			AvgBucketCount: 8.0,
			MinBucketCount: 8,
			MaxBucketCount: 8,
		})
	}

	var err error
	_, err = c.ActiveNativeHistogramMetrics(metricName, e2emimir.WithQueryShards(512))
	if cfg.shardActiveSeriesQueries {
		require.Error(t, err)
		require.Contains(t, err.Error(), "shard count 512 exceeds allowed maximum (128)")
	} else {
		require.NoError(t, err)
	}
}

func setupComponentsForActiveSeriesTest(t *testing.T, cfg queryFrontendTestConfig, numSeries int) (*e2e.Scenario, *e2emimir.Client) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)

	memcached := e2ecache.NewMemcached()
	consul := e2edb.NewConsul()
	require.NoError(t, s.StartAndWaitReady(consul, memcached))

	configFile, flags := cfg.setup(t, s)

	flags = mergeFlags(flags, map[string]string{
		"-query-frontend.cache-results":                      "true",
		"-query-frontend.results-cache.backend":              "memcached",
		"-query-frontend.results-cache.memcached.addresses":  "dns+" + memcached.NetworkEndpoint(e2ecache.MemcachedPort),
		"-query-frontend.query-stats-enabled":                strconv.FormatBool(cfg.queryStatsEnabled),
		"-query-frontend.query-sharding-total-shards":        "32",
		"-query-frontend.query-sharding-max-sharded-queries": "128",
		"-querier.cardinality-analysis-enabled":              "true",
		"-querier.max-concurrent":                            "128",
	})

	// Start the query-scheduler if enabled.
	var queryScheduler *e2emimir.MimirService
	if cfg.querySchedulerEnabled && cfg.querySchedulerDiscoveryMode == "dns" {
		queryScheduler = e2emimir.NewQueryScheduler("query-scheduler", flags)
		require.NoError(t, s.StartAndWaitReady(queryScheduler))
		flags["-query-frontend.scheduler-address"] = queryScheduler.NetworkGRPCEndpoint()
		flags["-querier.scheduler-address"] = queryScheduler.NetworkGRPCEndpoint()
	} else if cfg.querySchedulerEnabled && cfg.querySchedulerDiscoveryMode == "ring" {
		flags["-query-scheduler.service-discovery-mode"] = "ring"
		flags["-query-scheduler.ring.store"] = "consul"
		flags["-query-scheduler.ring.consul.hostname"] = consul.NetworkHTTPEndpoint()

		queryScheduler = e2emimir.NewQueryScheduler("query-scheduler", flags)
		require.NoError(t, s.StartAndWaitReady(queryScheduler))
	}

	if cfg.shardActiveSeriesQueries {
		flags["-query-frontend.shard-active-series-queries"] = "true"
	}

	// Start the query-frontend.
	queryFrontend := e2emimir.NewQueryFrontend("query-frontend", consul.NetworkHTTPEndpoint(), flags, e2emimir.WithConfigFile(configFile))
	require.NoError(t, s.Start(queryFrontend))

	if !cfg.querySchedulerEnabled {
		flags["-querier.frontend-address"] = queryFrontend.NetworkGRPCEndpoint()
	}

	// Start all other services.
	ingester := e2emimir.NewIngester("ingester", consul.NetworkHTTPEndpoint(), flags, e2emimir.WithConfigFile(configFile))
	distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags, e2emimir.WithConfigFile(configFile))
	querier := e2emimir.NewQuerier("querier", consul.NetworkHTTPEndpoint(), flags, e2emimir.WithConfigFile(configFile))

	require.NoError(t, s.StartAndWaitReady(querier, ingester, distributor))
	require.NoError(t, s.WaitReady(queryFrontend))

	// Check if we're discovering memcached or not.
	require.NoError(t, queryFrontend.WaitSumMetrics(e2e.Equals(1), "thanos_cache_dns_provider_results"))
	require.NoError(t, queryFrontend.WaitSumMetrics(e2e.Greater(0), "thanos_cache_dns_lookups_total"))

	// Wait until distributor and querier have updated the ingesters ring.
	require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	require.NoError(t, querier.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	// Push series for the test user to Mimir.
	c, err := e2emimir.NewClient(distributor.HTTPEndpoint(), queryFrontend.HTTPEndpoint(), "", "", userID)
	require.NoError(t, err)
	now := time.Now()
	series := make([]prompb.TimeSeries, numSeries)
	for i := 0; i < numSeries; i++ {
		var ts []prompb.TimeSeries
		if i%2 == 0 {
			// Let half of the series be float samples and the other half native histograms.
			ts, _, _ = e2e.GenerateSeries(metricName, now, prompb.Label{Name: "index", Value: strconv.Itoa(i)})
		} else {
			ts, _, _ = generateHistogramSeries(metricName, now, prompb.Label{Name: "index", Value: strconv.Itoa(i)})
		}
		series[i] = ts[0]
	}
	res, err := c.Push(series)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, res.StatusCode)

	return s, c
}
