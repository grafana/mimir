// SPDX-License-Identifier: AGPL-3.0-only
//go:build requires_docker

package integration

import (
	"testing"
	"time"

	"github.com/grafana/dskit/cache"
	"github.com/grafana/e2e"
	e2ecache "github.com/grafana/e2e/cache"
	e2edb "github.com/grafana/e2e/db"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/integration/e2emimir"
)

func TestQueryFrontendUnalignedQuery(t *testing.T) {
	for _, backend := range []string{cache.BackendMemcached, cache.BackendRedis} {
		t.Run(backend, func(t *testing.T) {
			s, err := e2e.NewScenario(networkName)
			require.NoError(t, err)
			defer s.Close()

			cacheService := cacheBackend(backend)
			consul := e2edb.NewConsul()
			require.NoError(t, s.StartAndWaitReady(consul, cacheService))

			const configFile = ""
			flags := mergeFlags(
				BlocksStorageFlags(),
				BlocksStorageS3Flags(),
			)

			minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
			require.NoError(t, s.StartAndWaitReady(minio))

			flags = mergeFlags(flags, map[string]string{
				"-query-frontend.cache-results":             "true",
				"-query-frontend.split-queries-by-interval": "2m",
				"-query-frontend.max-cache-freshness":       "0", // Cache everything.
			}, cacheConfig(backend, cacheService))

			// Start the query-frontend.
			queryFrontendAligned := e2emimir.NewQueryFrontend("query-frontend-aligned", consul.NetworkHTTPEndpoint(), mergeFlags(flags, map[string]string{"-query-frontend.align-queries-with-step": "true"}), e2emimir.WithConfigFile(configFile))
			require.NoError(t, s.Start(queryFrontendAligned))

			queryFrontendUnaligned := e2emimir.NewQueryFrontend("query-frontend-unaligned", consul.NetworkHTTPEndpoint(), mergeFlags(flags, map[string]string{"-query-frontend.align-queries-with-step": "false"}), e2emimir.WithConfigFile(configFile))
			require.NoError(t, s.Start(queryFrontendUnaligned))

			querierAligned := e2emimir.NewQuerier("querier-aligned", consul.NetworkHTTPEndpoint(), mergeFlags(flags, map[string]string{"-querier.frontend-address": queryFrontendAligned.NetworkGRPCEndpoint()}), e2emimir.WithConfigFile(configFile))
			querierUnaligned := e2emimir.NewQuerier("querier-unaligned", consul.NetworkHTTPEndpoint(), mergeFlags(flags, map[string]string{"-querier.frontend-address": queryFrontendUnaligned.NetworkGRPCEndpoint()}), e2emimir.WithConfigFile(configFile))

			// Start all other services.
			ingester := e2emimir.NewIngester("ingester", consul.NetworkHTTPEndpoint(), flags, e2emimir.WithConfigFile(configFile))
			distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags, e2emimir.WithConfigFile(configFile))

			require.NoError(t, s.StartAndWaitReady(querierAligned, querierUnaligned, ingester, distributor))
			require.NoError(t, s.WaitReady(queryFrontendAligned, queryFrontendUnaligned))

			// Check if we're discovering the cache only when running Memcached. The Redis client handles
			// discovery itself when running a cluster or connects to a single machine.
			if backend == cache.BackendMemcached {
				require.NoError(t, queryFrontendAligned.WaitSumMetrics(e2e.Equals(1), "thanos_cache_dns_provider_results"))
				require.NoError(t, queryFrontendUnaligned.WaitSumMetrics(e2e.Equals(1), "thanos_cache_dns_provider_results"))
			}

			// Wait until the distributor and queriers have updated the ring.
			// The distributor should have 512 tokens for the ingester ring and 1 for the distributor ring
			require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(512+1), "cortex_ring_tokens_total"))
			require.NoError(t, querierAligned.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))
			require.NoError(t, querierUnaligned.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))

			// Push a series for each user to Mimir.
			const user = "user"

			c, err := e2emimir.NewClient(distributor.HTTPEndpoint(), "", "", "", user)
			require.NoError(t, err)

			runTestPushSeriesAndUnalignedQuery(t, c, queryFrontendAligned, queryFrontendUnaligned, user, "series_1", generateFloatSeries, generateExpectedFloats)
			runTestPushSeriesAndUnalignedQuery(t, c, queryFrontendAligned, queryFrontendUnaligned, user, "hseries_1", generateHistogramSeries, generateExpectedHistograms)
		})
	}
}

// generateExpectedFunc defines how to generate the expected float values or histograms from the expectedVector returned with the generated series
type generateExpectedFunc func(start time.Time, end time.Time, step time.Duration, sampleTime time.Time, expectedVector model.Vector, seriesName string) model.Matrix

func runTestPushSeriesAndUnalignedQuery(t *testing.T, c *e2emimir.Client, queryFrontendAligned, queryFrontendUnaligned *e2emimir.MimirService, user, seriesName string, genSeries generateSeriesFunc, genExpected generateExpectedFunc) {
	const step = 1 * time.Minute

	now := time.Now()
	// We derive all other times in this test from "now", so make sure that "now" is not step-aligned.
	if now.Truncate(step).Equal(now) {
		now = now.Add(123 * time.Millisecond)
	}

	sampleTime := now.Add(-3 * time.Minute)

	series, expectedVector, _ := genSeries(seriesName, sampleTime)

	res, err := c.Push(series)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	start := now.Add(-5 * time.Minute)
	end := now.Add(5 * time.Minute)

	// First query the frontend that is doing step-alignment.
	{
		c, err := e2emimir.NewClient("", queryFrontendAligned.HTTPEndpoint(), "", "", user)
		require.NoError(t, err)

		res, err := c.QueryRange(seriesName, start, end, step)
		require.NoError(t, err)

		// Verify that returned range has sample appearing after "sampleTime", all ts are step-aligned (truncated to step).
		expected := genExpected(start.Truncate(step), end, step, sampleTime, expectedVector, seriesName)

		require.Equal(t, res.Type(), model.ValMatrix)
		require.Equal(t, res.(model.Matrix), expected)
	}

	// Next let's query the frontend that isn't doing step-alignment.
	{
		c, err := e2emimir.NewClient("", queryFrontendUnaligned.HTTPEndpoint(), "", "", user)
		require.NoError(t, err)

		res, err := c.QueryRange(seriesName, start, end, step)
		require.NoError(t, err)

		// Verify that returned result is not step-aligned ("now" is not step-aligned)
		require.NotEqual(t, start, start.Truncate(step))
		expected := genExpected(start, end, step, sampleTime, expectedVector, seriesName)

		require.Equal(t, res.Type(), model.ValMatrix)
		require.Equal(t, res.(model.Matrix), expected)
	}

}

func cacheBackend(backend string) *e2e.ConcreteService {
	if backend == cache.BackendRedis {
		return e2ecache.NewRedis()
	}

	return e2ecache.NewMemcached()
}

func cacheConfig(backend string, service *e2e.ConcreteService) map[string]string {
	if backend == cache.BackendRedis {
		return map[string]string{
			"-query-frontend.results-cache.backend":        "redis",
			"-query-frontend.results-cache.redis.endpoint": service.NetworkEndpoint(e2ecache.RedisPort),
		}
	}

	return map[string]string{
		"-query-frontend.results-cache.backend":             "memcached",
		"-query-frontend.results-cache.memcached.addresses": "dns+" + service.NetworkEndpoint(e2ecache.MemcachedPort),
	}
}

func generateExpectedFloats(start time.Time, end time.Time, step time.Duration, sampleTime time.Time, expectedVector model.Vector, seriesName string) model.Matrix {
	val := expectedVector[0].Value

	const lookbackPeriod = 5 * time.Minute

	values := []model.SamplePair(nil)
	for ts := start; !ts.After(end); ts = ts.Add(step) {
		if ts.Before(sampleTime) || ts.After(sampleTime.Add(lookbackPeriod)) {
			continue
		}
		values = append(values, model.SamplePair{
			Timestamp: model.Time(e2e.TimeToMilliseconds(ts)),
			Value:     val,
		})
	}

	expected := model.Matrix{
		&model.SampleStream{
			Metric: map[model.LabelName]model.LabelValue{"__name__": model.LabelValue(seriesName)},
			Values: values,
		},
	}
	return expected
}

func generateExpectedHistograms(start time.Time, end time.Time, step time.Duration, sampleTime time.Time, expectedVector model.Vector, seriesName string) model.Matrix {
	hist := expectedVector[0].Histogram

	const lookbackPeriod = 5 * time.Minute

	histograms := []model.SampleHistogramPair(nil)
	for ts := start; !ts.After(end); ts = ts.Add(step) {
		if ts.Before(sampleTime) || ts.After(sampleTime.Add(lookbackPeriod)) {
			continue
		}
		histograms = append(histograms, model.SampleHistogramPair{
			Timestamp: model.Time(e2e.TimeToMilliseconds(ts)),
			Histogram: hist,
		})
	}

	expected := model.Matrix{
		&model.SampleStream{
			Metric:     map[model.LabelName]model.LabelValue{"__name__": model.LabelValue(seriesName)},
			Histograms: histograms,
		},
	}
	return expected
}
