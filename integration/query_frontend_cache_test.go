// SPDX-License-Identifier: AGPL-3.0-only
//go:build requires_docker
// +build requires_docker

package integration

import (
	"testing"
	"time"

	"github.com/grafana/e2e"
	e2ecache "github.com/grafana/e2e/cache"
	e2edb "github.com/grafana/e2e/db"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/integration/e2emimir"
)

func TestQueryFrontendUnalignedQuery(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	memcached := e2ecache.NewMemcached()
	consul := e2edb.NewConsul()
	require.NoError(t, s.StartAndWaitReady(consul, memcached))

	const configFile = ""
	flags := BlocksStorageFlags()

	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(minio))

	flags = mergeFlags(flags, map[string]string{
		"-frontend.cache-results":                     "true",
		"-frontend.split-queries-by-interval":         "2m",
		"-querier.query-ingesters-within":             "12h", // Required by the test on query /series out of ingesters time range
		"-frontend.align-querier-with-step":           "true",
		"-query-frontend.max-cache-freshness":               "0", // Cache everything.
		"-frontend.results-cache.backend":             "memcached",
		"-frontend.results-cache.memcached.addresses": "dns+" + memcached.NetworkEndpoint(e2ecache.MemcachedPort),
	})

	// Start the query-frontend.
	queryFrontendAligned := e2emimir.NewQueryFrontendWithConfigFile("query-frontend-aligned", configFile, mergeFlags(flags, map[string]string{"-frontend.align-querier-with-step": "true"}), "")
	require.NoError(t, s.Start(queryFrontendAligned))

	queryFrontendUnaligned := e2emimir.NewQueryFrontendWithConfigFile("query-frontend-unaligned", configFile, mergeFlags(flags, map[string]string{"-frontend.align-querier-with-step": "false"}), "")
	require.NoError(t, s.Start(queryFrontendUnaligned))

	querierAligned := e2emimir.NewQuerierWithConfigFile("querier-aligned", consul.NetworkHTTPEndpoint(), configFile, mergeFlags(flags, map[string]string{"-querier.frontend-address": queryFrontendAligned.NetworkGRPCEndpoint()}), "")
	querierUnaligned := e2emimir.NewQuerierWithConfigFile("querier-unaligned", consul.NetworkHTTPEndpoint(), configFile, mergeFlags(flags, map[string]string{"-querier.frontend-address": queryFrontendUnaligned.NetworkGRPCEndpoint()}), "")

	// Start all other services.
	ingester := e2emimir.NewIngesterWithConfigFile("ingester", consul.NetworkHTTPEndpoint(), configFile, flags, "")
	distributor := e2emimir.NewDistributorWithConfigFile("distributor", consul.NetworkHTTPEndpoint(), configFile, flags, "")

	require.NoError(t, s.StartAndWaitReady(querierAligned, querierUnaligned, ingester, distributor))
	require.NoError(t, s.WaitReady(queryFrontendAligned, queryFrontendUnaligned))

	// Check if we're discovering memcache or not.
	require.NoError(t, queryFrontendAligned.WaitSumMetrics(e2e.Equals(1), "thanos_memcached_dns_provider_results"))
	require.NoError(t, queryFrontendUnaligned.WaitSumMetrics(e2e.Equals(1), "thanos_memcached_dns_provider_results"))

	// Wait until the distributor and queriers have updated the ring.
	// The distributor should have 512 tokens for the ingester ring and 1 for the distributor ring
	require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(512+1), "cortex_ring_tokens_total"))
	require.NoError(t, querierAligned.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))
	require.NoError(t, querierUnaligned.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))

	// Push a series for each user to Mimir.
	const user = "user"

	c, err := e2emimir.NewClient(distributor.HTTPEndpoint(), "", "", "", user)
	require.NoError(t, err)

	const step = 1 * time.Minute

	now := time.Now()
	// We derive all other times in this test from "now", so make sure that "now" is not step-aligned.
	if now.Truncate(step).Equal(now) {
		now = now.Add(123 * time.Millisecond)
	}

	sampleTime := now.Add(-3 * time.Minute)

	series, expectedVector := generateSeries("series_1", sampleTime)
	val := expectedVector[0].Value

	res, err := c.Push(series)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	start := now.Add(-5 * time.Minute)
	end := now.Add(5 * time.Minute)

	// First query the frontend that is doing step-alignment.
	{
		c, err := e2emimir.NewClient("", queryFrontendAligned.HTTPEndpoint(), "", "", user)
		require.NoError(t, err)

		res, err := c.QueryRange("series_1", start, end, step)
		require.NoError(t, err)

		// Verify that returned range has sample appearing after "sampleTime", all ts are step-aligned (truncated to step).
		expected := generateExpectedValues(start.Truncate(step), end, step, sampleTime, val)

		require.Equal(t, res.Type(), model.ValMatrix)
		require.Equal(t, res.(model.Matrix), expected)
	}

	// Next let's query the frontend that isn't doing step-alignment.
	{
		c, err := e2emimir.NewClient("", queryFrontendUnaligned.HTTPEndpoint(), "", "", user)
		require.NoError(t, err)

		res, err := c.QueryRange("series_1", start, end, step)
		require.NoError(t, err)

		// Verify that returned result is not step-aligned ("now" is not step-aligned)
		require.NotEqual(t, start, start.Truncate(step))
		expected := generateExpectedValues(start, end, step, sampleTime, val)

		require.Equal(t, res.Type(), model.ValMatrix)
		require.Equal(t, res.(model.Matrix), expected)
	}
}

func generateExpectedValues(start time.Time, end time.Time, step time.Duration, sampleTime time.Time, val model.SampleValue) model.Matrix {
	const loopbackPeriod = 5 * time.Minute

	values := []model.SamplePair(nil)
	for ts := start; !ts.After(end); ts = ts.Add(step) {
		if ts.Before(sampleTime) || ts.After(sampleTime.Add(loopbackPeriod)) {
			continue
		}
		values = append(values, model.SamplePair{
			Timestamp: model.Time(e2e.TimeToMilliseconds(ts)),
			Value:     val,
		})
	}

	expected := model.Matrix{
		&model.SampleStream{
			Metric: map[model.LabelName]model.LabelValue{"__name__": "series_1"},
			Values: values,
		},
	}
	return expected
}
