// SPDX-License-Identifier: AGPL-3.0-only

package integration

import (
	"testing"
	"time"

	"github.com/grafana/e2e"
	e2ecache "github.com/grafana/e2e/cache"
	e2edb "github.com/grafana/e2e/db"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/integration/e2emimir"
)

func TestQuerySplittingWithRangeVectorFunction(t *testing.T) {
	tenantID := "user-1"
	querier, now := setupRangeVectorSplittingTest(t, tenantID, nil)
	queryClient := createQueryClient(t, querier, tenantID)

	result, err := queryClient.Query("sum_over_time(test_metric[15m])", now)
	require.NoError(t, err)
	require.Equal(t, model.ValVector, result.Type())

	vec := result.(model.Vector)
	require.Len(t, vec, 1)

	// 15m range at `now` covers samples in (now-15m, now], which is 15 samples with values 7 through 21.
	expectedSum := model.SampleValue(210)
	assert.Equal(t, expectedSum, vec[0].Value)

	require.NoError(t, querier.WaitSumMetrics(e2e.Equals(1), "cortex_mimir_query_engine_range_vector_splitting_nodes_introduced_total"))

	// Run the same query again to verify cached intermediate results are read back correctly.
	result2, err := queryClient.Query("sum_over_time(test_metric[15m])", now)
	require.NoError(t, err)
	require.Equal(t, model.ValVector, result2.Type())

	vec2 := result2.(model.Vector)
	require.Len(t, vec2, 1)
	assert.Equal(t, expectedSum, vec2[0].Value)

	require.NoError(t, querier.WaitSumMetrics(e2e.Greater(0), "mimir_query_engine_intermediate_result_cache_hits_total"))
}

func TestQuerySplittingWithRangeVectorFunctionAndLBAC(t *testing.T) {
	tenantID := "user-1"
	flags := map[string]string{
		"-auth.label-access-control-enabled": "true",
	}
	querier, now := setupRangeVectorSplittingTest(t, tenantID, flags)
	queryClientWithoutLBAC := createQueryClient(t, querier, tenantID)

	// Run the query without any LBAC policy to populate the cache.
	result, err := queryClientWithoutLBAC.Query("sum_over_time(test_metric[15m])", now)
	require.NoError(t, err)
	require.Equal(t, model.ValVector, result.Type())

	vec := result.(model.Vector)
	require.Len(t, vec, 1)

	// 15m range at `now` covers samples in (now-15m, now], which is 15 samples with values 7 through 21.
	expectedSum := model.SampleValue(210)
	assert.Equal(t, expectedSum, vec[0].Value)

	require.NoError(t, querier.WaitSumMetrics(e2e.Equals(1), "cortex_mimir_query_engine_range_vector_splitting_nodes_introduced_total"))

	// Run the same query again to verify cached intermediate results are read back correctly.
	result2, err := queryClientWithoutLBAC.Query("sum_over_time(test_metric[15m])", now)
	require.NoError(t, err)
	require.Equal(t, model.ValVector, result2.Type())

	vec2 := result2.(model.Vector)
	require.Len(t, vec2, 1)
	assert.Equal(t, expectedSum, vec2[0].Value)

	require.NoError(t, querier.WaitSumMetrics(e2e.Greater(0), "mimir_query_engine_intermediate_result_cache_hits_total"))

	// Run the same query again but with an LBAC policy and verify that the cached results are not used.
	policy := tenantID + ":%7B__name__!=%22test_metric%22%7D"
	queryClientWithLBAC := createQueryClient(t, querier, tenantID, e2emimir.WithAddHeader("X-Prom-Label-Policy", policy))

	resultWithLBACPolicy, err := queryClientWithLBAC.Query("sum_over_time(test_metric[15m])", now)
	require.NoError(t, err)
	require.Equal(t, model.ValVector, resultWithLBACPolicy.Type())

	vec3 := resultWithLBACPolicy.(model.Vector)
	require.Len(t, vec3, 0, "the test_metric series should be filtered out by the LBAC policy applied to the request")
}

func setupRangeVectorSplittingTest(t *testing.T, tenantID string, extraFlags map[string]string) (*e2emimir.MimirService, time.Time) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	t.Cleanup(s.Close)

	memcached := e2ecache.NewMemcached()
	require.NoError(t, s.StartAndWaitReady(memcached))

	flags := mergeFlags(BlocksStorageFlags(), BlocksStorageS3Flags(), map[string]string{
		"-querier.query-engine": "mimir",
		"-querier.mimir-query-engine.range-vector-splitting.enabled":                                   "true",
		"-querier.mimir-query-engine.range-vector-splitting.split-interval":                            "5m",
		"-querier.mimir-query-engine.range-vector-splitting.backend":                                   "memcached",
		"-querier.mimir-query-engine.range-vector-splitting.memcached.addresses":                       "dns+" + memcached.NetworkEndpoint(e2ecache.MemcachedPort),
		"-querier.mimir-query-engine.enable-range-query-range-vector-common-subexpression-elimination": "true",
	}, extraFlags)

	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags)
	ingester := e2emimir.NewIngester("ingester", consul.NetworkHTTPEndpoint(), flags)
	querier := e2emimir.NewQuerier("querier", consul.NetworkHTTPEndpoint(), flags)
	require.NoError(t, s.StartAndWaitReady(distributor, ingester, querier))

	require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(512+1), "cortex_ring_tokens_total"))
	require.NoError(t, querier.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))

	writeClient, err := e2emimir.NewClient(distributor.HTTPEndpoint(), "", "", "", tenantID)
	require.NoError(t, err)

	now := time.Now()
	seriesName := "test_metric"
	numSamples := 21

	// Build samples spanning 20 minutes (one per minute), so a 15m range covers multiple 5m splits.
	samples := make([]prompb.Sample, numSamples)
	for i := 0; i < numSamples; i++ {
		samples[i] = prompb.Sample{
			Value:     float64(i + 1),
			Timestamp: now.Add(time.Duration(i-numSamples+1) * time.Minute).UnixMilli(),
		}
	}

	series := []prompb.TimeSeries{
		{
			Labels: []prompb.Label{
				{Name: model.MetricNameLabel, Value: seriesName},
			},
			Samples: samples,
		},
	}

	res, err := writeClient.Push(series)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	return querier, now
}

func createQueryClient(t *testing.T, querier *e2emimir.MimirService, tenantID string, opts ...e2emimir.ClientOption) *e2emimir.Client {
	client, err := e2emimir.NewClient("", querier.HTTPEndpoint(), "", "", tenantID, opts...)
	require.NoError(t, err)
	return client
}
