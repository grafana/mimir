// SPDX-License-Identifier: AGPL-3.0-only

//go:build requires_docker

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
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	memcached := e2ecache.NewMemcached()
	require.NoError(t, s.StartAndWaitReady(memcached))

	flags := mergeFlags(BlocksStorageFlags(), BlocksStorageS3Flags(), map[string]string{
		"-querier.query-engine": "mimir",
		"-querier.mimir-query-engine.range-vector-splitting.enabled":             "true",
		"-querier.mimir-query-engine.range-vector-splitting.split-interval":      "5m",
		"-querier.mimir-query-engine.range-vector-splitting.backend":             "memcached",
		"-querier.mimir-query-engine.range-vector-splitting.memcached.addresses": "dns+" + memcached.NetworkEndpoint(e2ecache.MemcachedPort),
	})

	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags)
	ingester := e2emimir.NewIngester("ingester", consul.NetworkHTTPEndpoint(), flags)
	querier := e2emimir.NewQuerier("querier", consul.NetworkHTTPEndpoint(), flags)
	require.NoError(t, s.StartAndWaitReady(distributor, ingester, querier))

	require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(512+1), "cortex_ring_tokens_total"))
	require.NoError(t, querier.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))

	writeClient, err := e2emimir.NewClient(distributor.HTTPEndpoint(), "", "", "", "user-1")
	require.NoError(t, err)

	now := time.Now()
	seriesName := "test_metric"
	numSamples := 21
	sampleValue := 1.0

	// Build samples spanning 20 minutes (one per minute), so a 15m range covers multiple 5m splits.
	samples := make([]prompb.Sample, numSamples)
	for i := 0; i < numSamples; i++ {
		samples[i] = prompb.Sample{
			Value:     sampleValue,
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

	queryClient, err := e2emimir.NewClient("", querier.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	result, err := queryClient.Query("sum_over_time(test_metric[15m])", now)
	require.NoError(t, err)
	require.Equal(t, model.ValVector, result.Type())

	vec := result.(model.Vector)
	require.Len(t, vec, 1)

	// 15m range at `now` covers samples in (now-15m, now], which is 15 samples at 1.0 each.
	expectedSum := model.SampleValue(15)
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
