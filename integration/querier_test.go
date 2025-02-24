// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/integration/querier_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.
//go:build requires_docker

package integration

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/grafana/e2e"
	e2ecache "github.com/grafana/e2e/cache"
	e2edb "github.com/grafana/e2e/db"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/integration/e2emimir"
	"github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/util"
)

func TestQuerierWithBlocksStorageRunningInMicroservicesMode(t *testing.T) {
	testQuerierWithBlocksStorageRunningInMicroservicesMode(t, generateFloatSeries)
}

func TestQuerierWithBlocksStorageRunningInMicroservicesModeWithHistograms(t *testing.T) {
	testQuerierWithBlocksStorageRunningInMicroservicesMode(t, generateHistogramSeries)
}

func testQuerierWithBlocksStorageRunningInMicroservicesMode(t *testing.T, seriesGenerator func(name string, ts time.Time, additionalLabels ...prompb.Label) (series []prompb.TimeSeries, vector model.Vector, matrix model.Matrix)) {
	tests := map[string]struct {
		tenantShardSize      int
		indexCacheBackend    string
		queryShardingEnabled bool
	}{
		"shard size 0, inmemory index cache": {
			tenantShardSize:   0,
			indexCacheBackend: tsdb.IndexCacheBackendInMemory,
		},
		"shard size 0, memcached index cache": {
			tenantShardSize:   0,
			indexCacheBackend: tsdb.IndexCacheBackendMemcached,
		},
		"shard size 1, memcached index cache": {
			tenantShardSize:   1,
			indexCacheBackend: tsdb.IndexCacheBackendMemcached,
		},
		"shard size 1, memcached index cache, query sharding enabled": {
			tenantShardSize:      1,
			indexCacheBackend:    tsdb.IndexCacheBackendMemcached,
			queryShardingEnabled: true,
		},
	}

	const blockRangePeriod = 5 * time.Second

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Configure the blocks storage to frequently compact TSDB head
	// and ship blocks to the storage.
	commonFlags := mergeFlags(BlocksStorageFlags(), BlocksStorageS3Flags(), map[string]string{
		"-blocks-storage.tsdb.block-ranges-period":      blockRangePeriod.String(),
		"-blocks-storage.tsdb.ship-interval":            "1s",
		"-blocks-storage.tsdb.retention-period":         "1ms", // Retention period counts from the moment the block was uploaded to storage so we're setting it deliberatelly small so block gets deleted as soon as possible
		"-compactor.first-level-compaction-wait-period": "1m",  // Do not compact aggressively
	})

	// Start dependencies in common with all test cases.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, commonFlags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	// Start Mimir components in common with all test cases.
	distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), commonFlags)
	ingester := e2emimir.NewIngester("ingester", consul.NetworkHTTPEndpoint(), commonFlags)
	require.NoError(t, s.StartAndWaitReady(distributor, ingester))

	// Wait until both the distributor and querier have updated the ring. The querier will also watch
	// the store-gateway ring if blocks sharding is enabled.
	// The distributor should have 512 tokens for the ingester ring and 1 for the distributor ring
	require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(512+1), "cortex_ring_tokens_total"))

	// Push some series to Mimir.
	writeClient, err := e2emimir.NewClient(distributor.HTTPEndpoint(), "", "", "", "user-1")
	require.NoError(t, err)

	series1Name := "series_1"
	series2Name := "series_2"
	series1Timestamp := time.Now()
	series2Timestamp := series1Timestamp.Add(blockRangePeriod * 2)
	series1, expectedVector1, _ := seriesGenerator(series1Name, series1Timestamp, prompb.Label{Name: series1Name, Value: series1Name})
	series2, expectedVector2, _ := seriesGenerator(series2Name, series2Timestamp, prompb.Label{Name: series2Name, Value: series2Name})

	res, err := writeClient.Push(series1)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	res, err = writeClient.Push(series2)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	// Wait until the TSDB head is compacted and shipped to the storage.
	// The shipped block contains the 1st series, while the 2ns series is in the head.
	require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(1), "cortex_ingester_shipper_uploads_total"))
	require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(1), "cortex_ingester_memory_series"))
	require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(2), "cortex_ingester_memory_series_created_total"))
	require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(1), "cortex_ingester_memory_series_removed_total"))

	// Push another series to further compact another block and delete the first block
	// due to expired retention.
	series3Name := "series_3"
	series3Timestamp := series2Timestamp.Add(blockRangePeriod * 2)
	series3, expectedVector3, _ := seriesGenerator(series3Name, series3Timestamp, prompb.Label{Name: series3Name, Value: series3Name})

	res, err = writeClient.Push(series3)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(2), "cortex_ingester_shipper_uploads_total"))
	require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(1), "cortex_ingester_memory_series"))
	require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(3), "cortex_ingester_memory_series_created_total"))
	require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(2), "cortex_ingester_memory_series_removed_total"))

	// Start the compactor to have the bucket index created before querying.
	compactor := e2emimir.NewCompactor("compactor", consul.NetworkHTTPEndpoint(), commonFlags)
	require.NoError(t, s.StartAndWaitReady(compactor))

	for testName, testCfg := range tests {
		t.Run(testName, func(t *testing.T) {
			// We start a dedicated memcached for each test case because we want each test to start
			// with an empty cache.
			memcached := e2ecache.NewMemcached()
			t.Cleanup(func() { require.NoError(t, s.Stop(memcached)) })
			require.NoError(t, s.StartAndWaitReady(memcached))

			flags := mergeFlags(commonFlags, map[string]string{
				"-blocks-storage.bucket-store.index-cache.memcached.addresses": "dns+" + memcached.NetworkEndpoint(e2ecache.MemcachedPort),
				"-blocks-storage.bucket-store.sync-interval":                   "1s",
				"-blocks-storage.bucket-store.index-cache.backend":             testCfg.indexCacheBackend,
				"-store-gateway.tenant-shard-size":                             fmt.Sprintf("%d", testCfg.tenantShardSize),
				"-query-frontend.query-stats-enabled":                          "true",
				"-query-frontend.parallelize-shardable-queries":                strconv.FormatBool(testCfg.queryShardingEnabled),
			})

			// Start store-gateways.
			storeGateway1 := e2emimir.NewStoreGateway("store-gateway-1", consul.NetworkHTTPEndpoint(), flags)
			storeGateway2 := e2emimir.NewStoreGateway("store-gateway-2", consul.NetworkHTTPEndpoint(), flags)
			storeGateways := e2emimir.NewCompositeMimirService(storeGateway1, storeGateway2)
			t.Cleanup(func() { require.NoError(t, s.Stop(storeGateway1, storeGateway2)) })
			require.NoError(t, s.StartAndWaitReady(storeGateway1, storeGateway2))

			// Start the query-frontend but do not check for readiness yet.
			queryFrontend := e2emimir.NewQueryFrontend("query-frontend", consul.NetworkHTTPEndpoint(), flags)
			t.Cleanup(func() { require.NoError(t, s.Stop(queryFrontend)) })
			require.NoError(t, s.Start(queryFrontend))

			// Configure the querier to connect to the query-frontend.
			flags["-querier.frontend-address"] = queryFrontend.NetworkGRPCEndpoint()

			// Start the querier with configuring store-gateway addresses if sharding is disabled.
			querier := e2emimir.NewQuerier("querier", consul.NetworkHTTPEndpoint(), flags)
			t.Cleanup(func() { require.NoError(t, s.Stop(querier)) })
			require.NoError(t, s.StartAndWaitReady(querier))
			require.NoError(t, s.WaitReady(queryFrontend))

			// Wait until the querier has updated the ring. The querier will also watch
			// the store-gateway ring if blocks sharding is enabled.
			require.NoError(t, querier.WaitSumMetrics(e2e.Equals(float64(512+(512*storeGateways.NumInstances()))), "cortex_ring_tokens_total"))

			// Wait until the store-gateway has synched the new uploaded blocks. When sharding is enabled
			// we don't known which store-gateway instance will synch the blocks, so we need to wait on
			// metrics extracted from all instances.
			require.NoError(t, storeGateways.WaitSumMetrics(e2e.Equals(2), "cortex_bucket_store_blocks_loaded"))

			// Check how many tenants have been discovered and synced by store-gateways.
			require.NoError(t, storeGateways.WaitSumMetrics(e2e.Equals(float64(1*storeGateways.NumInstances())), "cortex_bucket_stores_tenants_discovered"))
			if testCfg.tenantShardSize > 0 {
				require.NoError(t, storeGateways.WaitSumMetrics(e2e.Equals(float64(1)), "cortex_bucket_stores_tenants_synced"))
			} else {
				require.NoError(t, storeGateways.WaitSumMetrics(e2e.Equals(float64(1*storeGateways.NumInstances())), "cortex_bucket_stores_tenants_synced"))
			}

			// Query back the series (1 only in the storage, 1 only in the ingesters, 1 on both).
			expectedFetchedSeries := 0

			instantQueriesCount := 0
			c, err := e2emimir.NewClient("", queryFrontend.HTTPEndpoint(), "", "", "user-1")
			require.NoError(t, err)
			instantQueriesCount++

			result, err := c.Query(series1Name, series1Timestamp)
			require.NoError(t, err)
			require.Equal(t, model.ValVector, result.Type())
			assert.Equal(t, expectedVector1, result.(model.Vector))
			expectedFetchedSeries++ // Storage only.
			// thanos_store_index_cache_requests_total: ExpandedPostings: 1, Postings: 1, Series: 1
			instantQueriesCount++

			result, err = c.Query(series2Name, series2Timestamp)
			require.NoError(t, err)
			require.Equal(t, model.ValVector, result.Type())
			assert.Equal(t, expectedVector2, result.(model.Vector))
			expectedFetchedSeries += 2 // Ingester + storage.
			// thanos_store_index_cache_requests_total: ExpandedPostings: 3, Postings: 2, Series: 2
			instantQueriesCount++

			result, err = c.Query(series3Name, series3Timestamp)
			require.NoError(t, err)
			require.Equal(t, model.ValVector, result.Type())
			assert.Equal(t, expectedVector3, result.(model.Vector))
			expectedFetchedSeries++ // Ingester only.
			// thanos_store_index_cache_requests_total: ExpandedPostings: 5, Postings: 2, Series: 2
			instantQueriesCount++

			// Make sure the querier is using the bucket index blocks finder.
			require.NoError(t, querier.WaitSumMetrics(e2e.Greater(0), "cortex_bucket_index_loads_total"))

			// Check the in-memory index cache metrics (in the store-gateway).
			require.NoError(t, storeGateways.WaitSumMetrics(e2e.Equals(9), "thanos_store_index_cache_requests_total")) // 5 + 2 + 2
			require.NoError(t, storeGateways.WaitSumMetrics(e2e.Equals(0), "thanos_store_index_cache_hits_total"))     // no cache hit cause the cache was empty

			if testCfg.indexCacheBackend == tsdb.IndexCacheBackendInMemory {
				require.NoError(t, storeGateways.WaitSumMetrics(e2e.Equals(2*2+2+3), "thanos_store_index_cache_items"))             // 2 series both for postings and series cache, 2 expanded postings on one block, 3 on another one
				require.NoError(t, storeGateways.WaitSumMetrics(e2e.Equals(2*2+2+3), "thanos_store_index_cache_items_added_total")) // 2 series both for postings and series cache, 2 expanded postings on one block, 3 on another one
			} else if testCfg.indexCacheBackend == tsdb.IndexCacheBackendMemcached {
				require.NoError(t, storeGateways.WaitSumMetrics(e2e.Equals(9*2), "thanos_cache_operations_total")) // one set for each get
			}

			// Query back again the 1st series from storage. This time it should use the index cache.
			result, err = c.Query(series1Name, series1Timestamp)
			require.NoError(t, err)
			require.Equal(t, model.ValVector, result.Type())
			assert.Equal(t, expectedVector1, result.(model.Vector))
			expectedFetchedSeries++ // Storage only.

			require.NoError(t, storeGateways.WaitSumMetrics(e2e.Equals(9+2), "thanos_store_index_cache_requests_total"))
			require.NoError(t, storeGateways.WaitSumMetrics(e2e.Equals(2), "thanos_store_index_cache_hits_total")) // this time has used the index cache

			if testCfg.indexCacheBackend == tsdb.IndexCacheBackendInMemory {
				require.NoError(t, storeGateways.WaitSumMetrics(e2e.Equals(2*2+2+3), "thanos_store_index_cache_items"))             // as before
				require.NoError(t, storeGateways.WaitSumMetrics(e2e.Equals(2*2+2+3), "thanos_store_index_cache_items_added_total")) // as before
			} else if testCfg.indexCacheBackend == tsdb.IndexCacheBackendMemcached {
				require.NoError(t, storeGateways.WaitSumMetrics(e2e.Equals(9*2+2), "thanos_cache_operations_total")) // as before + 2 gets (expanded postings and series)
			}

			// Query range. We expect 1 data point with a value of 3 (number of series).
			// Run this query multiple times to ensure each time we get the same result.
			const numRangeQueries = 10

			for i := 0; i < numRangeQueries; i++ {
				result, err = c.QueryRange(`count({__name__=~"series.*"})`, series3Timestamp, series3Timestamp, time.Minute)
				require.NoError(t, err)
				require.Equal(t, model.ValMatrix, result.Type())

				matrix := result.(model.Matrix)
				require.Equal(t, 1, len(matrix))
				require.Equal(t, 1, len(matrix[0].Values))
				assert.Equal(t, model.SampleValue(3), matrix[0].Values[0].Value)
				expectedFetchedSeries += 4 // series_2 is fetched both from ingester and storage, while other series are fetched either from ingester or storage.
			}

			// This instant query can be sharded.
			result, err = c.Query(`count({__name__=~"series.*"})`, series3Timestamp)
			require.NoError(t, err)
			require.Equal(t, model.ValVector, result.Type())
			vector := result.(model.Vector)
			require.Equal(t, 1, len(vector))
			require.Equal(t, model.SampleValue(3), vector[0].Value)
			instantQueriesCount++
			expectedFetchedSeries += 4 // series_2 is fetched both from ingester and storage, while other series are fetched either from ingester or storage.

			// When query sharding is enabled, we expect the range & instant queries above to be sharded.
			if testCfg.queryShardingEnabled {
				require.NoError(t, queryFrontend.WaitSumMetrics(e2e.Equals(numRangeQueries+float64(instantQueriesCount)), "cortex_frontend_query_sharding_rewrites_attempted_total"))
				require.NoError(t, queryFrontend.WaitSumMetrics(e2e.Equals(numRangeQueries+1), "cortex_frontend_query_sharding_rewrites_succeeded_total"))
				require.NoError(t, queryFrontend.WaitSumMetrics(e2e.Equals((numRangeQueries+1)*16), "cortex_frontend_sharded_queries_total"))
			}

			// Check query stats (supported only when gRPC streaming is enabled).
			require.NoError(t, queryFrontend.WaitSumMetrics(e2e.Equals(float64(expectedFetchedSeries)), "cortex_query_fetched_series_total"))

			// Query metadata.
			testMetadataQueriesWithBlocksStorage(t, c, series1[0], series2[0], series3[0], blockRangePeriod)

			// Ensure no service-specific metrics prefix is used by the wrong service.
			assertServiceMetricsPrefixes(t, Distributor, distributor)
			assertServiceMetricsPrefixes(t, Ingester, ingester)
			assertServiceMetricsPrefixes(t, Querier, querier)
			assertServiceMetricsPrefixes(t, StoreGateway, storeGateway1)
			assertServiceMetricsPrefixes(t, StoreGateway, storeGateway2)
		})
	}
}

func TestQuerierWithBlocksStorageRunningInSingleBinaryMode(t *testing.T) {
	tests := map[string]struct {
		indexCacheBackend    string
		queryShardingEnabled bool
	}{
		"inmemory index cache": {
			indexCacheBackend: tsdb.IndexCacheBackendInMemory,
		},
		"memcached index cache": {
			indexCacheBackend: tsdb.IndexCacheBackendMemcached,
		},
		"inmemory index cache, query sharding enabled": {
			indexCacheBackend:    tsdb.IndexCacheBackendInMemory,
			queryShardingEnabled: true,
		},
	}

	for testName, testCfg := range tests {
		t.Run(testName, func(t *testing.T) {
			const blockRangePeriod = 5 * time.Second

			s, err := e2e.NewScenario(networkName)
			require.NoError(t, err)
			defer s.Close()

			// Start dependencies.
			consul := e2edb.NewConsul()
			minio := e2edb.NewMinio(9000, blocksBucketName)
			memcached := e2ecache.NewMemcached()
			require.NoError(t, s.StartAndWaitReady(consul, minio, memcached))

			// Setting the replication factor equal to the number of Mimir replicas
			// make sure each replica creates the same blocks, so the total number of
			// blocks is stable and easy to assert on.
			const seriesReplicationFactor = 2

			// Configure the blocks storage to frequently compact TSDB head
			// and ship blocks to the storage.
			flags := mergeFlags(BlocksStorageFlags(), BlocksStorageS3Flags(), map[string]string{
				"-blocks-storage.tsdb.block-ranges-period":                     blockRangePeriod.String(),
				"-blocks-storage.tsdb.ship-interval":                           "1s",
				"-blocks-storage.bucket-store.sync-interval":                   "1s",
				"-blocks-storage.tsdb.retention-period":                        ((blockRangePeriod * 2) - 1).String(),
				"-blocks-storage.bucket-store.index-cache.backend":             testCfg.indexCacheBackend,
				"-blocks-storage.bucket-store.index-cache.memcached.addresses": "dns+" + memcached.NetworkEndpoint(e2ecache.MemcachedPort),

				// Ingester.
				"-ingester.ring.store":                     "consul",
				"-ingester.ring.consul.hostname":           consul.NetworkHTTPEndpoint(),
				"-ingester.partition-ring.store":           "consul",
				"-ingester.partition-ring.consul.hostname": consul.NetworkHTTPEndpoint(),
				// Distributor.
				"-ingester.ring.replication-factor": strconv.FormatInt(seriesReplicationFactor, 10),
				"-distributor.ring.store":           "consul",
				"-distributor.ring.consul.hostname": consul.NetworkHTTPEndpoint(),
				// Store-gateway.
				"-store-gateway.sharding-ring.store":              "consul",
				"-store-gateway.sharding-ring.consul.hostname":    consul.NetworkHTTPEndpoint(),
				"-store-gateway.sharding-ring.replication-factor": "1",
				"-compactor.ring.store":                           "consul",
				"-compactor.ring.consul.hostname":                 consul.NetworkHTTPEndpoint(),
				"-compactor.cleanup-interval":                     "2s", // Update bucket index often.
				// Query-frontend.
				"-query-frontend.parallelize-shardable-queries": strconv.FormatBool(testCfg.queryShardingEnabled),
			})

			// Start Mimir replicas.
			mimir1 := e2emimir.NewSingleBinary("mimir-1", e2e.MergeFlags(DefaultSingleBinaryFlags(), flags))
			mimir2 := e2emimir.NewSingleBinary("mimir-2", e2e.MergeFlags(DefaultSingleBinaryFlags(), flags))
			cluster := e2emimir.NewCompositeMimirService(mimir1, mimir2)
			require.NoError(t, s.StartAndWaitReady(mimir1, mimir2))

			// Wait until Mimir replicas have updated the ring state.
			for _, replica := range cluster.Instances() {
				numTokensPerInstance := 512     // Ingesters ring.
				numTokensPerInstance++          // Distributors ring
				numTokensPerInstance += 512     // Compactor ring.
				numTokensPerInstance += 512 * 2 // Store-gateway ring (read both by the querier and store-gateway).

				require.NoError(t, replica.WaitSumMetrics(e2e.Equals(float64(numTokensPerInstance*cluster.NumInstances())), "cortex_ring_tokens_total"))
			}

			c, err := e2emimir.NewClient(mimir1.HTTPEndpoint(), mimir2.HTTPEndpoint(), "", "", "user-1")
			require.NoError(t, err)

			// Push some series to Mimir.
			series1Name := "series_1"
			series2Name := "series_2"
			series1Timestamp := time.Now()
			series2Timestamp := series1Timestamp.Add(blockRangePeriod * 2)
			series1, expectedVector1, _ := generateFloatSeries(series1Name, series1Timestamp, prompb.Label{Name: series1Name, Value: series1Name})
			series2, expectedVector2, _ := generateHistogramSeries(series2Name, series2Timestamp, prompb.Label{Name: series2Name, Value: series2Name})

			res, err := c.Push(series1)
			require.NoError(t, err)
			require.Equal(t, 200, res.StatusCode)

			res, err = c.Push(series2)
			require.NoError(t, err)
			require.Equal(t, 200, res.StatusCode)

			// Wait until the TSDB head is compacted and shipped to the storage.
			// The shipped block contains the 1st series, while the 2nd series is in the head.
			require.NoError(t, cluster.WaitSumMetrics(e2e.Equals(float64(1*cluster.NumInstances())), "cortex_ingester_shipper_uploads_total"))
			require.NoError(t, cluster.WaitSumMetrics(e2e.Equals(float64(1*cluster.NumInstances())), "cortex_ingester_memory_series"))
			require.NoError(t, cluster.WaitSumMetrics(e2e.Equals(float64(2*cluster.NumInstances())), "cortex_ingester_memory_series_created_total"))
			require.NoError(t, cluster.WaitSumMetrics(e2e.Equals(float64(1*cluster.NumInstances())), "cortex_ingester_memory_series_removed_total"))

			// Push another series to further compact another block and delete the first block
			// due to expired retention.
			series3Name := "series_3"
			series3Timestamp := series2Timestamp.Add(blockRangePeriod * 2)
			series3, expectedVector3, _ := generateFloatSeries(series3Name, series3Timestamp, prompb.Label{Name: series3Name, Value: series3Name})

			res, err = c.Push(series3)
			require.NoError(t, err)
			require.Equal(t, 200, res.StatusCode)

			require.NoError(t, cluster.WaitSumMetrics(e2e.Equals(float64(2*cluster.NumInstances())), "cortex_ingester_shipper_uploads_total"))
			require.NoError(t, cluster.WaitSumMetrics(e2e.Equals(float64(1*cluster.NumInstances())), "cortex_ingester_memory_series"))
			require.NoError(t, cluster.WaitSumMetrics(e2e.Equals(float64(3*cluster.NumInstances())), "cortex_ingester_memory_series_created_total"))
			require.NoError(t, cluster.WaitSumMetrics(e2e.Equals(float64(2*cluster.NumInstances())), "cortex_ingester_memory_series_removed_total"))

			// Wait until the store-gateway has synched the new uploaded blocks. The number of blocks loaded
			// may be greater than expected if the compactor is running (there may have been compacted).
			const shippedBlocks = 2
			require.NoError(t, cluster.WaitSumMetrics(e2e.GreaterOrEqual(float64(shippedBlocks*seriesReplicationFactor)), "cortex_bucket_store_blocks_loaded"))

			var expectedCacheRequests int
			var expectedCacheHits int
			var expectedMemcachedOps int

			// Query back the series (1 only in the storage, 1 only in the ingesters, 1 on both).
			// For every series in a block we expect
			// - 1 cache request for expanded postings
			// - 1 cache request for postings for each matcher (provided matchers are simple MatchEQ matchers)
			// - 1 cache request for each series
			//
			// If the series does not exist in the block, then we return early after checking expanded postings and
			// subsequently the index on disk.
			result, err := c.Query(series1Name, series1Timestamp)
			require.NoError(t, err)
			require.Equal(t, model.ValVector, result.Type())
			assert.Equal(t, expectedVector1, result.(model.Vector))
			expectedCacheRequests += seriesReplicationFactor * 3                                  // expanded postings, postings, series
			expectedMemcachedOps += (seriesReplicationFactor * 3) + (seriesReplicationFactor * 3) // Same reasoning as for expectedCacheRequests, but this also includes a set for each get that is not a hit

			result, err = c.Query(series2Name, series2Timestamp)
			require.NoError(t, err)
			require.Equal(t, model.ValVector, result.Type())
			assert.Equal(t, expectedVector2, result.(model.Vector))
			expectedCacheRequests += seriesReplicationFactor*3 + seriesReplicationFactor      // expanded postings, postings, series for 1 time range; only expanded postings for another
			expectedMemcachedOps += 2 * (seriesReplicationFactor*3 + seriesReplicationFactor) // Same reasoning as for expectedCacheRequests, but this also includes a set for each get that is not a hit

			result, err = c.Query(series3Name, series3Timestamp)
			require.NoError(t, err)
			require.Equal(t, model.ValVector, result.Type())
			assert.Equal(t, expectedVector3, result.(model.Vector))
			expectedCacheRequests += seriesReplicationFactor * 2                          // expanded postings for 2 time ranges
			expectedMemcachedOps += seriesReplicationFactor*2 + seriesReplicationFactor*2 // Same reasoning as for expectedCacheRequests, but this also includes a set for each get that is not a hit

			// Check the in-memory index cache metrics (in the store-gateway).
			require.NoError(t, cluster.WaitSumMetrics(e2e.Equals(float64(expectedCacheRequests)), "thanos_store_index_cache_requests_total"), "expected %v requests", expectedCacheRequests)
			require.NoError(t, cluster.WaitSumMetrics(e2e.Equals(float64(expectedCacheHits)), "thanos_store_index_cache_hits_total"), "expected %v hits", expectedCacheHits)
			if testCfg.indexCacheBackend == tsdb.IndexCacheBackendInMemory {
				require.NoError(t, cluster.WaitSumMetrics(e2e.Equals(float64((2*2+2+3)*seriesReplicationFactor)), "thanos_store_index_cache_items"))             // 2 series both for postings and series cache, 2 expanded postings on one block, 3 on another one
				require.NoError(t, cluster.WaitSumMetrics(e2e.Equals(float64((2*2+2+3)*seriesReplicationFactor)), "thanos_store_index_cache_items_added_total")) // 2 series both for postings and series cache, 2 expanded postings on one block, 3 on another one
			} else if testCfg.indexCacheBackend == tsdb.IndexCacheBackendMemcached {
				require.NoError(t, cluster.WaitSumMetrics(e2e.Equals(float64(expectedMemcachedOps)), "thanos_cache_operations_total"), "expected %v operations", expectedMemcachedOps)
			}

			// Query back again the 1st series from storage. This time it should use the index cache.
			// It should get a hit on expanded postings; this means that it will not request individual postings for matchers.
			// It should get a hit on series.
			// We expect 2 cache requests and 2 cache hits.
			result, err = c.Query(series1Name, series1Timestamp)
			require.NoError(t, err)
			require.Equal(t, model.ValVector, result.Type())
			assert.Equal(t, expectedVector1, result.(model.Vector))
			expectedCacheRequests += seriesReplicationFactor * 2 // expanded postings and series
			expectedCacheHits += seriesReplicationFactor * 2
			expectedMemcachedOps += seriesReplicationFactor * 2 // there is no set after the gets this time

			require.NoError(t, cluster.WaitSumMetrics(e2e.Equals(float64(expectedCacheRequests)), "thanos_store_index_cache_requests_total"), "expected %v requests", expectedCacheRequests)
			require.NoError(t, cluster.WaitSumMetrics(e2e.Equals(float64(expectedCacheHits)), "thanos_store_index_cache_hits_total"), "expected %v hits", expectedCacheHits) // this time has used the index cache

			if testCfg.indexCacheBackend == tsdb.IndexCacheBackendInMemory {
				require.NoError(t, cluster.WaitSumMetrics(e2e.Equals(float64((2*2+2+3)*seriesReplicationFactor)), "thanos_store_index_cache_items"))             // as before
				require.NoError(t, cluster.WaitSumMetrics(e2e.Equals(float64((2*2+2+3)*seriesReplicationFactor)), "thanos_store_index_cache_items_added_total")) // as before
			} else if testCfg.indexCacheBackend == tsdb.IndexCacheBackendMemcached {
				require.NoError(t, cluster.WaitSumMetrics(e2e.Equals(float64(expectedMemcachedOps)), "thanos_cache_operations_total"), "expected %v operations", expectedMemcachedOps)
			}

			// Query metadata.
			testMetadataQueriesWithBlocksStorage(t, c, series1[0], series2[0], series3[0], blockRangePeriod)
		})
	}
}

func TestMimirPromQLEngine(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	flags := mergeFlags(BlocksStorageFlags(), BlocksStorageS3Flags(), map[string]string{
		"-querier.query-engine": "mimir",
	})

	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags)
	ingester := e2emimir.NewIngester("ingester", consul.NetworkHTTPEndpoint(), flags)
	querier := e2emimir.NewQuerier("querier", consul.NetworkHTTPEndpoint(), flags)
	require.NoError(t, s.StartAndWaitReady(distributor, ingester, querier))

	// Wait until the distributor and querier have updated the ring.
	// The distributor should have 512 tokens for the ingester ring and 1 for the distributor ring,
	// and the querier should have 512 tokens for the ingester ring.
	require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(512+1), "cortex_ring_tokens_total"))
	require.NoError(t, querier.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))

	// Push a series to Mimir.
	writeClient, err := e2emimir.NewClient(distributor.HTTPEndpoint(), "", "", "", "user-1")
	require.NoError(t, err)

	seriesName := "series_1"
	seriesTimestamp := time.Now()
	series, expectedVector, _ := generateFloatSeries(seriesName, seriesTimestamp, prompb.Label{Name: seriesName, Value: seriesName})

	res, err := writeClient.Push(series)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	// Query back the same series using the streaming PromQL engine.
	c, err := e2emimir.NewClient("", querier.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	result, err := c.Query(seriesName, seriesTimestamp)
	require.NoError(t, err)
	require.Equal(t, model.ValVector, result.Type())
	assert.Equal(t, expectedVector, result.(model.Vector))
}

func testMetadataQueriesWithBlocksStorage(
	t *testing.T,
	c *e2emimir.Client,
	lastSeriesInStorage prompb.TimeSeries,
	lastSeriesInIngesterBlocks prompb.TimeSeries,
	firstSeriesInIngesterHead prompb.TimeSeries,
	blockRangePeriod time.Duration,
) {
	// This is hacky, don't use for anything serious
	timeStampFromSeries := func(ts prompb.TimeSeries) time.Time {
		if len(ts.Samples) > 0 {
			return util.TimeFromMillis(ts.Samples[0].Timestamp)
		}
		if len(ts.Histograms) > 0 {
			return util.TimeFromMillis(ts.Histograms[0].Timestamp)
		}
		panic("No samples or histograms")
	}

	var (
		lastSeriesInIngesterBlocksName = getMetricName(lastSeriesInIngesterBlocks.Labels)
		firstSeriesInIngesterHeadName  = getMetricName(firstSeriesInIngesterHead.Labels)
		lastSeriesInStorageName        = getMetricName(lastSeriesInStorage.Labels)

		lastSeriesInStorageTs        = timeStampFromSeries(lastSeriesInStorage)
		lastSeriesInIngesterBlocksTs = timeStampFromSeries(lastSeriesInIngesterBlocks)
		firstSeriesInIngesterHeadTs  = timeStampFromSeries(firstSeriesInIngesterHead)
	)

	type seriesTest struct {
		lookup string
		ok     bool
		resp   []prompb.Label
	}
	type labelValuesTest struct {
		matches []string
		resp    []string
	}

	testCases := map[string]struct {
		from time.Time
		to   time.Time

		seriesTests []seriesTest

		labelValuesTests []labelValuesTest

		labelNames []string
	}{
		"query metadata entirely inside the head range": {
			from: firstSeriesInIngesterHeadTs,
			to:   firstSeriesInIngesterHeadTs.Add(blockRangePeriod),
			seriesTests: []seriesTest{
				{
					lookup: firstSeriesInIngesterHeadName,
					ok:     true,
					resp:   firstSeriesInIngesterHead.Labels,
				},
				{
					lookup: lastSeriesInIngesterBlocksName,
					ok:     false,
				},
				{
					lookup: lastSeriesInStorageName,
					ok:     false,
				},
			},
			labelValuesTests: []labelValuesTest{
				{
					resp: []string{firstSeriesInIngesterHeadName},
				},
				{
					resp:    []string{firstSeriesInIngesterHeadName},
					matches: []string{firstSeriesInIngesterHeadName},
				},
				{
					resp:    []string{},
					matches: []string{lastSeriesInStorageName},
				},
			},
			labelNames: []string{labels.MetricName, firstSeriesInIngesterHeadName},
		},
		"query metadata entirely inside the ingester range but outside the head range": {
			from: lastSeriesInIngesterBlocksTs,
			to:   lastSeriesInIngesterBlocksTs.Add(blockRangePeriod / 2),
			seriesTests: []seriesTest{
				{
					lookup: firstSeriesInIngesterHeadName,
					ok:     false,
				},
				{
					lookup: lastSeriesInIngesterBlocksName,
					ok:     true,
					resp:   lastSeriesInIngesterBlocks.Labels,
				},
				{
					lookup: lastSeriesInStorageName,
					ok:     false,
				},
			},
			labelValuesTests: []labelValuesTest{
				{
					resp: []string{lastSeriesInIngesterBlocksName},
				},
				{
					resp:    []string{lastSeriesInIngesterBlocksName},
					matches: []string{lastSeriesInIngesterBlocksName},
				},
				{
					resp:    []string{},
					matches: []string{firstSeriesInIngesterHeadName},
				},
			},
			labelNames: []string{labels.MetricName, lastSeriesInIngesterBlocksName},
		},
		"query metadata partially inside the ingester range": {
			from: lastSeriesInStorageTs.Add(-blockRangePeriod),
			to:   firstSeriesInIngesterHeadTs.Add(blockRangePeriod),
			seriesTests: []seriesTest{
				{
					lookup: firstSeriesInIngesterHeadName,
					ok:     true,
					resp:   firstSeriesInIngesterHead.Labels,
				},
				{
					lookup: lastSeriesInIngesterBlocksName,
					ok:     true,
					resp:   lastSeriesInIngesterBlocks.Labels,
				},
				{
					lookup: lastSeriesInStorageName,
					ok:     true,
					resp:   lastSeriesInStorage.Labels,
				},
			},
			labelValuesTests: []labelValuesTest{
				{
					resp: []string{lastSeriesInStorageName, lastSeriesInIngesterBlocksName, firstSeriesInIngesterHeadName},
				},
				{
					resp:    []string{lastSeriesInStorageName},
					matches: []string{lastSeriesInStorageName},
				},
				{
					resp:    []string{lastSeriesInIngesterBlocksName},
					matches: []string{lastSeriesInIngesterBlocksName},
				},
				{
					resp:    []string{lastSeriesInStorageName, lastSeriesInIngesterBlocksName},
					matches: []string{lastSeriesInStorageName, lastSeriesInIngesterBlocksName},
				},
			},
			labelNames: []string{labels.MetricName, lastSeriesInStorageName, lastSeriesInIngesterBlocksName, firstSeriesInIngesterHeadName},
		},
		"query metadata entirely outside the ingester range should not return the head data": {
			from: lastSeriesInStorageTs.Add(-2 * blockRangePeriod),
			to:   lastSeriesInStorageTs,
			seriesTests: []seriesTest{
				{
					lookup: firstSeriesInIngesterHeadName,
					ok:     false,
					resp:   firstSeriesInIngesterHead.Labels,
				},
				{
					lookup: lastSeriesInIngesterBlocksName,
					ok:     false,
				},
				{
					lookup: lastSeriesInStorageName,
					ok:     true,
					resp:   lastSeriesInStorage.Labels,
				},
			},
			labelValuesTests: []labelValuesTest{
				{
					resp: []string{lastSeriesInStorageName},
				},
				{
					resp:    []string{lastSeriesInStorageName},
					matches: []string{lastSeriesInStorageName},
				},
				{
					resp:    []string{},
					matches: []string{firstSeriesInIngesterHeadName},
				},
			},
			labelNames: []string{labels.MetricName, lastSeriesInStorageName},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			for _, st := range tc.seriesTests {
				seriesRes, err := c.Series([]string{st.lookup}, tc.from, tc.to)
				require.NoError(t, err)
				if st.ok {
					require.Equal(t, 1, len(seriesRes), st)
					require.Equal(t, model.LabelSet(prompbLabelsToMetric(st.resp)), seriesRes[0], st)
				} else {
					require.Equal(t, 0, len(seriesRes), st)
				}
			}

			for _, lvt := range tc.labelValuesTests {
				labelsRes, err := c.LabelValues(labels.MetricName, tc.from, tc.to, lvt.matches)
				require.NoError(t, err)
				exp := model.LabelValues{}
				for _, val := range lvt.resp {
					exp = append(exp, model.LabelValue(val))
				}
				require.Equal(t, exp, labelsRes, lvt)
			}

			labelNames, err := c.LabelNames(tc.from, tc.to, nil)
			require.NoError(t, err)
			require.Equal(t, tc.labelNames, labelNames)
		})
	}
}

func TestQuerierWithBlocksStorageOnMissingBlocksFromStorage(t *testing.T) {
	const blockRangePeriod = 5 * time.Second

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Configure the blocks storage to frequently compact TSDB head
	// and ship blocks to the storage.
	flags := mergeFlags(BlocksStorageFlags(), BlocksStorageS3Flags(), map[string]string{
		"-blocks-storage.tsdb.block-ranges-period": blockRangePeriod.String(),
		"-blocks-storage.tsdb.ship-interval":       "1s",
		"-blocks-storage.tsdb.retention-period":    ((blockRangePeriod * 2) - 1).String(),
	})

	// Start dependencies.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	// Start Mimir components for the write path.
	distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags)
	ingester := e2emimir.NewIngester("ingester", consul.NetworkHTTPEndpoint(), flags)
	require.NoError(t, s.StartAndWaitReady(distributor, ingester))

	// Wait until the distributor has updated the ring.
	// The distributor should have 512 tokens for the ingester ring and 1 for the distributor ring
	require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(512+1), "cortex_ring_tokens_total"))

	// Push some series to Mimir.
	c, err := e2emimir.NewClient(distributor.HTTPEndpoint(), "", "", "", "user-1")
	require.NoError(t, err)

	series1Name := "series_1"
	series2Name := "series_2"
	series1Timestamp := time.Now()
	series2Timestamp := series1Timestamp.Add(blockRangePeriod * 2)
	series1, expectedVector1, _ := generateFloatSeries(series1Name, series1Timestamp)
	series2, expectedVector2, _ := generateHistogramSeries(series2Name, series2Timestamp)

	res, err := c.Push(series1)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	res, err = c.Push(series2)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	// Wait until the TSDB head is compacted and shipped to the storage.
	// The shipped block contains the 1st series, while the 2nd series in in the head.
	require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(1), "cortex_ingester_shipper_uploads_total"))
	require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(2), "cortex_ingester_memory_series_created_total"))
	require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(1), "cortex_ingester_memory_series_removed_total"))
	require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(1), "cortex_ingester_memory_series"))

	// Start the compactor to have the bucket index created before querying.
	compactor := e2emimir.NewCompactor("compactor", consul.NetworkHTTPEndpoint(), flags)
	require.NoError(t, s.StartAndWaitReady(compactor))

	// Start the querier and store-gateway, and configure them to frequently sync blocks fast enough to trigger consistency check
	// We will induce an error in the store gateway by deleting blocks and the querier ignores the direct error
	// and relies on checking that all blocks were queried. However this consistency check will skip most recent
	// blocks (less than 3*sync-interval age) as they could be unnoticed by the store-gateway and ingesters
	// have them anyway. We turn down the sync-interval to speed up the test.
	storeGateway := e2emimir.NewStoreGateway("store-gateway", consul.NetworkHTTPEndpoint(), mergeFlags(flags, map[string]string{
		"-blocks-storage.bucket-store.sync-interval": "1s",
	}))
	querier := e2emimir.NewQuerier("querier", consul.NetworkHTTPEndpoint(), mergeFlags(flags, map[string]string{
		"-blocks-storage.bucket-store.sync-interval": "1s",
	}))
	require.NoError(t, s.StartAndWaitReady(querier, storeGateway))

	// Wait until the querier and store-gateway have updated the ring
	require.NoError(t, querier.WaitSumMetrics(e2e.Equals(512*2), "cortex_ring_tokens_total"))
	require.NoError(t, storeGateway.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))

	// Wait until the blocks are old enough for consistency check
	// 1 sync on startup, 3 to go over the consistency check limit explained above
	require.NoError(t, storeGateway.WaitSumMetrics(e2e.GreaterOrEqual(1+3), "cortex_blocks_meta_syncs_total"))

	// Query back the series.
	c, err = e2emimir.NewClient("", querier.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	result, err := c.Query(series1Name, series1Timestamp)
	require.NoError(t, err)
	require.Equal(t, model.ValVector, result.Type())
	assert.Equal(t, expectedVector1, result.(model.Vector))

	result, err = c.Query(series2Name, series2Timestamp)
	require.NoError(t, err)
	require.Equal(t, model.ValVector, result.Type())
	assert.Equal(t, expectedVector2, result.(model.Vector))

	// Delete all blocks from the storage.
	storage, err := e2emimir.NewS3ClientForMinio(minio, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, err)
	require.NoError(t, storage.DeleteBlocks("user-1"))

	// Query back again the series. Now we do expect a 500 error because the blocks are
	// missing from the storage.
	_, err = c.Query(series1Name, series1Timestamp)
	require.Error(t, err)
	var apiErr *v1.Error
	require.ErrorAs(t, err, &apiErr)
	assert.Contains(t, apiErr.Detail, "get range reader: The specified key does not exist")

	// We expect this to still be queryable as it was not in the cleared storage
	_, err = c.Query(series2Name, series2Timestamp)
	require.NoError(t, err)
	require.Equal(t, model.ValVector, result.Type())
	assert.Equal(t, expectedVector2, result.(model.Vector))
}

func TestQueryLimitsWithBlocksStorageRunningInMicroServices(t *testing.T) {
	const blockRangePeriod = 5 * time.Second

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Configure the blocks storage to frequently compact TSDB head
	// and ship blocks to the storage.
	flags := mergeFlags(BlocksStorageFlags(), BlocksStorageS3Flags(), map[string]string{
		"-blocks-storage.tsdb.block-ranges-period":   blockRangePeriod.String(),
		"-blocks-storage.tsdb.ship-interval":         "1s",
		"-blocks-storage.bucket-store.sync-interval": "1s",
		"-blocks-storage.tsdb.retention-period":      ((blockRangePeriod * 2) - 1).String(),
		"-querier.max-fetched-series-per-query":      "3",
	})

	// Start dependencies.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	memcached := e2ecache.NewMemcached()
	require.NoError(t, s.StartAndWaitReady(consul, minio, memcached))

	// Add the memcached address to the flags.
	flags["-blocks-storage.bucket-store.index-cache.memcached.addresses"] = "dns+" + memcached.NetworkEndpoint(e2ecache.MemcachedPort)

	// Start Mimir components.
	distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags)
	ingester := e2emimir.NewIngester("ingester", consul.NetworkHTTPEndpoint(), flags)
	storeGateway := e2emimir.NewStoreGateway("store-gateway", consul.NetworkHTTPEndpoint(), flags)
	require.NoError(t, s.StartAndWaitReady(distributor, ingester, storeGateway))

	querier := e2emimir.NewQuerier("querier", consul.NetworkHTTPEndpoint(), flags)
	require.NoError(t, s.StartAndWaitReady(querier))

	c, err := e2emimir.NewClient(distributor.HTTPEndpoint(), querier.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	// Push some series to Mimir.
	series1Name := "series_1"
	series2Name := "series_2"
	series3Name := "series_3"
	series4Name := "series_4"
	series1Timestamp := time.Now()
	series2Timestamp := series1Timestamp.Add(blockRangePeriod * 2)
	series3Timestamp := series1Timestamp.Add(blockRangePeriod * 2)
	series4Timestamp := series1Timestamp.Add(blockRangePeriod * 3)

	series1, _, _ := generateFloatSeries(series1Name, series1Timestamp, prompb.Label{Name: series1Name, Value: series1Name})
	series2, _, _ := generateHistogramSeries(series2Name, series2Timestamp, prompb.Label{Name: series2Name, Value: series2Name})
	series3, _, _ := generateFloatSeries(series3Name, series3Timestamp, prompb.Label{Name: series3Name, Value: series3Name})
	series4, _, _ := generateHistogramSeries(series4Name, series4Timestamp, prompb.Label{Name: series4Name, Value: series4Name})

	res, err := c.Push(series1)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)
	res, err = c.Push(series2)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	result, err := c.QueryRange("{__name__=~\"series_.+\"}", series1Timestamp, series2Timestamp.Add(1*time.Hour), blockRangePeriod)
	require.NoError(t, err)
	require.Equal(t, model.ValMatrix, result.Type())

	res, err = c.Push(series3)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)
	res, err = c.Push(series4)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	_, err = c.QueryRange("{__name__=~\"series_.+\"}", series1Timestamp, series4Timestamp.Add(1*time.Hour), blockRangePeriod)
	require.Error(t, err)
	assert.ErrorContains(t, err, "the query exceeded the maximum number of series")
}

func TestHashCollisionHandling(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	flags := mergeFlags(
		BlocksStorageFlags(),
		BlocksStorageS3Flags(),
	)

	// Start dependencies.
	minio := e2edb.NewMinio(9000, blocksBucketName)

	consul := e2edb.NewConsul()
	require.NoError(t, s.StartAndWaitReady(minio, consul))

	// Start Mimir components for the write path.
	distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags)
	ingester := e2emimir.NewIngester("ingester", consul.NetworkHTTPEndpoint(), flags)
	require.NoError(t, s.StartAndWaitReady(distributor, ingester))

	// Wait until the distributor has updated the ring.
	// The distributor should have 512 tokens for the ingester ring and 1 for the distributor ring
	require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(512+1), "cortex_ring_tokens_total"))

	// Push a series for each user to Mimir.
	now := time.Now()

	c, err := e2emimir.NewClient(distributor.HTTPEndpoint(), "", "", "", "user-0")
	require.NoError(t, err)

	var series []prompb.TimeSeries
	var expectedVector model.Vector
	// Generate two series which collide on fingerprints and fast fingerprints.
	tsMillis := e2e.TimeToMilliseconds(now)
	metric1 := []prompb.Label{
		{Name: "A", Value: "K6sjsNNczPl"},
		{Name: labels.MetricName, Value: "fingerprint_collision"},
	}
	metric2 := []prompb.Label{
		{Name: "A", Value: "cswpLMIZpwt"},
		{Name: labels.MetricName, Value: "fingerprint_collision"},
	}

	series = append(series, prompb.TimeSeries{
		Labels: metric1,
		Samples: []prompb.Sample{
			{Value: float64(0), Timestamp: tsMillis},
		},
	})
	expectedVector = append(expectedVector, &model.Sample{
		Metric:    prompbLabelsToMetric(metric1),
		Value:     model.SampleValue(float64(0)),
		Timestamp: model.Time(tsMillis),
	})
	series = append(series, prompb.TimeSeries{
		Labels: metric2,
		Samples: []prompb.Sample{
			{Value: float64(1), Timestamp: tsMillis},
		},
	})
	expectedVector = append(expectedVector, &model.Sample{
		Metric:    prompbLabelsToMetric(metric2),
		Value:     model.SampleValue(float64(1)),
		Timestamp: model.Time(tsMillis),
	})

	res, err := c.Push(series)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	querier := e2emimir.NewQuerier("querier", consul.NetworkHTTPEndpoint(), flags)
	require.NoError(t, s.StartAndWaitReady(querier))

	// Wait until the querier has updated the ring.
	require.NoError(t, querier.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))

	// Query the series.
	c, err = e2emimir.NewClient("", querier.HTTPEndpoint(), "", "", "user-0")
	require.NoError(t, err)

	result, err := c.Query("fingerprint_collision", now)
	require.NoError(t, err)
	require.Equal(t, model.ValVector, result.Type())
	require.Equal(t, expectedVector, result.(model.Vector))
}

func getMetricName(lbls []prompb.Label) string {
	for _, lbl := range lbls {
		if lbl.Name == labels.MetricName {
			return lbl.Value
		}
	}

	panic(fmt.Sprintf("series %v has no metric name", lbls))
}
