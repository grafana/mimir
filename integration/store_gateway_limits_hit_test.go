// SPDX-License-Identifier: AGPL-3.0-only
//go:build requires_docker
// +build requires_docker

package integration

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/grafana/e2e"
	e2edb "github.com/grafana/e2e/db"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/integration/e2emimir"
	"github.com/grafana/mimir/pkg/util/globalerror"
)

func Test_MaxSeriesAndChunksPerQueryLimitHit(t *testing.T) {
	const (
		blockRangePeriod  = 500 * time.Millisecond
		numSeriesPerBlock = 5
	)

	scenario, err := e2e.NewScenario(networkName)
	require.NoError(t, err)

	flags := mergeFlags(
		BlocksStorageFlags(),
		BlocksStorageS3Flags(),
		map[string]string{
			"-log.level":                        "debug",
			"-ingester.ring.replication-factor": "3",

			// Frequently compact and ship blocks to storage so we can query them through the store gateway.
			"-blocks-storage.bucket-store.sync-interval":        "1s",
			"-store-gateway.sharding-ring.replication-factor":   "2",
			"-blocks-storage.tsdb.block-ranges-period":          blockRangePeriod.String(),
			"-blocks-storage.tsdb.head-compaction-idle-timeout": "1s",
			"-blocks-storage.tsdb.retention-period":             blockRangePeriod.String(), // We want blocks to be immediately deleted from ingesters.
			"-blocks-storage.tsdb.ship-interval":                "1s",
			"-blocks-storage.tsdb.head-compaction-interval":     "500ms",
			"-compactor.first-level-compaction-wait-period":     "1m", // Do not compact aggressively
		},
	)

	querierFlags := map[string]string{
		// The querier uses a multiple of this period to allow the store-gateway to sync the bucket.
		// If the period is too low (like it is - 1s), it leads to flaky CI tests
		// because the store-gateway may delay syncing the bucket. A multiple of 1s doesn't give it enough leeway.
		// So we keep the store-gateway synching frequently, but lower the querier's expectations.
		"-blocks-storage.bucket-store.sync-interval": "5s",
	}

	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, scenario.StartAndWaitReady(consul, minio))

	// Start Mimir write path components.
	distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags)
	ingester1 := e2emimir.NewIngester("ingester-1", consul.NetworkHTTPEndpoint(), flags)
	ingester2 := e2emimir.NewIngester("ingester-2", consul.NetworkHTTPEndpoint(), flags)
	ingester3 := e2emimir.NewIngester("ingester-3", consul.NetworkHTTPEndpoint(), flags)
	require.NoError(t, scenario.StartAndWaitReady(distributor, ingester1, ingester2, ingester3))

	// Wait until distributor has discovered the ingester via the ring.
	require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(3), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	// Write 3 series. Wait until each series is shipped to the storage before pushing the next one,
	// to ensure each series is in a different block. The 3rd series is written only to trigger the
	// retention in the ingester and remove the first 2 blocks (containing the first 2 series).
	timeStamp1 := time.Now()
	timeStamp2 := timeStamp1.Add(blockRangePeriod * 3)
	timeStamp3 := timeStamp1.Add(blockRangePeriod * 5)
	client, err := e2emimir.NewClient(distributor.HTTPEndpoint(), "", "", "", "test")

	for i, ts := range []time.Time{timeStamp1, timeStamp2, timeStamp3} {
		t.Logf("Writing block %d at timestamp %v", i+1, ts)
		for j := 0; j < numSeriesPerBlock; j++ {
			series, _, _ := generateAlternatingSeries(i)(fmt.Sprintf("series_%d", j), ts)
			pushTimeSeries(t, client, series)
		}

		// Wait until the TSDB head is shipped to storage and removed from the ingester.
		// We assume that the other two ingesters are doing the same in lockstep.
		t.Logf("Waiting for block %d to be shipped to storage...", i+1)
		require.NoError(t, ingester1.WaitSumMetrics(e2e.Equals(float64(i+1)), "cortex_ingester_shipper_uploads_total"))
		require.NoError(t, ingester1.WaitSumMetrics(e2e.Equals(0), "cortex_ingester_memory_series"))
		t.Logf("Block %d shipped successfully", i+1)
	}

	tests := map[string]struct {
		additionalStoreGatewayFlags map[string]string
		additionalQuerierFlags      map[string]string
		expectedErrorKey            string
	}{
		"when store-gateway hits max_fetched_series_per_query, 'err-mimir-max-series-per-query' is returned": {
			additionalStoreGatewayFlags: map[string]string{"-querier.max-fetched-series-per-query": "3"},
			expectedErrorKey:            string(globalerror.MaxSeriesPerQuery),
		},
		"when querier hits max_fetched_series_per_query, 'err-mimir-max-series-per-query' is returned": {
			additionalQuerierFlags: map[string]string{"-querier.max-fetched-series-per-query": "3"},
			expectedErrorKey:       string(globalerror.MaxSeriesPerQuery),
		},
		"when querier hits max_fetched_series_per_query querying only the store-gateway, 'err-mimir-max-series-per-query' is returned": {
			additionalQuerierFlags: map[string]string{
				"-querier.query-ingesters-within":       "1ms",
				"-querier.max-fetched-series-per-query": "3",
			},
			expectedErrorKey: string(globalerror.MaxSeriesPerQuery),
		},
		"when querier hits max_fetched_series_per_query querying only the ingester, 'err-mimir-max-series-per-query' is returned": {
			additionalQuerierFlags: map[string]string{
				"-querier.query-store-after":            "24h",
				"-querier.query-ingesters-within":       "25h",
				"-querier.max-fetched-series-per-query": "3",
			},
			expectedErrorKey: string(globalerror.MaxSeriesPerQuery),
		},
		"when store-gateway hits max_fetched_chunks_per_query, 'err-mimir-max-chunks-per-query' is returned": {
			additionalStoreGatewayFlags: map[string]string{"-querier.max-fetched-chunks-per-query": "3"},
			expectedErrorKey:            string(globalerror.MaxChunksPerQuery),
		},
		"when querier hits max_fetched_chunks_per_query, 'err-mimir-max-chunks-per-query' is returned": {
			additionalQuerierFlags: map[string]string{"-querier.max-fetched-chunks-per-query": "3"},
			expectedErrorKey:       string(globalerror.MaxChunksPerQuery),
		},
		"when querier hits max_fetched_chunks_per_query querying only the store-gateway, 'err-mimir-max-chunks-per-query' is returned": {
			additionalQuerierFlags: map[string]string{
				"-querier.query-ingesters-within":       "1ms",
				"-querier.max-fetched-chunks-per-query": "3",
			},
			expectedErrorKey: string(globalerror.MaxChunksPerQuery),
		},
		"when querier hits max_fetched_chunks_per_query querying only the ingester, 'err-mimir-max-chunks-per-query' is returned": {
			additionalQuerierFlags: map[string]string{
				"-querier.query-store-after":            "24h",
				"-querier.query-ingesters-within":       "25h",
				"-querier.max-fetched-chunks-per-query": "3",
			},
			expectedErrorKey: string(globalerror.MaxChunksPerQuery),
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			// Start Mimir read components and wait until ready.
			// Compactor needs to start before store-gateway so that the bucket index is updated.
			compactor := e2emimir.NewCompactor("compactor", consul.NetworkHTTPEndpoint(), flags)
			require.NoError(t, scenario.StartAndWaitReady(compactor))

			querierFlags := mergeFlags(flags, querierFlags, testData.additionalQuerierFlags)

			// The querier and store-gateway will be ready after they discovered the blocks in the storage.
			t.Logf("Starting querier with flags: %v", querierFlags)
			t.Logf("Starting store-gateways with flags: %v", testData.additionalStoreGatewayFlags)

			querier := e2emimir.NewQuerier("querier", consul.NetworkHTTPEndpoint(), mergeFlags(flags, querierFlags))
			storeGateway1 := e2emimir.NewStoreGateway("store-gateway-1", consul.NetworkHTTPEndpoint(), mergeFlags(flags, testData.additionalStoreGatewayFlags))
			storeGateway2 := e2emimir.NewStoreGateway("store-gateway-2", consul.NetworkHTTPEndpoint(), mergeFlags(flags, testData.additionalStoreGatewayFlags))
			require.NoError(t, scenario.StartAndWaitReady(querier, storeGateway1, storeGateway2))

			// Log component readiness
			t.Logf("Components started and ready: querier, store-gateway-1, store-gateway-2")
			t.Cleanup(func() {
				require.NoError(t, scenario.Stop(querier, storeGateway1, storeGateway2, compactor))
			})

			client, err = e2emimir.NewClient(distributor.HTTPEndpoint(), querier.HTTPEndpoint(), "", "", "test")
			require.NoError(t, err)

			// Log query parameters for debugging
			t.Logf("Running query with params: query={__name__=~\"series_.+\"}, start=%v, end=%v, expectedErrorKey=%s",
				timeStamp1.Add(-time.Second), timeStamp3.Add(time.Second), testData.expectedErrorKey)

			// Wait for store-gateways to have loaded blocks
			require.NoError(t, storeGateway1.WaitSumMetricsWithOptions(e2e.GreaterOrEqual(1), []string{"cortex_bucket_store_blocks_loaded"}, e2e.WaitMissingMetrics))
			require.NoError(t, storeGateway2.WaitSumMetricsWithOptions(e2e.GreaterOrEqual(1), []string{"cortex_bucket_store_blocks_loaded"}, e2e.WaitMissingMetrics))

			// Verify we cannot successfully query timeseries because of the series/chunks limit.
			rangeResultResponse, rangeResultBody, err := client.QueryRangeRaw("{__name__=~\"series_.+\"}", timeStamp1.Add(-time.Second), timeStamp3.Add(time.Second), time.Second)
			require.NoError(t, err)

			// Add detailed logging if assertion is about to fail
			if rangeResultResponse.StatusCode != http.StatusUnprocessableEntity {
				t.Logf("UNEXPECTED STATUS CODE: got %d, expected %d (422)", rangeResultResponse.StatusCode, http.StatusUnprocessableEntity)
				t.Logf("Response body: %s", string(rangeResultBody))
				t.Logf("Response headers: %+v", rangeResultResponse.Header)
			}
			require.Equal(t, http.StatusUnprocessableEntity, rangeResultResponse.StatusCode,
				"Expected 422 but got %d. Body: %s", rangeResultResponse.StatusCode, string(rangeResultBody))

			// Add detailed logging if error key assertion is about to fail
			if !strings.Contains(string(rangeResultBody), testData.expectedErrorKey) {
				t.Logf("ERROR KEY NOT FOUND: expected '%s' in response body", testData.expectedErrorKey)
				t.Logf("Full response body: %s", string(rangeResultBody))
			}
			require.True(t, strings.Contains(string(rangeResultBody), testData.expectedErrorKey),
				"Expected error key '%s' not found in response body: %s", testData.expectedErrorKey, string(rangeResultBody))
		})
	}
}

func pushTimeSeries(t *testing.T, client *e2emimir.Client, timeSeries []prompb.TimeSeries) {
	res, err := client.Push(timeSeries)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)
}
