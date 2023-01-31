// SPDX-License-Identifier: AGPL-3.0-only
//go:build requires_docker
// +build requires_docker

package integration

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/grafana/e2e"
	e2edb "github.com/grafana/e2e/db"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/integration/e2emimir"
	"github.com/grafana/mimir/pkg/storegateway"
	"github.com/grafana/mimir/pkg/util/globalerror"
)

const (
	blockRangePeriod = 5 * time.Second
)

func Test_MaxSeriesAndChunksPerQueryLimitHit(t *testing.T) {
	setup := func(t *testing.T, additionalStoreGatewayFlags, additionalQuerierFlags map[string]string) (scenario *e2e.Scenario, ingester, storeGateway *e2emimir.MimirService, client *e2emimir.Client) {
		scenario, err := e2e.NewScenario(networkName)
		require.NoError(t, err)

		flags := mergeFlags(
			BlocksStorageFlags(),
			BlocksStorageS3Flags(),
			map[string]string{
				"-ingester.ring.replication-factor": "1",

				// Frequently compact and ship blocks to storage so we can query them through the store gateway.
				"-blocks-storage.tsdb.block-ranges-period":          blockRangePeriod.String(),
				"-blocks-storage.tsdb.head-compaction-idle-timeout": "1s",
				"-blocks-storage.tsdb.retention-period":             ((2 * blockRangePeriod) - 1).String(),
				"-blocks-storage.tsdb.ship-interval":                "1s",
			},
		)

		consul := e2edb.NewConsul()
		minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
		require.NoError(t, scenario.StartAndWaitReady(consul, minio))

		// Start Mimir components.
		distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags)

		ingester = e2emimir.NewIngester("ingester", consul.NetworkHTTPEndpoint(), flags)

		additionalStoreGatewayFlags = mergeFlags(flags, additionalStoreGatewayFlags)
		storeGateway = e2emimir.NewStoreGateway("store-gateway", consul.NetworkHTTPEndpoint(), additionalStoreGatewayFlags)

		require.NoError(t, scenario.StartAndWaitReady(distributor, ingester, storeGateway))

		additionalQuerierFlags = mergeFlags(flags, additionalQuerierFlags)
		querier := e2emimir.NewQuerier("querier", consul.NetworkHTTPEndpoint(), additionalQuerierFlags)

		require.NoError(t, scenario.StartAndWaitReady(querier))

		client, err = e2emimir.NewClient(distributor.HTTPEndpoint(), querier.HTTPEndpoint(), "", "", "test")
		require.NoError(t, err)
		return
	}

	tests := map[string]struct {
		additionalStoreGatewayFlags map[string]string
		additionalQuerierFlags      map[string]string
		expectedErrorKey            string
	}{
		"when store-gateway hits max_fetched_series_per_query, 'exceeded series limit' is returned": {
			additionalStoreGatewayFlags: map[string]string{"-querier.max-fetched-series-per-query": "3"},
			expectedErrorKey:            storegateway.ErrSeriesLimitMessage,
		},
		"when querier hits max_fetched_series_per_query, 'err-mimir-max-series-per-query' is returned": {
			additionalQuerierFlags: map[string]string{"-querier.max-fetched-series-per-query": "3"},
			expectedErrorKey:       string(globalerror.MaxSeriesPerQuery),
		},
		"when store-gateway hits max_fetched_chunks_per_query, 'exceeded chunks limit' is returned": {
			additionalStoreGatewayFlags: map[string]string{"-querier.max-fetched-chunks-per-query": "3"},
			expectedErrorKey:            storegateway.ErrChunksLimitMessage,
		},
		"when querier hits max_fetched_chunks_per_query, 'err-mimir-max-chunks-per-query' is returned": {
			additionalQuerierFlags: map[string]string{"-querier.max-fetched-chunks-per-query": "4"},
			expectedErrorKey:       string(globalerror.MaxChunksPerQuery),
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			scenario, ingester, storeGateway, client := setup(t, testData.additionalStoreGatewayFlags, testData.additionalQuerierFlags)
			defer scenario.Close()

			timeStamp1 := time.Now()
			timeStamp2 := timeStamp1
			timeStamp3 := timeStamp2.Add(2 * blockRangePeriod)
			timeStamp4 := timeStamp3
			timeSeries1, _, _ := generateSeries("series_1", timeStamp1, prompb.Label{Name: "series_1", Value: "series_1"})
			timeSeries2, _, _ := generateSeries("series_2", timeStamp2, prompb.Label{Name: "series_2", Value: "series_2"})
			timeSeries3, _, _ := generateSeries("series_3", timeStamp3, prompb.Label{Name: "series_3", Value: "series_3"})
			timeSeries4, _, _ := generateSeries("series_4", timeStamp4, prompb.Label{Name: "series_4", Value: "series_4"})

			pushTimeSeries(t, client, timeSeries1)
			pushTimeSeries(t, client, timeSeries2)

			// ensures that data will be queried from store-gateway, and not from ingester: at this stage 1 block of data should be stored
			waitUntilShippedToStorage(t, ingester, storeGateway, 1)

			// Verify we can successfully read the data we have just pushed
			rangeResultResponse, _, err := client.QueryRangeRaw("{__name__=~\"series_.+\"}", timeStamp1, timeStamp2.Add(1*time.Hour), time.Second)
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, rangeResultResponse.StatusCode)

			pushTimeSeries(t, client, timeSeries3)
			pushTimeSeries(t, client, timeSeries4)

			// ensures that data will be queried from store-gateway, and not from ingester: at this stage 2 blocks of data should be stored
			waitUntilShippedToStorage(t, ingester, storeGateway, 2)

			// Verify we cannot read the data we just pushed because the limit is hit, and the status code 422 is returned
			rangeResultResponse, rangeResultBody, err := client.QueryRangeRaw("{__name__=~\"series_.+\"}", timeStamp1, timeStamp4.Add(1*time.Hour), time.Second)
			require.NoError(t, err)
			require.Equal(t, http.StatusUnprocessableEntity, rangeResultResponse.StatusCode)
			require.True(t, strings.Contains(string(rangeResultBody), testData.expectedErrorKey))
		})
	}
}

func pushTimeSeries(t *testing.T, client *e2emimir.Client, timeSeries []prompb.TimeSeries) {
	res, err := client.Push(timeSeries)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)
}

func waitUntilShippedToStorage(t *testing.T, ingester, storeGateway *e2emimir.MimirService, blockCount float64) {
	// Wait until the TSDB head is shipped to storage, removed from the ingester, and loaded by the
	// store-gateway to ensure we're querying the store-gateway.
	require.NoError(t, ingester.WaitSumMetrics(e2e.GreaterOrEqual(blockCount), "cortex_ingester_shipper_uploads_total"))
	require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(0), "cortex_ingester_memory_series"))
	require.NoError(t, storeGateway.WaitSumMetrics(e2e.GreaterOrEqual(blockCount), "cortex_bucket_store_blocks_loaded"))
}
