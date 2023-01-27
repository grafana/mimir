// SPDX-License-Identifier: AGPL-3.0-only
//go:build requires_docker
// +build requires_docker

package integration

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/gogo/status"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/grpcclient"
	"github.com/grafana/e2e"
	e2ecache "github.com/grafana/e2e/cache"
	e2edb "github.com/grafana/e2e/db"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"
	grpc_metadata "google.golang.org/grpc/metadata"

	"github.com/grafana/mimir/integration/e2emimir"
	"github.com/grafana/mimir/pkg/storegateway"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
)

const (
	blockRangePeriod = 5 * time.Second
	ingesterTag      = "ingester"
	distributorTag   = "distributor"
	storeGatewayTag  = "store-gateway"
	querierTag       = "querier"
	orgID            = "test"
	series1          = "series_1"
	series2          = "series_2"
	series3          = "series_3"
	series4          = "series_4"
)

func TestStoreGateway_StoreGatewayLimitHit(t *testing.T) {
	scenario, mimirServices, client := createClient(t, map[string]string{"-querier.max-fetched-series-per-query": "3"})
	defer scenario.Close()

	timeStamps, timeSeries := createTimeSeries(t, []string{series1, series2, series3, series4})

	pushTimeSeries(t, client, timeSeries[series1])
	pushTimeSeries(t, client, timeSeries[series2])
	pushTimeSeries(t, client, timeSeries[series3])
	pushTimeSeries(t, client, timeSeries[series4])

	// ensures that data will be queried from store-gateway, and not from ingester: at this stage 1 block of data should be stored
	waitUntilShippedToStorage(t, mimirServices, 1)

	cfg := grpcclient.Config{}
	flagext.DefaultValues(&cfg)
	reg := prometheus.NewPedanticRegistry()
	factory := storegateway.NewStoreGatewayClientFactory(cfg, reg)
	storeGatewayClient, err := factory(mimirServices[storeGatewayTag].GRPCEndpoint())
	require.NoError(t, err)

	ctx := prepareGRPCContext()
	req := &storepb.SeriesRequest{
		MinTime: timeStamps[series1].Unix() * 1000,
		MaxTime: timeStamps[series4].Add(1*time.Hour).Unix() * 1000,
		Matchers: []storepb.LabelMatcher{
			{Type: storepb.LabelMatcher_RE, Name: labels.MetricName, Value: "series_.+"},
		},
	}
	stream, err := storeGatewayClient.(*storegateway.ClientImpl).Series(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, stream)

	res, err := stream.Recv()
	require.Nil(t, res)
	require.NotNil(t, err)
	st, ok := status.FromError(errors.Cause(err))
	require.True(t, ok)
	require.Equal(t, uint32(http.StatusUnprocessableEntity), uint32(st.Code()))
}

func TestStoreGateway_QuerierLimitHit(t *testing.T) {
	scenario, mimirServices, client := createClient(t, map[string]string{"-querier.max-fetched-series-per-query": "3"})
	defer scenario.Close()

	timeStamps12, timeSeries12 := createTimeSeries(t, []string{series1, series2})
	pushTimeSeries(t, client, timeSeries12[series1])
	pushTimeSeries(t, client, timeSeries12[series2])

	// ensures that data will be queried from store-gateway, and not from ingester: at this stage 1 block of data should be stored
	waitUntilShippedToStorage(t, mimirServices, 1)

	// Verify we can successfully read the data we have just pushed
	rangeResultResponse, _, err := client.QueryRangeRaw("{__name__=~\"series_.+\"}", timeStamps12[series1], timeStamps12[series2].Add(1*time.Hour), time.Second)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, rangeResultResponse.StatusCode)

	timeStamps34, timeSeries34 := createTimeSeries(t, []string{series3, series4})
	pushTimeSeries(t, client, timeSeries34[series3])
	pushTimeSeries(t, client, timeSeries34[series4])

	// ensures that data will be queried from store-gateway, and not from ingester: at this stage 2 blocks of data should be stored
	waitUntilShippedToStorage(t, mimirServices, 2)

	// Verify we cannot read the data we just pushed because the limit is hit, and the status code 422 is returned
	rangeResultResponse, rangeResultBody, err := client.QueryRangeRaw("{__name__=~\"series_.+\"}", timeStamps12[series1], timeStamps34[series4].Add(1*time.Hour), time.Second)
	require.NoError(t, err)
	require.Equal(t, http.StatusUnprocessableEntity, rangeResultResponse.StatusCode)
	require.JSONEq(t, `{"status":"error","errorType":"execution","error":"expanding series: the query exceeded the maximum number of series (limit: 3 series) (err-mimir-max-series-per-query). To adjust the related per-tenant limit, configure -querier.max-fetched-series-per-query, or contact your service administrator."}`, string(rangeResultBody))
}

func prepareGRPCContext() context.Context {
	ctx := user.InjectOrgID(context.Background(), orgID)
	// We have to store userID in the incoming metadata because we have to emulate the
	// case it's coming from a gRPC request, while here we're running everything in-memory.
	return grpc_metadata.AppendToOutgoingContext(ctx, storegateway.GrpcContextMetadataTenantID, orgID)
}

func createTimeSeries(t *testing.T, tags []string) (map[string]time.Time, map[string][]prompb.TimeSeries) {
	timeStamps := make(map[string]time.Time)
	timeSeries := make(map[string][]prompb.TimeSeries)
	timeStamp := time.Now()

	for i, tag := range tags {
		timeStamp.Add(blockRangePeriod * time.Duration(i))
		timeStamps[tag] = timeStamp
		series, _, _ := generateSeries(tag, timeStamp, prompb.Label{Name: tag, Value: tag})
		timeSeries[tag] = series
	}

	return timeStamps, timeSeries
}

func pushTimeSeries(t *testing.T, client *e2emimir.Client, timeSeries []prompb.TimeSeries) {
	res, err := client.Push(timeSeries)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)
}

func waitUntilShippedToStorage(t *testing.T, mimirServices map[string]*e2emimir.MimirService, blockCount float64) {
	// Wait until the TSDB head is shipped to storage, removed from the ingester, and loaded by the
	// store-gateway to ensure we're querying the store-gateway.
	require.NoError(t, mimirServices[ingesterTag].WaitSumMetrics(e2e.GreaterOrEqual(blockCount), "cortex_ingester_shipper_uploads_total"))
	require.NoError(t, mimirServices[ingesterTag].WaitSumMetrics(e2e.Equals(0), "cortex_ingester_memory_series"))
	require.NoError(t, mimirServices[storeGatewayTag].WaitSumMetrics(e2e.GreaterOrEqual(blockCount), "cortex_bucket_store_blocks_loaded"))
}

func createClient(t *testing.T, otherFlags map[string]string) (*e2e.Scenario, map[string]*e2emimir.MimirService, *e2emimir.Client) {
	scenario, err := e2e.NewScenario(networkName)
	require.NoError(t, err)

	flags := mergeFlags(
		BlocksStorageFlags(),
		BlocksStorageS3Flags(),
		map[string]string{
			"-ingester.ring.replication-factor": "1",

			// Frequently compact and ship blocks to storage so we can query them through the store gateway.
			"-blocks-storage.tsdb.block-ranges-period":          "2s",
			"-blocks-storage.tsdb.ship-interval":                "1s",
			"-blocks-storage.tsdb.retention-period":             "3s",
			"-blocks-storage.tsdb.head-compaction-idle-timeout": "1s",
		},
		otherFlags,
	)

	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	memcached := e2ecache.NewMemcached()
	require.NoError(t, scenario.StartAndWaitReady(consul, minio, memcached))

	// Add the memcached address to the flags.
	flags["-blocks-storage.bucket-store.index-cache.memcached.addresses"] = "dns+" + memcached.NetworkEndpoint(e2ecache.MemcachedPort)

	mimirServices := make(map[string]*e2emimir.MimirService)
	// Start Mimir components.
	distributor := e2emimir.NewDistributor(distributorTag, consul.NetworkHTTPEndpoint(), flags)
	mimirServices[distributorTag] = distributor

	ingester := e2emimir.NewIngester(ingesterTag, consul.NetworkHTTPEndpoint(), flags)
	mimirServices[ingesterTag] = ingester

	storeGateway := e2emimir.NewStoreGateway(storeGatewayTag, consul.NetworkHTTPEndpoint(), flags, e2emimir.WithGRPCPortExposed())
	mimirServices[storeGatewayTag] = storeGateway

	require.NoError(t, scenario.StartAndWaitReady(distributor, ingester, storeGateway))

	querier := e2emimir.NewQuerier(querierTag, consul.NetworkHTTPEndpoint(), flags)
	mimirServices[querierTag] = querier

	require.NoError(t, scenario.StartAndWaitReady(querier))

	client, err := e2emimir.NewClient(distributor.HTTPEndpoint(), querier.HTTPEndpoint(), "", "", orgID)
	require.NoError(t, err)

	return scenario, mimirServices, client
}
