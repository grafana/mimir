// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/integration/querier_sharding_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.
//go:build requires_docker
// +build requires_docker

package integration

import (
	"fmt"
	"strconv"
	"sync"
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

func TestQueryGap(t *testing.T) {
	// Going too high starts hitting file descriptor limit, since we run all queriers concurrently.
	const numQueries = 100

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	memcached := e2ecache.NewMemcached()
	consul := e2edb.NewConsul()
	require.NoError(t, s.StartAndWaitReady(consul, memcached))

	flags := mergeFlags(BlocksStorageFlags(), BlocksStorageS3Flags(), map[string]string{
		"-query-frontend.cache-results":                     "true",
		"-query-frontend.results-cache.backend":             "memcached",
		"-query-frontend.results-cache.memcached.addresses": "dns+" + memcached.NetworkEndpoint(e2ecache.MemcachedPort),
		"-query-frontend.results-cache.compression":         "snappy",
		"-querier.max-outstanding-requests-per-tenant":      strconv.Itoa(numQueries), // To avoid getting errors.
	})

	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(minio))

	// Use only single querier for each user.
	flags["-query-frontend.max-queriers-per-tenant"] = "1"
	// Enable out of order ingestion for 10 minutes.
	flags["-ingester.out-of-order-time-window"] = "10m"

	// Start the query-scheduler if enabled.
	var queryScheduler *e2emimir.MimirService
	queryScheduler = e2emimir.NewQueryScheduler("query-scheduler", flags)
	require.NoError(t, s.StartAndWaitReady(queryScheduler))
	flags["-query-frontend.scheduler-address"] = queryScheduler.NetworkGRPCEndpoint()
	flags["-querier.scheduler-address"] = queryScheduler.NetworkGRPCEndpoint()

	// Start the query-frontend.
	queryFrontend := e2emimir.NewQueryFrontend("query-frontend", flags)
	require.NoError(t, s.Start(queryFrontend))

	// Start all other services.
	ingester1 := e2emimir.NewIngester("ingester-1", consul.NetworkHTTPEndpoint(), flags)
	ingester2 := e2emimir.NewIngester("ingester-2", consul.NetworkHTTPEndpoint(), flags)
	ingester3 := e2emimir.NewIngester("ingester-3", consul.NetworkHTTPEndpoint(), flags)
	ingester4 := e2emimir.NewIngester("ingester-4", consul.NetworkHTTPEndpoint(), flags)
	ingester5 := e2emimir.NewIngester("ingester-5", consul.NetworkHTTPEndpoint(), flags)
	//ingesters := e2emimir.NewCompositeMimirService(ingester1, ingester2, ingester3, ingester4, ingester5)
	distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags)
	querier1 := e2emimir.NewQuerier("querier-1", consul.NetworkHTTPEndpoint(), flags)
	querier2 := e2emimir.NewQuerier("querier-2", consul.NetworkHTTPEndpoint(), flags)

	require.NoError(t, s.StartAndWaitReady(querier1, querier2, ingester1, ingester2, ingester3, ingester4, ingester5, distributor))
	require.NoError(t, s.WaitReady(queryFrontend))

	// Wait until distributor and queriers have updated the ring.
	// The distributor should have 5*512 tokens for the ingester ring and 1 for the distributor ring
	require.NoError(t, distributor.WaitSumMetrics(e2e.Equals((5*512)+1), "cortex_ring_tokens_total"))
	require.NoError(t, querier1.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))
	require.NoError(t, querier2.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))

	// Push a series for each user to Mimir.
	now := time.Now()

	distClient, err := e2emimir.NewClient(distributor.HTTPEndpoint(), "", "", "", userID)
	require.NoError(t, err)

	// TODO: Change this and add samples over 12h for >1 series.
	startTime := now.Add(-12 * time.Hour)
	startTime = time.Unix(60*(startTime.Unix()/60), 0) // Align to 1 minute.
	numSeries := 10
	series := make([]prompb.TimeSeries, numSeries)
	expMatrix := make(model.Matrix, numSeries)
	for i := 0; i < 10; i++ {
		series[i] = prompb.TimeSeries{
			Labels: []prompb.Label{
				{Name: "__name__", Value: "cortex_test_total"},
				{Name: "id", Value: fmt.Sprintf("%d", i)},
			},
		}
		expMatrix[i].Metric = model.Metric{
			"__name__": "cortex_test_total",
			"id":       model.LabelValue(fmt.Sprintf("%d", i)),
		}
	}
	sampleValue := float64(0)
	for ts := startTime; now.Sub(ts) > 0; ts = ts.Add(15 * time.Second) {
		for i := 0; i < 10; i++ {
			tsMillis, val := e2e.TimeToMilliseconds(ts), sampleValue
			series[i].Samples = []prompb.Sample{
				{Value: val, Timestamp: tsMillis},
			}
			expMatrix[i].Values = append(expMatrix[i].Values, model.SamplePair{
				Timestamp: model.Time(tsMillis),
				Value:     model.SampleValue(val),
			})
			res, err := distClient.Push(series)
			require.NoError(t, err)
			require.Equal(t, 200, res.StatusCode)
			sampleValue += 10
		}
	}

	//var series []prompb.TimeSeries
	//series, expectedVector := generateSeries("series_1", now)
	//res, err := distClient.Push(series)
	//require.NoError(t, err)
	//require.Equal(t, 200, res.StatusCode)

	wg := sync.WaitGroup{}

	// Run all queries concurrently to get better distribution of requests between queriers.
	for i := 0; i < numQueries; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			c, err := e2emimir.NewClient("", queryFrontend.HTTPEndpoint(), "", "", userID)
			require.NoError(t, err)

			// TODO: change this to query range and a sum(rate()) query.
			result, err := c.QueryRange("cortex_test_total", startTime, now, 15*time.Second)
			require.NoError(t, err)
			require.Equal(t, model.ValMatrix, result.Type())
			assert.Equal(t, expMatrix, result.(model.Matrix))
		}()
	}

	wg.Wait()

	require.NoError(t, queryFrontend.WaitSumMetrics(e2e.Equals(numQueries), "cortex_query_frontend_queries_total"))

	// Ensure no service-specific metrics prefix is used by the wrong service.
	assertServiceMetricsPrefixes(t, Distributor, distributor)
	assertServiceMetricsPrefixes(t, Ingester, ingester1)
	assertServiceMetricsPrefixes(t, Ingester, ingester2)
	assertServiceMetricsPrefixes(t, Ingester, ingester3)
	assertServiceMetricsPrefixes(t, Ingester, ingester4)
	assertServiceMetricsPrefixes(t, Ingester, ingester5)
	assertServiceMetricsPrefixes(t, Querier, querier1)
	assertServiceMetricsPrefixes(t, Querier, querier2)
	assertServiceMetricsPrefixes(t, QueryFrontend, queryFrontend)
	assertServiceMetricsPrefixes(t, QueryScheduler, queryScheduler)
}
