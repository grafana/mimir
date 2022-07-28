// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/integration/querier_sharding_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.
//go:build requires_docker
// +build requires_docker

package integration

import (
	"fmt"
	"math/rand"
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
	const numQueries = 50

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
		"-query-frontend.query-sharding-total-shards":       "16",
		"-ingester.out-of-order-time-window":                "10m",
		"-blocks-storage.tsdb.head-compaction-interval":     "5s",
	})

	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(minio))

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
	require.NoError(t, querier1.WaitSumMetrics(e2e.Equals(5*512), "cortex_ring_tokens_total"))
	require.NoError(t, querier2.WaitSumMetrics(e2e.Equals(5*512), "cortex_ring_tokens_total"))

	// Push a series for each user to Mimir.
	now := time.Now()

	distClient, err := e2emimir.NewClient(distributor.HTTPEndpoint(), "", "", "", userID)
	require.NoError(t, err)

	startTime := now.Add(-12 * time.Hour)
	startTime = time.Unix(7200*(startTime.Unix()/7200), 0).Add(-2 * time.Hour) // Align to 2h.
	now = time.Unix(7200*(now.Unix()/7200), 0).Add(-time.Hour)                 // Align to the odd hours.

	numSeries := 10
	series := make([]prompb.TimeSeries, numSeries)
	for i := 0; i < numSeries; i++ {
		series[i] = prompb.TimeSeries{
			Labels: []prompb.Label{
				{Name: "__name__", Value: "cortex_test_total"},
				{Name: "id", Value: fmt.Sprintf("%d", i)},
			},
		}
	}
	fmt.Println("Ingestion started")
	sampleValue := float64(0)
	for ts := startTime; now.Sub(ts) > 0; ts = ts.Add(time.Minute) {
		tsMillis, val := e2e.TimeToMilliseconds(ts), sampleValue
		for i := 0; i < numSeries; i++ {
			series[i].Samples = []prompb.Sample{
				{Value: val, Timestamp: tsMillis},
				{Value: val + 10, Timestamp: tsMillis + (15 * time.Second.Milliseconds())},
				{Value: val + 20, Timestamp: tsMillis + (30 * time.Second.Milliseconds())},
				{Value: val + 30, Timestamp: tsMillis + (45 * time.Second.Milliseconds())},
			}
			if ts.Add(45 * time.Second).After(now) {
				now = ts.Add(45 * time.Second)
			}
		}
		sampleValue += 40
		res, err := distClient.Push(series)
		require.NoError(t, err)
		require.Equal(t, 200, res.StatusCode)

		if tsMillis%(2*time.Minute.Milliseconds()) == 0 {
			// Add a OOO sample every 2 mins.
			sIdx := rand.Intn(numSeries)
			scrapesOld := rand.Intn(30) + 1
			oooSeries := []prompb.TimeSeries{
				{
					Labels: series[sIdx].Labels,
					Samples: []prompb.Sample{
						{
							Value:     val - float64(scrapesOld*10),
							Timestamp: tsMillis - (int64(scrapesOld) * 15 * time.Second.Milliseconds()),
						},
					},
				},
			}
			res, err := distClient.Push(oooSeries)
			require.NoError(t, err)
			require.Equal(t, 200, res.StatusCode)
		}

		if tsMillis%(2*time.Hour.Milliseconds()) == 0 {
			// Wait for a compaction every 2hr.
			fmt.Println("Waiting 10 seconds for compaction")
			<-time.After(10 * time.Second)
		}
	}

	startTime = startTime.Add(5 * time.Minute)

	wg := sync.WaitGroup{}
	fmt.Println("Ingestion done")
	// Run all queries concurrently to get better distribution of requests between queriers.
	for i := 0; i < numQueries; i++ {
		wg.Add(1)

		go func(idx int) {
			defer wg.Done()
			c, err := e2emimir.NewClient("", queryFrontend.HTTPEndpoint(), "", "", userID)
			require.NoError(t, err)

			result, err := c.QueryRange(fmt.Sprintf("sum(rate(cortex_test_total[5m])) + %d", idx), startTime, now, 30*time.Second)
			require.NoError(t, err)
			require.Equal(t, model.ValMatrix, result.Type())

			expMatrix := model.Matrix{
				&model.SampleStream{Metric: model.Metric{}},
			}
			for ts := startTime; now.Sub(ts) > 0; ts = ts.Add(30 * time.Second) {
				expMatrix[0].Values = append(expMatrix[0].Values,
					model.SamplePair{
						Timestamp: model.Time(e2e.TimeToMilliseconds(ts)),
						Value:     model.SampleValue(float64(idx) + (float64(numSeries) * float64(10) / float64(15))),
					},
				)
			}
			assert.Equal(t, expMatrix, result.(model.Matrix))
		}(i)
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
