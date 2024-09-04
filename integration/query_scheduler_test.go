// SPDX-License-Identifier: AGPL-3.0-only
//go:build requires_docker

package integration

import (
	"sort"
	"testing"
	"time"

	"github.com/grafana/e2e"
	e2edb "github.com/grafana/e2e/db"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/integration/e2emimir"
)

func TestQuerySchedulerWithMaxUsedInstances(t *testing.T) {
	runTestQuerySchedulerWithMaxUsedInstances(t, "series_1", generateFloatSeries)
}

func TestQuerySchedulerWithMaxUsedInstancesHistogram(t *testing.T) {
	runTestQuerySchedulerWithMaxUsedInstances(t, "hseries_1", generateHistogramSeries)
}

func runTestQuerySchedulerWithMaxUsedInstances(t *testing.T, seriesName string, genSeries generateSeriesFunc) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	flags := mergeFlags(
		BlocksStorageFlags(),
		BlocksStorageS3Flags(),
		map[string]string{
			"-query-scheduler.service-discovery-mode": "ring",
			"-query-scheduler.ring.store":             "consul",
			"-query-scheduler.max-used-instances":     "1",
			"-querier.max-concurrent":                 "8",
		},
	)

	// Start dependencies.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	flags["-query-scheduler.ring.consul.hostname"] = consul.NetworkHTTPEndpoint()

	// Start 2 query-scheduler. We override the address registered in the ring so that we can easily predict it
	// when computing the expected in-use query-scheduler instance.
	queryScheduler1 := e2emimir.NewQueryScheduler("query-scheduler-1", mergeFlags(flags, map[string]string{"-query-scheduler.ring.instance-addr": e2e.NetworkContainerHost(s.NetworkName(), "query-scheduler-1")}))
	queryScheduler2 := e2emimir.NewQueryScheduler("query-scheduler-2", mergeFlags(flags, map[string]string{"-query-scheduler.ring.instance-addr": e2e.NetworkContainerHost(s.NetworkName(), "query-scheduler-2")}))
	require.NoError(t, s.StartAndWaitReady(queryScheduler1, queryScheduler2))

	// Start all other Mimir services.
	queryFrontend := e2emimir.NewQueryFrontend("query-frontend", consul.NetworkHTTPEndpoint(), flags)
	ingester := e2emimir.NewIngester("ingester", consul.NetworkHTTPEndpoint(), flags)
	distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags)
	querier := e2emimir.NewQuerier("querier", consul.NetworkHTTPEndpoint(), flags)
	require.NoError(t, s.StartAndWaitReady(queryFrontend, querier, ingester, distributor))

	// Wait until distributor and querier have updated the ingesters ring.
	for _, service := range []*e2emimir.MimirService{distributor, querier} {
		require.NoError(t, service.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
			labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
			labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))),
			"Service: %s", service.Name())
	}

	// Wait until query-frontend and querier have updated the query-schedulers ring.
	require.NoError(t, queryFrontend.WaitSumMetricsWithOptions(e2e.Equals(2), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "query-frontend-query-scheduler-client"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	require.NoError(t, querier.WaitSumMetricsWithOptions(e2e.Equals(2), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "querier-query-scheduler-client"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	// Compute which is the expected in-use query-scheduler.
	schedulers := []*e2emimir.MimirService{queryScheduler1, queryScheduler2}
	sort.Slice(schedulers, func(i, j int) bool { return schedulers[i].NetworkGRPCEndpoint() < schedulers[j].NetworkGRPCEndpoint() })
	inUseScheduler := schedulers[0]
	notInUseScheduler := schedulers[1]

	// The minimum number of connections per scheduler is 4 in order to avoid queue starvation
	// when the RequestQueue utilizes the querier-worker queue prioritization algorithm.
	// Although the max-concurrent is set to 8, the querier will create an extra 4 connections
	// per not-in-use scheduler to meet the minimum requirements per connected RequestQueue instance.
	require.NoError(t, inUseScheduler.WaitSumMetricsWithOptions(e2e.Equals(8), []string{"cortex_query_scheduler_connected_querier_clients"}))
	require.NoError(t, notInUseScheduler.WaitSumMetricsWithOptions(e2e.Equals(4), []string{"cortex_query_scheduler_connected_querier_clients"}))

	// We expect the query-frontend to only open connections to the in-use scheduler.
	require.NoError(t, inUseScheduler.WaitSumMetricsWithOptions(e2e.Greater(0), []string{"cortex_query_scheduler_connected_frontend_clients"}))
	require.NoError(t, notInUseScheduler.WaitSumMetricsWithOptions(e2e.Equals(0), []string{"cortex_query_scheduler_connected_frontend_clients"}))

	// Push some series to Mimir.
	now := time.Now()
	series, expectedVector, _ := genSeries(seriesName, now, prompb.Label{Name: "foo", Value: "bar"})

	c, err := e2emimir.NewClient(distributor.HTTPEndpoint(), querier.HTTPEndpoint(), "", "", userID)
	require.NoError(t, err)

	res, err := c.Push(series)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	// Query the series.
	result, err := c.Query(seriesName, now)
	require.NoError(t, err)
	require.Equal(t, model.ValVector, result.Type())
	assert.Equal(t, expectedVector, result.(model.Vector))

	// Terminate the in-use query-scheduler.
	require.NoError(t, s.Stop(inUseScheduler))

	// We expect the querier to transfer all connections up to the configured max to the previously not-in-use scheduler.
	require.NoError(t, notInUseScheduler.WaitSumMetricsWithOptions(e2e.Equals(8), []string{"cortex_query_scheduler_connected_querier_clients"}))

	// We expect the query-frontend to open connections to the previously not-in-use scheduler.
	require.NoError(t, notInUseScheduler.WaitSumMetricsWithOptions(e2e.Greater(0), []string{"cortex_query_scheduler_connected_frontend_clients"}))

	// Query the series.
	result, err = c.Query(seriesName, now)
	require.NoError(t, err)
	require.Equal(t, model.ValVector, result.Type())
	assert.Equal(t, expectedVector, result.(model.Vector))
}
