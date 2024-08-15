// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/integration/querier_tenant_federation_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.
//go:build requires_docker

package integration

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/grafana/e2e"
	e2ecache "github.com/grafana/e2e/cache"
	e2edb "github.com/grafana/e2e/db"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/integration/e2emimir"
)

type querierTenantFederationConfig struct {
	querySchedulerEnabled  bool
	shuffleShardingEnabled bool
}

func TestQuerierTenantFederation(t *testing.T) {
	runQuerierTenantFederationTest(t, querierTenantFederationConfig{})
}

func TestQuerierTenantFederationWithQueryScheduler(t *testing.T) {
	runQuerierTenantFederationTest(t, querierTenantFederationConfig{
		querySchedulerEnabled: true,
	})
}

func TestQuerierTenantFederationWithShuffleSharding(t *testing.T) {
	runQuerierTenantFederationTest(t, querierTenantFederationConfig{
		shuffleShardingEnabled: true,
	})
}

func TestQuerierTenantFederationWithQuerySchedulerAndShuffleSharding(t *testing.T) {
	runQuerierTenantFederationTest(t, querierTenantFederationConfig{
		querySchedulerEnabled:  true,
		shuffleShardingEnabled: true,
	})
}

func runQuerierTenantFederationTest(t *testing.T, cfg querierTenantFederationConfig) {
	const numUsers = 10

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
		"-tenant-federation.enabled":                        "true",
		"-ingester.max-global-exemplars-per-user":           "10000",
	})

	// Start the query-scheduler if enabled.
	var queryScheduler *e2emimir.MimirService
	if cfg.querySchedulerEnabled {
		queryScheduler = e2emimir.NewQueryScheduler("query-scheduler", flags)
		require.NoError(t, s.StartAndWaitReady(queryScheduler))
		flags["-query-frontend.scheduler-address"] = queryScheduler.NetworkGRPCEndpoint()
		flags["-querier.scheduler-address"] = queryScheduler.NetworkGRPCEndpoint()
	}

	if cfg.shuffleShardingEnabled {
		// Use only single querier for each user.
		flags["-query-frontend.max-queriers-per-tenant"] = "1"
	}

	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(minio))

	// Start the query-frontend.
	queryFrontend := e2emimir.NewQueryFrontend("query-frontend", consul.NetworkHTTPEndpoint(), flags)
	require.NoError(t, s.Start(queryFrontend))

	if !cfg.querySchedulerEnabled {
		flags["-querier.frontend-address"] = queryFrontend.NetworkGRPCEndpoint()
	}

	// Start all other services.
	ingester := e2emimir.NewIngester("ingester", consul.NetworkHTTPEndpoint(), flags)
	distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags)
	querier := e2emimir.NewQuerier("querier", consul.NetworkHTTPEndpoint(), flags)

	var querier2 *e2emimir.MimirService
	if cfg.shuffleShardingEnabled {
		querier2 = e2emimir.NewQuerier("querier-2", consul.NetworkHTTPEndpoint(), flags)
	}

	require.NoError(t, s.StartAndWaitReady(querier, ingester, distributor))
	require.NoError(t, s.WaitReady(queryFrontend))
	if cfg.shuffleShardingEnabled {
		require.NoError(t, s.StartAndWaitReady(querier2))
	}

	// Wait until distributor and queriers have updated the ring.
	// The distributor should have 512 tokens for the ingester ring and 1 for the distributor ring
	require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(512+1), "cortex_ring_tokens_total"))
	require.NoError(t, querier.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))
	if cfg.shuffleShardingEnabled {
		require.NoError(t, querier2.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))
	}

	// Push a series for each user to Mimir.
	now := time.Now()
	expectedVectors := make([]model.Vector, numUsers)
	tenantIDs := make([]string, numUsers)

	for u := 0; u < numUsers; u++ {
		tenantIDs[u] = fmt.Sprintf("user-%d", u)
		c, err := e2emimir.NewClient(distributor.HTTPEndpoint(), "", "", "", tenantIDs[u])
		require.NoError(t, err)

		var series []prompb.TimeSeries
		series, expectedVectors[u], _ = generateAlternatingSeries(u)("series_1", now)

		res, err := c.Push(series)
		require.NoError(t, err)
		require.Equal(t, 200, res.StatusCode)
	}

	// query all tenants
	c, err := e2emimir.NewClient(distributor.HTTPEndpoint(), queryFrontend.HTTPEndpoint(), "", "", strings.Join(tenantIDs, "|"))
	require.NoError(t, err)

	result, err := c.Query("series_1", now)
	require.NoError(t, err)

	assert.ElementsMatch(t, mergeResults(tenantIDs, expectedVectors), result.(model.Vector))

	// query exemplars for all tenants
	exemplars, err := c.QueryExemplars("series_1", now.Add(-1*time.Hour), now.Add(1*time.Hour))
	require.NoError(t, err)
	assert.Len(t, exemplars, numUsers)

	// ensure a push to multiple tenants is failing
	series, _, _ := generateFloatSeries("series_1", now)
	res, err := c.Push(series)
	require.NoError(t, err)
	require.Equal(t, 500, res.StatusCode)

	series, _, _ = generateHistogramSeries("series_1", now)
	res, err = c.Push(series)
	require.NoError(t, err)
	require.Equal(t, 500, res.StatusCode)

	// check metric label values for total queries in the query frontend
	require.NoError(t, queryFrontend.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_query_frontend_queries_total"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "user", strings.Join(tenantIDs, "|")),
		labels.MustNewMatcher(labels.MatchEqual, "op", "query"))))

	require.NoError(t, queryFrontend.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_query_frontend_queries_total"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "user", strings.Join(tenantIDs, "|")),
		labels.MustNewMatcher(labels.MatchEqual, "op", "other"))))

	// check metric label values for query queue length in either query frontend or query scheduler
	queueComponent := queryFrontend
	queueMetricName := "cortex_query_frontend_queue_length"
	if cfg.querySchedulerEnabled {
		queueComponent = queryScheduler
		queueMetricName = "cortex_query_scheduler_queue_length"
	}
	require.NoError(t, queueComponent.WaitSumMetricsWithOptions(e2e.Equals(0), []string{queueMetricName}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "user", strings.Join(tenantIDs, "|")))))

	// TODO: check cache invalidation on tombstone cache gen increase
	// TODO: check fairness in queryfrontend
}

func mergeResults(tenantIDs []string, resultsPerTenant []model.Vector) model.Vector {
	var v model.Vector
	for pos, tenantID := range tenantIDs {
		for _, r := range resultsPerTenant[pos] {
			s := *r
			s.Metric = r.Metric.Clone()
			s.Metric[model.LabelName("__tenant_id__")] = model.LabelValue(tenantID)
			v = append(v, &s)
		}
	}
	return v
}
