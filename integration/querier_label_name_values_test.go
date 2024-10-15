// SPDX-License-Identifier: AGPL-3.0-only
//go:build requires_docker

package integration

import (
	"fmt"
	"net/http"
	"sort"
	"testing"
	"time"

	"github.com/grafana/dskit/test"
	"github.com/grafana/e2e"
	e2ecache "github.com/grafana/e2e/cache"
	e2edb "github.com/grafana/e2e/db"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/integration/e2emimir"
	"github.com/grafana/mimir/pkg/querier/api"
)

// Define cardinality 'env' and 'job' label sets.
var cardinalityEnvLabelValues = []string{"prod", "staging", "dev"}
var cardinalityJobLabelValues = []string{"distributor", "ingester", "store-gateway", "querier", "compactor"}

func TestQuerierLabelNamesAndValues(t *testing.T) {
	const numSeriesToPush = 1000

	// Test cases
	tests := map[string]struct {
		selector       string
		limit          int
		expectedResult api.LabelNamesCardinalityResponse
	}{
		"obtain label names and values with default selector and limit": {
			expectedResult: api.LabelNamesCardinalityResponse{
				LabelValuesCountTotal: 1008,
				LabelNamesCount:       3,
				Cardinality: []*api.LabelNamesCardinalityItem{
					{LabelName: labels.MetricName, LabelValuesCount: 1000},
					{LabelName: "job", LabelValuesCount: 5},
					{LabelName: "env", LabelValuesCount: 3},
				},
			},
		},
		"apply request selector": {
			selector: "{job=~'store-.*'}",
			expectedResult: api.LabelNamesCardinalityResponse{
				LabelValuesCountTotal: 204,
				LabelNamesCount:       3,
				Cardinality: []*api.LabelNamesCardinalityItem{
					{LabelName: labels.MetricName, LabelValuesCount: 200},
					{LabelName: "env", LabelValuesCount: 3},
					{LabelName: "job", LabelValuesCount: 1},
				},
			},
		},
		"limit cardinality response elements": {
			limit: 1,
			expectedResult: api.LabelNamesCardinalityResponse{
				LabelValuesCountTotal: 1008,
				LabelNamesCount:       3,
				Cardinality: []*api.LabelNamesCardinalityItem{
					{LabelName: labels.MetricName, LabelValuesCount: 1000},
				},
			},
		},
		"apply request selector and limit": {
			selector: "{job=~'store-.*'}",
			limit:    1,
			expectedResult: api.LabelNamesCardinalityResponse{
				LabelValuesCountTotal: 204,
				LabelNamesCount:       3,
				Cardinality: []*api.LabelNamesCardinalityItem{
					{LabelName: labels.MetricName, LabelValuesCount: 200},
				},
			},
		},
	}

	// Run tests.
	for tn, tc := range tests {
		t.Run(tn, func(t *testing.T) {
			s, err := e2e.NewScenario(networkName)
			require.NoError(t, err)
			defer s.Close()

			// Start dependencies.
			memcached := e2ecache.NewMemcached()
			consul := e2edb.NewConsul()
			require.NoError(t, s.StartAndWaitReady(consul, memcached))

			// Set configuration.
			flags := mergeFlags(BlocksStorageFlags(), BlocksStorageS3Flags(), map[string]string{
				"-querier.cardinality-analysis-enabled": "true",
				"-ingester.ring.replication-factor":     "3",

				// Enable the cardinality results cache with a very short TTL just to exercise its code.
				"-query-frontend.cache-results":                           "true",
				"-query-frontend.results-cache.backend":                   "memcached",
				"-query-frontend.results-cache.memcached.addresses":       "dns+" + memcached.NetworkEndpoint(e2ecache.MemcachedPort),
				"-query-frontend.results-cache-ttl-for-cardinality-query": "1ms",
			})

			// Start minio.
			minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
			require.NoError(t, s.StartAndWaitReady(minio))

			// Start the query-frontend.
			queryFrontend := e2emimir.NewQueryFrontend("query-frontend", consul.NetworkHTTPEndpoint(), flags)
			require.NoError(t, s.Start(queryFrontend))
			flags["-querier.frontend-address"] = queryFrontend.NetworkGRPCEndpoint()

			// Start all other Mimir services.
			distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags)
			ingester1 := e2emimir.NewIngester("ingester-1", consul.NetworkHTTPEndpoint(), flags)
			ingester2 := e2emimir.NewIngester("ingester-2", consul.NetworkHTTPEndpoint(), flags)
			ingester3 := e2emimir.NewIngester("ingester-3", consul.NetworkHTTPEndpoint(), flags)
			querier := e2emimir.NewQuerier("querier", consul.NetworkHTTPEndpoint(), flags)

			require.NoError(t, s.StartAndWaitReady(distributor, ingester1, ingester2, ingester3, querier))
			require.NoError(t, s.WaitReady(queryFrontend))

			// Wait until distributor and queriers have updated the ring.
			require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(3), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
				labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
				labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

			require.NoError(t, querier.WaitSumMetricsWithOptions(e2e.Equals(3), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
				labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
				labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

			// Push series.
			now := time.Now()

			client, err := e2emimir.NewClient(distributor.HTTPEndpoint(), queryFrontend.HTTPEndpoint(), "", "", userID)
			require.NoError(t, err)

			for i := 1; i <= numSeriesToPush; i++ {
				metricName := fmt.Sprintf("series_%d", i)
				series, _, _ := generateAlternatingSeries(i)(metricName, now,
					prompb.Label{Name: "env", Value: cardinalityEnvLabelValues[i%len(cardinalityEnvLabelValues)]},
					prompb.Label{Name: "job", Value: cardinalityJobLabelValues[i%len(cardinalityJobLabelValues)]},
				)

				res, err := client.Push(series)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, res.StatusCode)
			}

			// Since the Push() response is sent as soon as the quorum is reached, when we reach this point
			// the final ingester may not have received series yet.
			// To avoid flaky test we retry the assertions until we hit the desired state within a reasonable timeout.
			test.Poll(t, 3*time.Second, numSeriesToPush*3, func() interface{} {
				var totalIngestedSeries int
				for _, ing := range []*e2emimir.MimirService{ingester1, ingester2, ingester3} {
					values, err := ing.SumMetrics([]string{"cortex_ingester_memory_series"})
					require.NoError(t, err)

					numMemorySeries := e2e.SumValues(values)
					totalIngestedSeries += int(numMemorySeries)
				}
				return totalIngestedSeries
			})

			// Fetch label names and values.
			lbNamesAndValuesResp, err := client.LabelNamesAndValues(tc.selector, tc.limit)
			require.NoError(t, err)

			// Test results.
			require.Equal(t, tc.expectedResult, *lbNamesAndValuesResp)
		})
	}
}

func TestQuerierLabelValuesCardinality(t *testing.T) {
	const numSeriesToPush = 1000
	// Test cases
	tests := map[string]struct {
		labelNames     []string
		selector       string
		limit          int
		expectedResult api.LabelValuesCardinalityResponse
	}{
		"obtain labels cardinality with default selector and limit": {
			labelNames: []string{"env", "job"},
			expectedResult: api.LabelValuesCardinalityResponse{
				SeriesCountTotal: numSeriesToPush,
				Labels: []api.LabelNamesCardinality{
					{
						LabelName:        "env",
						LabelValuesCount: 3,
						SeriesCount:      1000,
						Cardinality: []api.LabelValuesCardinality{
							{LabelValue: "staging", SeriesCount: 334},
							{LabelValue: "dev", SeriesCount: 333},
							{LabelValue: "prod", SeriesCount: 333},
						},
					},
					{
						LabelName:        "job",
						LabelValuesCount: 5,
						SeriesCount:      1000,
						Cardinality: []api.LabelValuesCardinality{
							{LabelValue: "compactor", SeriesCount: 200},
							{LabelValue: "distributor", SeriesCount: 200},
							{LabelValue: "ingester", SeriesCount: 200},
							{LabelValue: "querier", SeriesCount: 200},
							{LabelValue: "store-gateway", SeriesCount: 200},
						},
					},
				},
			},
		},
		"obtain env label cardinality with default selector": {
			labelNames: []string{"env"},
			expectedResult: api.LabelValuesCardinalityResponse{
				SeriesCountTotal: numSeriesToPush,
				Labels: []api.LabelNamesCardinality{
					{
						LabelName:        "env",
						LabelValuesCount: 3,
						SeriesCount:      1000,
						Cardinality: []api.LabelValuesCardinality{
							{LabelValue: "staging", SeriesCount: 334},
							{LabelValue: "dev", SeriesCount: 333},
							{LabelValue: "prod", SeriesCount: 333},
						},
					},
				},
			},
		},
		"obtain labels cardinality applying selector": {
			labelNames: []string{"env", "job"},
			selector:   "{job=~'store-.*'}",
			expectedResult: api.LabelValuesCardinalityResponse{
				SeriesCountTotal: numSeriesToPush,
				Labels: []api.LabelNamesCardinality{
					{
						LabelName:        "env",
						LabelValuesCount: 3,
						SeriesCount:      200,
						Cardinality: []api.LabelValuesCardinality{
							{LabelValue: "dev", SeriesCount: 67},
							{LabelValue: "staging", SeriesCount: 67},
							{LabelValue: "prod", SeriesCount: 66},
						},
					},
					{
						LabelName:        "job",
						LabelValuesCount: 1,
						SeriesCount:      200,
						Cardinality: []api.LabelValuesCardinality{
							{LabelValue: "store-gateway", SeriesCount: 200},
						},
					},
				},
			},
		},
		"obtain labels cardinality with default and custom limit": {
			labelNames: []string{"env", "job"},
			limit:      2,
			expectedResult: api.LabelValuesCardinalityResponse{
				SeriesCountTotal: numSeriesToPush,
				Labels: []api.LabelNamesCardinality{
					{
						LabelName:        "env",
						LabelValuesCount: 3,
						SeriesCount:      1000,
						Cardinality: []api.LabelValuesCardinality{
							{LabelValue: "staging", SeriesCount: 334},
							{LabelValue: "dev", SeriesCount: 333},
						},
					},
					{
						LabelName:        "job",
						LabelValuesCount: 5,
						SeriesCount:      1000,
						Cardinality: []api.LabelValuesCardinality{
							{LabelValue: "compactor", SeriesCount: 200},
							{LabelValue: "distributor", SeriesCount: 200},
						},
					},
				},
			},
		},
	}

	// Run tests.
	for tn, tc := range tests {
		t.Run(tn, func(t *testing.T) {
			s, err := e2e.NewScenario(networkName)
			require.NoError(t, err)
			defer s.Close()

			// Set configuration.
			flags := mergeFlags(BlocksStorageFlags(), BlocksStorageS3Flags(), map[string]string{
				"-querier.cardinality-analysis-enabled": "true",
				"-ingester.ring.replication-factor":     "3",
			})

			// Start dependencies.
			consul := e2edb.NewConsul()
			minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
			require.NoError(t, s.StartAndWaitReady(consul, minio))

			// Start the query-frontend.
			queryFrontend := e2emimir.NewQueryFrontend("query-frontend", consul.NetworkHTTPEndpoint(), flags)
			require.NoError(t, s.Start(queryFrontend))
			flags["-querier.frontend-address"] = queryFrontend.NetworkGRPCEndpoint()

			// Start all other Mimir services.
			distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags)
			ingester1 := e2emimir.NewIngester("ingester-1", consul.NetworkHTTPEndpoint(), flags)
			ingester2 := e2emimir.NewIngester("ingester-2", consul.NetworkHTTPEndpoint(), flags)
			ingester3 := e2emimir.NewIngester("ingester-3", consul.NetworkHTTPEndpoint(), flags)
			querier := e2emimir.NewQuerier("querier", consul.NetworkHTTPEndpoint(), flags)

			require.NoError(t, s.StartAndWaitReady(distributor, ingester1, ingester2, ingester3, querier))
			require.NoError(t, s.WaitReady(queryFrontend))

			// Wait until distributor and queriers have updated the ring.
			require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(3), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
				labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
				labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))),
			)

			require.NoError(t, querier.WaitSumMetricsWithOptions(e2e.Equals(3), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
				labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
				labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))),
			)

			// Push series.
			now := time.Now()

			client, err := e2emimir.NewClient(distributor.HTTPEndpoint(), queryFrontend.HTTPEndpoint(), "", "", userID)
			require.NoError(t, err)

			for i := 1; i <= numSeriesToPush; i++ {
				metricName := fmt.Sprintf("series_%d", i)
				series, _, _ := generateAlternatingSeries(i)(metricName, now,
					prompb.Label{Name: "env", Value: cardinalityEnvLabelValues[i%len(cardinalityEnvLabelValues)]},
					prompb.Label{Name: "job", Value: cardinalityJobLabelValues[i%len(cardinalityJobLabelValues)]},
				)

				res, err := client.Push(series)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, res.StatusCode)
			}

			// Since the Push() response is sent as soon as the quorum is reached, when we reach this point
			// the final ingester may not have received series yet.
			// To avoid flaky test we retry the assertions until we hit the desired state within a reasonable timeout.
			test.Poll(t, 3*time.Second, numSeriesToPush*3, func() interface{} {
				var totalIngestedSeries int
				for _, ing := range []*e2emimir.MimirService{ingester1, ingester2, ingester3} {
					values, err := ing.SumMetrics([]string{"cortex_ingester_memory_series"})
					require.NoError(t, err)

					numMemorySeries := e2e.SumValues(values)
					totalIngestedSeries += int(numMemorySeries)
				}
				return totalIngestedSeries
			})

			// Fetch label values cardinality.
			lbValuesCardinalityResp, err := client.LabelValuesCardinality(tc.labelNames, tc.selector, tc.limit)
			require.NoError(t, err)

			// Ensure all ingesters have been invoked.
			ingesters := []*e2emimir.MimirService{ingester1, ingester2, ingester3}
			for _, ing := range ingesters {
				require.NoError(t, ing.WaitSumMetricsWithOptions(
					e2e.Equals(1),
					[]string{"cortex_request_duration_seconds"},
					e2e.WithMetricCount,
					e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "route", "/cortex.Ingester/LabelValuesCardinality"))))
			}

			// Make sure the resultant label names are sorted
			sort.Slice(lbValuesCardinalityResp.Labels, func(l, r int) bool {
				return lbValuesCardinalityResp.Labels[l].LabelName < lbValuesCardinalityResp.Labels[r].LabelName
			})
			require.Equal(t, tc.expectedResult, *lbValuesCardinalityResp)
		})
	}
}
