// SPDX-License-Identifier: AGPL-3.0-only
//go:build requires_docker

package integration

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"testing"
	"time"

	"github.com/grafana/dskit/test"
	"github.com/grafana/e2e"
	e2edb "github.com/grafana/e2e/db"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/integration/e2emimir"
)

// Define cardinality 'env' and 'job' label sets.
var cardinalityEnvLabelValues = []string{"prod", "staging", "dev"}
var cardinalityJobLabelValues = []string{"distributor", "ingester", "store-gateway", "querier", "compactor"}

func TestQuerierLabelNamesAndValues(t *testing.T) {
	const numSeriesToPush = 1000

	// Define response types.
	type labelNamesAndValuesCardinality struct {
		LabelName        string `json:"label_name"`
		LabelValuesCount int    `json:"label_values_count"`
	}
	type labelNamesAndValuesResponse struct {
		LabelValuesCountTotal int                              `json:"label_values_count_total"`
		LabelNamesCount       int                              `json:"label_names_count"`
		Cardinality           []labelNamesAndValuesCardinality `json:"cardinality"`
	}

	// Test cases
	tests := map[string]struct {
		selector       string
		limit          int
		expectedResult labelNamesAndValuesResponse
	}{
		"obtain label names and values with default selector and limit": {
			expectedResult: labelNamesAndValuesResponse{
				LabelValuesCountTotal: 1008,
				LabelNamesCount:       3,
				Cardinality: []labelNamesAndValuesCardinality{
					{LabelName: labels.MetricName, LabelValuesCount: 1000},
					{LabelName: "job", LabelValuesCount: 5},
					{LabelName: "env", LabelValuesCount: 3},
				},
			},
		},
		"apply request selector": {
			selector: "{job=~'store-.*'}",
			expectedResult: labelNamesAndValuesResponse{
				LabelValuesCountTotal: 204,
				LabelNamesCount:       3,
				Cardinality: []labelNamesAndValuesCardinality{
					{LabelName: labels.MetricName, LabelValuesCount: 200},
					{LabelName: "env", LabelValuesCount: 3},
					{LabelName: "job", LabelValuesCount: 1},
				},
			},
		},
		"limit cardinality response elements": {
			limit: 1,
			expectedResult: labelNamesAndValuesResponse{
				LabelValuesCountTotal: 1008,
				LabelNamesCount:       3,
				Cardinality: []labelNamesAndValuesCardinality{
					{LabelName: labels.MetricName, LabelValuesCount: 1000},
				},
			},
		},
		"apply request selector and limit": {
			selector: "{job=~'store-.*'}",
			limit:    1,
			expectedResult: labelNamesAndValuesResponse{
				LabelValuesCountTotal: 204,
				LabelNamesCount:       3,
				Cardinality: []labelNamesAndValuesCardinality{
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
			queryFrontend := e2emimir.NewQueryFrontend("query-frontend", flags)
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
				var genSeries generateSeriesFunc
				if i%2 == 0 {
					genSeries = generateFloatSeries
				} else {
					genSeries = generateHistogramSeries
				}
				series, _, _ := genSeries(metricName, now,
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
			res, err := client.LabelNamesAndValues(tc.selector, tc.limit)
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, res.StatusCode)

			// Test results.
			var lbNamesAndValuesResp labelNamesAndValuesResponse
			require.NoError(t, json.NewDecoder(res.Body).Decode(&lbNamesAndValuesResp))

			require.Equal(t, tc.expectedResult, lbNamesAndValuesResp)
		})
	}
}

func TestQuerierLabelValuesCardinality(t *testing.T) {
	const numSeriesToPush = 1000

	// Define response types.
	type labelValuesCardinality struct {
		LabelValue  string `json:"label_value"`
		SeriesCount uint64 `json:"series_count"`
	}

	type labelNamesCardinality struct {
		LabelName        string                   `json:"label_name"`
		LabelValuesCount uint64                   `json:"label_values_count"`
		SeriesCount      uint64                   `json:"series_count"`
		Cardinality      []labelValuesCardinality `json:"cardinality"`
	}

	type labelValuesCardinalityResponse struct {
		SeriesCountTotal uint64                  `json:"series_count_total"`
		Labels           []labelNamesCardinality `json:"labels"`
	}

	// Test cases
	tests := map[string]struct {
		labelNames     []string
		selector       string
		limit          int
		expectedResult labelValuesCardinalityResponse
	}{
		"obtain labels cardinality with default selector and limit": {
			labelNames: []string{"env", "job"},
			expectedResult: labelValuesCardinalityResponse{
				SeriesCountTotal: numSeriesToPush,
				Labels: []labelNamesCardinality{
					{
						LabelName:        "env",
						LabelValuesCount: 3,
						SeriesCount:      1000,
						Cardinality: []labelValuesCardinality{
							{LabelValue: "staging", SeriesCount: 334},
							{LabelValue: "dev", SeriesCount: 333},
							{LabelValue: "prod", SeriesCount: 333},
						},
					},
					{
						LabelName:        "job",
						LabelValuesCount: 5,
						SeriesCount:      1000,
						Cardinality: []labelValuesCardinality{
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
			expectedResult: labelValuesCardinalityResponse{
				SeriesCountTotal: numSeriesToPush,
				Labels: []labelNamesCardinality{
					{
						LabelName:        "env",
						LabelValuesCount: 3,
						SeriesCount:      1000,
						Cardinality: []labelValuesCardinality{
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
			expectedResult: labelValuesCardinalityResponse{
				SeriesCountTotal: numSeriesToPush,
				Labels: []labelNamesCardinality{
					{
						LabelName:        "env",
						LabelValuesCount: 3,
						SeriesCount:      200,
						Cardinality: []labelValuesCardinality{
							{LabelValue: "dev", SeriesCount: 67},
							{LabelValue: "staging", SeriesCount: 67},
							{LabelValue: "prod", SeriesCount: 66},
						},
					},
					{
						LabelName:        "job",
						LabelValuesCount: 1,
						SeriesCount:      200,
						Cardinality: []labelValuesCardinality{
							{LabelValue: "store-gateway", SeriesCount: 200},
						},
					},
				},
			},
		},
		"obtain labels cardinality with default and custom limit": {
			labelNames: []string{"env", "job"},
			limit:      2,
			expectedResult: labelValuesCardinalityResponse{
				SeriesCountTotal: numSeriesToPush,
				Labels: []labelNamesCardinality{
					{
						LabelName:        "env",
						LabelValuesCount: 3,
						SeriesCount:      1000,
						Cardinality: []labelValuesCardinality{
							{LabelValue: "staging", SeriesCount: 334},
							{LabelValue: "dev", SeriesCount: 333},
						},
					},
					{
						LabelName:        "job",
						LabelValuesCount: 5,
						SeriesCount:      1000,
						Cardinality: []labelValuesCardinality{
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
			queryFrontend := e2emimir.NewQueryFrontend("query-frontend", flags)
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
				var genSeries generateSeriesFunc
				if i%2 == 0 {
					genSeries = generateFloatSeries
				} else {
					genSeries = generateHistogramSeries
				}
				series, _, _ := genSeries(metricName, now,
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
			res, err := client.LabelValuesCardinality(tc.labelNames, tc.selector, tc.limit)
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, res.StatusCode)

			// Ensure all ingesters have been invoked.
			ingesters := []*e2emimir.MimirService{ingester1, ingester2, ingester3}
			for _, ing := range ingesters {
				require.NoError(t, ing.WaitSumMetricsWithOptions(
					e2e.Equals(1),
					[]string{"cortex_request_duration_seconds"},
					e2e.WithMetricCount,
					e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "route", "/cortex.Ingester/LabelValuesCardinality"))))
			}

			// Test results.
			var lbValuesCardinalityResp labelValuesCardinalityResponse
			require.NoError(t, json.NewDecoder(res.Body).Decode(&lbValuesCardinalityResp))

			// Make sure the resultant label names are sorted
			sort.Slice(lbValuesCardinalityResp.Labels, func(l, r int) bool {
				return lbValuesCardinalityResp.Labels[l].LabelName < lbValuesCardinalityResp.Labels[r].LabelName
			})
			require.Equal(t, tc.expectedResult, lbValuesCardinalityResp)
		})
	}
}
