// SPDX-License-Identifier: AGPL-3.0-only
//go:build requires_docker
// +build requires_docker

package integration

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/integration/e2e"
	e2edb "github.com/grafana/mimir/integration/e2e/db"
	"github.com/grafana/mimir/integration/e2emimir"
)

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

	// Create 'env' and 'job' label sets.
	envLabelVal := map[int]string{
		0: "prod",
		1: "staging",
		2: "dev",
	}
	jobLabelVal := map[int]string{
		0: "distributor",
		1: "ingester",
		2: "store-gateway",
		3: "querier",
		4: "compactor",
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
			flags := mergeFlags(BlocksStorageFlags(), map[string]string{
				"-querier.cardinality-analysis-enabled": "true",
				"-distributor.replication-factor":       "3",
			})

			// Start dependencies.
			consul := e2edb.NewConsul()
			minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
			require.NoError(t, s.StartAndWaitReady(consul, minio))

			// Start the query-frontend.
			queryFrontend := e2emimir.NewQueryFrontend("query-frontend", flags, "")
			require.NoError(t, s.Start(queryFrontend))
			flags["-querier.frontend-address"] = queryFrontend.NetworkGRPCEndpoint()

			// Start all other Mimir services.
			distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags, "")
			ingester1 := e2emimir.NewIngester("ingester-1", consul.NetworkHTTPEndpoint(), flags, "")
			ingester2 := e2emimir.NewIngester("ingester-2", consul.NetworkHTTPEndpoint(), flags, "")
			ingester3 := e2emimir.NewIngester("ingester-3", consul.NetworkHTTPEndpoint(), flags, "")
			querier := e2emimir.NewQuerier("querier", consul.NetworkHTTPEndpoint(), flags, "")

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
				series, _ := generateSeries(metricName, now,
					prompb.Label{Name: "env", Value: envLabelVal[i%len(envLabelVal)]},
					prompb.Label{Name: "job", Value: jobLabelVal[i%len(jobLabelVal)]},
				)

				res, err := client.Push(series)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, res.StatusCode)
			}

			// Fetch label names and values.
			res, err := client.LabelNamesAndValues(tc.selector, tc.limit)
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, res.StatusCode)

			// Ensure all ingesters have been invoked.
			ingesters := []*e2emimir.MimirService{ingester1, ingester2, ingester3}
			for _, ing := range ingesters {
				require.NoError(t, ing.WaitSumMetricsWithOptions(
					e2e.Equals(1),
					[]string{"cortex_request_duration_seconds"},
					e2e.WithMetricCount,
					e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "route", "/cortex.Ingester/LabelNamesAndValues"))))
			}

			// Test results.
			var lbNamesAndValuesResp labelNamesAndValuesResponse
			require.NoError(t, json.NewDecoder(res.Body).Decode(&lbNamesAndValuesResp))

			require.Equal(t, tc.expectedResult, lbNamesAndValuesResp)
		})
	}
}
