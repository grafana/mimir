// SPDX-License-Identifier: AGPL-3.0-only
//go:build requires_docker

package integration

import (
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/grafana/dskit/test"
	"github.com/grafana/e2e"
	e2ecache "github.com/grafana/e2e/cache"
	e2edb "github.com/grafana/e2e/db"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	v1 "github.com/prometheus/prometheus/web/api/v1"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/integration/e2emimir"
)

func TestQueryFrontendLabelsQueryOptimizerIdempotency(t *testing.T) {
	now := time.Now()

	testSeries := []prompb.TimeSeries{
		generateFloatSeriesModel("series_1", now, prompb.Label{Name: "service", Value: "service-1"}, prompb.Label{Name: "pod", Value: "pod-1"}),
		generateFloatSeriesModel("series_1", now, prompb.Label{Name: "service", Value: "service-1"}, prompb.Label{Name: "pod", Value: "pod-2"}),
		generateFloatSeriesModel("series_2", now, prompb.Label{Name: "service", Value: "service-2"}, prompb.Label{Name: "pod", Value: "pod-1"}),
		generateFloatSeriesModel("series_2", now, prompb.Label{Name: "service", Value: "service-3"}, prompb.Label{Name: "pod", Value: "pod-2"}),
		generateFloatSeriesModel("series_2", now, prompb.Label{Name: "service", Value: "service-4"}),
		generateFloatSeriesModel("series_2", now, prompb.Label{Name: "pod", Value: "pod-3"}),
		generateFloatSeriesModel("series_3", now),
	}

	// Requirements:
	// 1. Each test case MUST trigger the optimization.
	// 2. The list of test cases MUST be a slice to have predictable order.
	testCases := []struct {
		name               string
		matchers           []string
		expectedLabelNames []string

		// The map key is the label name and the value is the expected label values.
		expectedLabelValues map[string]model.LabelValues
	}{
		{
			name:               `one matchers set including __name__!=""`,
			matchers:           []string{`{__name__!="", pod="pod-1"}`},
			expectedLabelNames: []string{"__name__", "pod", "service"},
			expectedLabelValues: map[string]model.LabelValues{
				"__name__": {"series_1", "series_2", "series_3"},
				"service":  {"service-1", "service-2"},
				"pod":      {"pod-1"},
				"unknown":  {},
			},
		},
		{
			name:               `multiple matchers sets including __name__!=""`,
			matchers:           []string{`{__name__!=""}`, `{service!=""}`},
			expectedLabelNames: []string{"__name__", "pod", "service"},
			expectedLabelValues: map[string]model.LabelValues{
				"__name__": {"series_1", "series_2"},
				"service":  {"service-1", "service-2", "service-3"},
				"pod":      {"pod-1", "pod-2"},
				"unknown":  {},
			},
		},
		{
			name:               `matchers with regex .* pattern`,
			matchers:           []string{`{service=~".*", pod="pod-1"}`},
			expectedLabelNames: []string{"__name__", "pod", "service"},
			expectedLabelValues: map[string]model.LabelValues{
				"__name__": {"series_1", "series_2"},
				"service":  {"service-1", "service-2"},
				"pod":      {"pod-1"},
				"unknown":  {},
			},
		},
	}

	setupAndRun := func(t *testing.T, optimizerEnabled bool, fn func(queryFrontend *e2emimir.MimirService, client *e2emimir.Client)) {
		s, err := e2e.NewScenario(networkName)
		require.NoError(t, err)
		defer s.Close()

		// Start dependencies
		memcached := e2ecache.NewMemcached()
		consul := e2edb.NewConsul()
		require.NoError(t, s.StartAndWaitReady(consul, memcached))

		flags := mergeFlags(BlocksStorageFlags(), BlocksStorageS3Flags(), map[string]string{
			"-query-frontend.labels-query-optimizer-enabled": strconv.FormatBool(optimizerEnabled),
		})

		// Start minio
		minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
		require.NoError(t, s.StartAndWaitReady(minio))

		// Start the query-scheduler.
		queryScheduler := e2emimir.NewQueryScheduler("query-scheduler", flags)
		require.NoError(t, s.StartAndWaitReady(queryScheduler))
		flags["-query-frontend.scheduler-address"] = queryScheduler.NetworkGRPCEndpoint()
		flags["-querier.scheduler-address"] = queryScheduler.NetworkGRPCEndpoint()

		// Start the query-frontend
		queryFrontend := e2emimir.NewQueryFrontend("query-frontend", consul.NetworkHTTPEndpoint(), flags)
		require.NoError(t, s.Start(queryFrontend))

		// Start all other Mimir services
		distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags)
		ingester := e2emimir.NewIngester("ingester", consul.NetworkHTTPEndpoint(), flags)
		querier := e2emimir.NewQuerier("querier", consul.NetworkHTTPEndpoint(), flags)

		require.NoError(t, s.StartAndWaitReady(distributor, ingester, querier))
		require.NoError(t, s.WaitReady(queryFrontend))

		// Wait until distributor and queriers have updated the ring
		require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
			labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
			labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

		require.NoError(t, querier.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
			labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
			labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

		// Push series
		client, err := e2emimir.NewClient(distributor.HTTPEndpoint(), queryFrontend.HTTPEndpoint(), "", "", userID)
		require.NoError(t, err)

		res, err := client.Push(testSeries)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, res.StatusCode)

		// Wait for all series to be ingested
		test.Poll(t, 3*time.Second, len(testSeries), func() interface{} {
			values, err := ingester.SumMetrics([]string{"cortex_ingester_memory_series"})
			require.NoError(t, err)
			numMemorySeries := e2e.SumValues(values)
			return int(numMemorySeries)
		})

		fn(queryFrontend, client)
	}

	expectedLabelNames := map[string][]string{}
	expectedLabelValues := map[string]map[string]model.LabelValues{}

	// First run with optimizer disabled to get expected results.
	t.Run("with optimizer disabled", func(t *testing.T) {
		setupAndRun(t, false, func(queryFrontend *e2emimir.MimirService, client *e2emimir.Client) {
			for _, tc := range testCases {
				totalQueries := 0

				t.Run(tc.name, func(t *testing.T) {
					labelNames, err := client.LabelNames(v1.MinTime, v1.MaxTime, tc.matchers)
					totalQueries++
					require.NoError(t, err)

					expectedLabelNames[tc.name] = labelNames
					expectedLabelValues[tc.name] = map[string]model.LabelValues{}

					for labelName := range tc.expectedLabelValues {
						labelValues, err := client.LabelValues(labelName, v1.MinTime, v1.MaxTime, tc.matchers)
						totalQueries++
						require.NoError(t, err)

						expectedLabelValues[tc.name][labelName] = labelValues
					}
				})
			}

			// Ensure the optimizer was actually disabled.
			require.NoError(t, queryFrontend.WaitSumMetrics(e2e.Equals(0), "cortex_query_frontend_labels_optimizer_queries_total"))
		})
	})

	// The run with optimizer enabled and ensure the results match the previously fetched one.
	t.Run("with optimizer enabled", func(t *testing.T) {
		setupAndRun(t, true, func(queryFrontend *e2emimir.MimirService, client *e2emimir.Client) {
			totalQueries := 0

			for _, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {
					labelNames, err := client.LabelNames(v1.MinTime, v1.MaxTime, tc.matchers)
					totalQueries++
					require.NoError(t, err)
					require.Equal(t, expectedLabelNames[tc.name], labelNames)

					for labelName := range tc.expectedLabelValues {
						labelValues, err := client.LabelValues(labelName, v1.MinTime, v1.MaxTime, tc.matchers)
						totalQueries++
						require.NoError(t, err)
						require.Equal(t, expectedLabelValues[tc.name][labelName], labelValues)
					}
				})
			}

			// Ensure the optimizer was actually enabled.
			require.NoError(t, queryFrontend.WaitSumMetrics(e2e.Equals(float64(totalQueries)), "cortex_query_frontend_labels_optimizer_queries_total"))

			// Ensure all test cases triggered the optimization
			require.NoError(t, queryFrontend.WaitSumMetrics(e2e.Equals(float64(totalQueries)), "cortex_query_frontend_labels_optimizer_queries_rewritten_total"))
		})
	})
}
