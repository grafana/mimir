// SPDX-License-Identifier: AGPL-3.0-only
//go:build requires_docker

package integration

import (
	"fmt"
	"net/http"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/grafana/dskit/test"
	"github.com/grafana/e2e"
	e2edb "github.com/grafana/e2e/db"
	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/integration/e2emimir"
)

func TestDistributor(t *testing.T) {
	t.Run("caching_unmarshal_data_enabled", func(t *testing.T) {
		testDistributorWithCachingUnmarshalData(t, true)
	})

	t.Run("caching_unmarshal_data_disabled", func(t *testing.T) {
		testDistributorWithCachingUnmarshalData(t, false)
	})
}

func testDistributorWithCachingUnmarshalData(t *testing.T, cachingUnmarshalDataEnabled bool) {
	queryEnd := time.Now().Round(time.Second)
	queryStart := queryEnd.Add(-1 * time.Hour)
	queryStep := 10 * time.Minute

	overridesWithExemplars := func(maxExemplars int) string {
		return fmt.Sprintf("overrides:\n  \"%s\":\n    max_global_exemplars_per_user: %d\n", userID, maxExemplars)
	}

	testCases := map[string]struct {
		inSeries        [][]prompb.TimeSeries
		runtimeConfig   string
		queries         map[string]model.Matrix
		exemplarQueries map[string][]promv1.ExemplarQueryResult
	}{
		"no special features": {
			inSeries: [][]prompb.TimeSeries{{{
				Labels:  []prompb.Label{{Name: "__name__", Value: "foobar"}},
				Samples: []prompb.Sample{{Timestamp: queryStart.UnixMilli(), Value: 100}},
			}}},
			queries: map[string]model.Matrix{
				"foobar": {{
					Metric: model.Metric{"__name__": "foobar"},
					Values: []model.SamplePair{{Timestamp: model.Time(queryStart.UnixMilli()), Value: model.SampleValue(100)}},
				}},
			},
		},

		"drop empty labels": {
			inSeries: [][]prompb.TimeSeries{{{
				Labels:  []prompb.Label{{Name: "__name__", Value: "series_with_empty_label"}, {Name: "empty", Value: ""}},
				Samples: []prompb.Sample{{Timestamp: queryStart.UnixMilli(), Value: 100}},
			}}},
			queries: map[string]model.Matrix{
				"series_with_empty_label": {{
					Metric: model.Metric{"__name__": "series_with_empty_label"},
					Values: []model.SamplePair{{Timestamp: model.Time(queryStart.UnixMilli()), Value: model.SampleValue(100)}},
				}},
			},
		},

		"wrong labels order": {
			inSeries: [][]prompb.TimeSeries{{{
				Labels:  []prompb.Label{{Name: "__name__", Value: "series_with_wrong_labels_order"}, {Name: "zzz", Value: "1"}, {Name: "aaa", Value: "2"}},
				Samples: []prompb.Sample{{Timestamp: queryStart.UnixMilli(), Value: 100}},
			}}},
			queries: map[string]model.Matrix{
				"series_with_wrong_labels_order": {{
					Metric: model.Metric{"__name__": "series_with_wrong_labels_order", "aaa": "2", "zzz": "1"},
					Values: []model.SamplePair{{Timestamp: model.Time(queryStart.UnixMilli()), Value: model.SampleValue(100)}},
				}},
			},
		},

		"deduplicated requests": {
			inSeries: [][]prompb.TimeSeries{
				// Request from replica "a"
				{{
					Labels:  []prompb.Label{{Name: "__name__", Value: "series1"}, {Name: "cluster", Value: "C"}, {Name: "replica", Value: "a"}},
					Samples: []prompb.Sample{{Timestamp: queryStart.UnixMilli(), Value: 100}},
				}},
				// Request from replica "b", will be ignored.
				{{
					Labels:  []prompb.Label{{Name: "__name__", Value: "series2"}, {Name: "cluster", Value: "C"}, {Name: "replica", Value: "b"}},
					Samples: []prompb.Sample{{Timestamp: queryStart.UnixMilli(), Value: 100}},
				}},
			},
			queries: map[string]model.Matrix{
				"series1": {{
					Metric: model.Metric{"__name__": "series1", "cluster": "C"},
					Values: []model.SamplePair{{Timestamp: model.Time(queryStart.UnixMilli()), Value: model.SampleValue(100)}},
				}},
				"series2": {},
			},

			runtimeConfig: `
overrides:
  "` + userID + `":
    ha_cluster_label: "cluster"
    ha_replica_label: "replica"
`,
		},

		"dropped labels": {
			inSeries: [][]prompb.TimeSeries{
				{{
					Labels:  []prompb.Label{{Name: "__name__", Value: "series_with_dropped_label"}, {Name: "dropped_label", Value: "some value"}},
					Samples: []prompb.Sample{{Timestamp: queryStart.UnixMilli(), Value: 100}},
				}},
			},
			queries: map[string]model.Matrix{
				"series_with_dropped_label": {{
					Metric: model.Metric{"__name__": "series_with_dropped_label"},
					Values: []model.SamplePair{{Timestamp: model.Time(queryStart.UnixMilli()), Value: model.SampleValue(100)}},
				}},
			},
			runtimeConfig: `
overrides:
  "` + userID + `":
    drop_labels:
      - dropped_label
`,
		},

		"relabeling test, using prometheus label": {
			inSeries: [][]prompb.TimeSeries{
				{{
					Labels:  []prompb.Label{{Name: "__name__", Value: "series_with_relabeling_applied"}, {Name: "prometheus", Value: "cluster/instance"}},
					Samples: []prompb.Sample{{Timestamp: queryStart.UnixMilli(), Value: 100}},
				}},
			},
			queries: map[string]model.Matrix{
				"series_with_relabeling_applied": {{
					Metric: model.Metric{
						"__name__":            "series_with_relabeling_applied",
						"prometheus":          "cluster/instance",
						"prometheus_instance": "instance", // label created by relabelling.
					},
					Values: []model.SamplePair{{Timestamp: model.Time(queryStart.UnixMilli()), Value: model.SampleValue(100)}},
				}},
			},

			runtimeConfig: `
overrides:
  "` + userID + `":
    metric_relabel_configs:
      - source_labels: [prometheus]
        regex: ".*/(.+)"
        target_label: "prometheus_instance"
        replacement: "$1"
        action: replace
`,
		},

		"series with exemplars": {
			inSeries: [][]prompb.TimeSeries{{{
				Labels:    []prompb.Label{{Name: "__name__", Value: "foobar_with_exemplars"}},
				Samples:   []prompb.Sample{{Timestamp: queryStart.UnixMilli(), Value: 100}},
				Exemplars: []prompb.Exemplar{{Labels: []prompb.Label{{Name: "test", Value: "test"}}, Value: 123.0, Timestamp: queryStart.UnixMilli()}},
			}}},
			exemplarQueries: map[string][]promv1.ExemplarQueryResult{
				"foobar_with_exemplars": {{
					SeriesLabels: model.LabelSet{
						"__name__": "foobar_with_exemplars",
					},
					Exemplars: []promv1.Exemplar{{
						Labels:    model.LabelSet{"test": "test"},
						Value:     123.0,
						Timestamp: model.Time(queryStart.UnixMilli()),
					}},
				}},
			},
			runtimeConfig: overridesWithExemplars(100),
		},

		"series with old exemplars": {
			inSeries: [][]prompb.TimeSeries{{{
				Labels:  []prompb.Label{{Name: "__name__", Value: "foobar_with_old_exemplars"}},
				Samples: []prompb.Sample{{Timestamp: queryStart.UnixMilli(), Value: 100}},
				Exemplars: []prompb.Exemplar{{
					Labels:    []prompb.Label{{Name: "test", Value: "test"}},
					Value:     123.0,
					Timestamp: queryStart.Add(-10 * time.Minute).UnixMilli(), // Exemplars older than 10 minutes from oldest sample for the series in the request are dropped by distributor.
				}},
			}}},
			exemplarQueries: map[string][]promv1.ExemplarQueryResult{
				"foobar_with_old_exemplars": {},
			},
			runtimeConfig: overridesWithExemplars(100),
		},

		"sending series with exemplars, when exemplars are disabled": {
			inSeries: [][]prompb.TimeSeries{{{
				Labels:    []prompb.Label{{Name: "__name__", Value: "foobar_with_exemplars_disabled"}},
				Samples:   []prompb.Sample{{Timestamp: queryStart.UnixMilli(), Value: 100}},
				Exemplars: []prompb.Exemplar{{Labels: []prompb.Label{{Name: "test", Value: "test"}}, Value: 123.0, Timestamp: queryStart.UnixMilli()}},
			}}},
			exemplarQueries: map[string][]promv1.ExemplarQueryResult{
				"foobar_with_exemplars_disabled": {},
			},
			// By disabling exemplars via runtime config, distributor will stop sending them to ingester.
			runtimeConfig: overridesWithExemplars(0),
		},

		"reduce native histogram buckets via down scaling": {
			runtimeConfig: `
overrides:
  "` + userID + `":
    native_histograms_ingestion_enabled: true
    max_native_histogram_buckets: 7
`,
			inSeries: [][]prompb.TimeSeries{{{
				Labels: []prompb.Label{{Name: "__name__", Value: "histogram_down_scaling_series"}},
				Histograms: []prompb.Histogram{{
					// This histogram has 4+4=8 buckets (without zero bucket), but only 7 are allowed by the runtime config.
					Count:          &prompb.Histogram_CountInt{CountInt: 12},
					ZeroCount:      &prompb.Histogram_ZeroCountInt{ZeroCountInt: 2},
					ZeroThreshold:  0.001,
					Sum:            18.4,
					Schema:         0,
					NegativeSpans:  []prompb.BucketSpan{{Offset: 0, Length: 2}, {Offset: 1, Length: 2}},
					NegativeDeltas: []int64{1, 1, -1, 0},
					PositiveSpans:  []prompb.BucketSpan{{Offset: 0, Length: 2}, {Offset: 1, Length: 2}},
					PositiveDeltas: []int64{1, 1, -1, 0},
					Timestamp:      queryStart.UnixMilli(),
				}},
			}}},
			queries: map[string]model.Matrix{
				"histogram_down_scaling_series": {{
					Metric: model.Metric{
						"__name__": "histogram_down_scaling_series",
					},
					Histograms: []model.SampleHistogramPair{{Timestamp: model.Time(queryStart.UnixMilli()), Histogram: &model.SampleHistogram{
						Count: 12,
						Sum:   18.4,
						Buckets: model.HistogramBuckets{
							// This histogram has 3+3=6 buckets (without zero bucket), which was down scaled from 4+4=8 buckets.
							&model.HistogramBucket{Boundaries: 1, Lower: -16, Upper: -4, Count: 2},
							&model.HistogramBucket{Boundaries: 1, Lower: -4, Upper: -1, Count: 2},
							&model.HistogramBucket{Boundaries: 1, Lower: -1, Upper: -0.25, Count: 1},
							&model.HistogramBucket{Boundaries: 3, Lower: -0.001, Upper: 0.001, Count: 2},
							&model.HistogramBucket{Boundaries: 0, Lower: 0.25, Upper: 1, Count: 1},
							&model.HistogramBucket{Boundaries: 0, Lower: 1, Upper: 4, Count: 2},
							&model.HistogramBucket{Boundaries: 0, Lower: 4, Upper: 16, Count: 2},
						},
					},
					}},
				}},
			},
		},
	}

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	previousRuntimeConfig := ""
	require.NoError(t, writeFileToSharedDir(s, "runtime.yaml", []byte(previousRuntimeConfig)))

	// Start dependencies.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, blocksBucketName)
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	baseFlags := map[string]string{
		"-distributor.ingestion-tenant-shard-size":           "0",
		"-ingester.ring.heartbeat-period":                    "1s",
		"-distributor.ha-tracker.enable":                     "true",
		"-distributor.ha-tracker.enable-for-all-users":       "true",
		"-distributor.ha-tracker.store":                      "consul",
		"-distributor.ha-tracker.consul.hostname":            consul.NetworkHTTPEndpoint(),
		"-distributor.ha-tracker.prefix":                     "prom_ha/",
		"-timeseries-unmarshal-caching-optimization-enabled": strconv.FormatBool(cachingUnmarshalDataEnabled),
	}

	flags := mergeFlags(
		BlocksStorageFlags(),
		BlocksStorageS3Flags(),
		baseFlags,
	)

	// We want only distributor to be reloading runtime config.
	distributorFlags := mergeFlags(flags, map[string]string{
		"-runtime-config.file":          filepath.Join(e2e.ContainerSharedDir, "runtime.yaml"),
		"-runtime-config.reload-period": "100ms",
		// Set non-zero default for number of exemplars. That way our values used in the test (0 and 100) will show up in runtime config diff.
		"-ingester.max-global-exemplars-per-user": "3",
	})

	// Ingester will not reload runtime config.
	ingesterFlags := mergeFlags(flags, map[string]string{
		// Ingester will always see exemplars enabled. We do this to avoid waiting for ingester to apply new setting to TSDB.
		"-ingester.max-global-exemplars-per-user": "100",
	})

	// Start Mimir components.
	distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), distributorFlags)
	ingester := e2emimir.NewIngester("ingester", consul.NetworkHTTPEndpoint(), ingesterFlags)
	querier := e2emimir.NewQuerier("querier", consul.NetworkHTTPEndpoint(), flags)
	require.NoError(t, s.StartAndWaitReady(distributor, ingester, querier))

	// Wait until distributor has updated the ring.
	require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	// Wait until querier has updated the ring.
	require.NoError(t, querier.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	client, err := e2emimir.NewClient(distributor.HTTPEndpoint(), querier.HTTPEndpoint(), "", "", userID)
	require.NoError(t, err)

	runtimeConfigURL := fmt.Sprintf("http://%s/runtime_config?mode=diff", distributor.HTTPEndpoint())

	for testName, tc := range testCases {
		t.Run(testName, func(t *testing.T) {
			for _, ser := range tc.inSeries {
				if tc.runtimeConfig != previousRuntimeConfig {
					currentRuntimeConfig, err := getURL(runtimeConfigURL)
					require.NoError(t, err)

					// Write new runtime config
					require.NoError(t, writeFileToSharedDir(s, "runtime.yaml", []byte(tc.runtimeConfig)))

					// Wait until distributor has reloaded runtime config.
					test.Poll(t, 1*time.Second, true, func() interface{} {
						newRuntimeConfig, err := getURL(runtimeConfigURL)
						require.NoError(t, err)
						return currentRuntimeConfig != newRuntimeConfig
					})

					previousRuntimeConfig = tc.runtimeConfig
				}

				res, err := client.Push(ser)
				require.NoError(t, err)
				require.True(t, res.StatusCode == http.StatusOK || res.StatusCode == http.StatusAccepted, res.Status)
			}

			for q, res := range tc.queries {
				result, err := client.QueryRange(q, queryStart, queryEnd, queryStep)
				require.NoError(t, err)

				require.Equal(t, res.String(), result.String())
			}

			for q, expResult := range tc.exemplarQueries {
				result, err := client.QueryExemplars(q, queryStart, queryEnd)
				require.NoError(t, err)

				require.Equal(t, expResult, result)
			}
		})
	}
}
