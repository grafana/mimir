// SPDX-License-Identifier: AGPL-3.0-only
//go:build requires_docker

package integration

import (
	"fmt"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/grafana/dskit/test"
	"github.com/grafana/e2e"
	e2edb "github.com/grafana/e2e/db"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/integration/e2emimir"
)

func TestDistributor(t *testing.T) {
	queryEnd := time.Now().Round(time.Second)
	queryStart := queryEnd.Add(-1 * time.Hour)
	queryStep := 10 * time.Minute

	testCases := map[string]struct {
		inSeries      [][]prompb.TimeSeries
		runtimeConfig string
		queries       map[string]model.Matrix
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
		"-distributor.ingestion-tenant-shard-size":     "0",
		"-ingester.ring.heartbeat-period":              "1s",
		"-distributor.ha-tracker.enable":               "true",
		"-distributor.ha-tracker.enable-for-all-users": "true",
		"-distributor.ha-tracker.store":                "consul",
		"-distributor.ha-tracker.consul.hostname":      consul.NetworkHTTPEndpoint(),
		"-distributor.ha-tracker.prefix":               "prom_ha/",
		"-runtime-config.file":                         filepath.Join(e2e.ContainerSharedDir, "runtime.yaml"),
		"-runtime-config.reload-period":                "100ms",
	}

	flags := mergeFlags(
		BlocksStorageFlags(),
		BlocksStorageS3Flags(),
		baseFlags,
	)

	// Start Mimir components.
	distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags)
	ingester := e2emimir.NewIngester("ingester", consul.NetworkHTTPEndpoint(), flags)
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
		})
	}
}
