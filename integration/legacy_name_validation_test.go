// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/integration/alertmanager_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.
//go:build requires_docker

package integration

import (
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/grafana/e2e"
	e2edb "github.com/grafana/e2e/db"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/rulefmt"
	"github.com/prometheus/prometheus/prompb"
	promRW2 "github.com/prometheus/prometheus/prompb/io/prometheus/write/v2"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/integration/e2emimir"
)

// TestLegacyNameValidation_DistributorWrite ensures that distributors discard invalid
// metric and label names in the write path.
func TestLegacyNameValidation_DistributorWrite(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	require.NoError(t, writeFileToSharedDir(s, "runtime.yaml", []byte("")))

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
		"-timeseries-unmarshal-caching-optimization-enabled": "false",
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

	now := time.Now().Truncate(time.Millisecond).UnixMilli()

	testCases := []struct {
		name       string
		rw1request *prompb.WriteRequest
		rw2request *promRW2.Request
	}{
		{
			name: "utf8 metric name",
			rw1request: &prompb.WriteRequest{
				Timeseries: []prompb.TimeSeries{
					{
						Labels:  []prompb.Label{{Name: "__name__", Value: "utf_metric😀C"}},
						Samples: []prompb.Sample{{Timestamp: now, Value: 100}},
					},
				},
				Metadata: []prompb.MetricMetadata{
					{
						MetricFamilyName: "utf_metric😀C_total",
						Help:             "some helpC",
						Unit:             "someunitC",
						Type:             prompb.MetricMetadata_COUNTER,
					},
				},
			},
			rw2request: &promRW2.Request{
				Timeseries: []promRW2.TimeSeries{
					{
						LabelsRefs: []uint32{0, 1},
						Samples:    []promRW2.Sample{{Timestamp: now, Value: 100}},
						Metadata: promRW2.Metadata{
							Type:    promRW2.Metadata_METRIC_TYPE_COUNTER,
							HelpRef: 2,
							UnitRef: 3,
						},
					},
				},
				Symbols: []string{
					"__name__", "utf_metric😀C_total",
					"some helpC",
					"someunitC",
				},
			},
		},
		{
			name: "utf8 label name",
			rw1request: &prompb.WriteRequest{
				Timeseries: []prompb.TimeSeries{
					{
						Labels: []prompb.Label{
							{Name: "__name__", Value: "legacy_metricC"},
							{Name: "utf8_label😀", Value: "test"},
						},
						Samples: []prompb.Sample{{Timestamp: now, Value: 100}},
					},
				},
			},
			rw2request: &promRW2.Request{
				Timeseries: []promRW2.TimeSeries{
					{
						LabelsRefs: []uint32{0, 1, 2, 3},
						Samples:    []promRW2.Sample{{Timestamp: now, Value: 100}},
					},
				},
				Symbols: []string{
					"__name__", "legacy_metricC_total",
					"utf8_label😀", "test",
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Run("rw1", func(t *testing.T) {
				res, err := client.PushRW1(tc.rw1request)
				require.NoError(t, err)
				require.Equal(t, res.StatusCode, http.StatusBadRequest)
			})
			t.Run("rw2", func(t *testing.T) {
				res, err := client.PushRW2(tc.rw2request)
				require.NoError(t, err)
				require.Equal(t, res.StatusCode, http.StatusBadRequest)
			})
		})
	}
}

// TestLegacyNameValidation_Read ensures that Mimir discards queries containing
// invalid metric and label names.
func TestLegacyNameValidation_Querier(t *testing.T) {
	for _, queryEngine := range []string{"mimir", "prometheus"} {
		t.Run(queryEngine, func(t *testing.T) {
			testLegacyNameValidationQuerier(t, queryEngine)
		})
	}
}

func testLegacyNameValidationQuerier(t *testing.T, queryEngine string) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, blocksBucketName)
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	// Configure the blocks storage to frequently compact TSDB head
	// and ship blocks to the storage.
	const blockRangePeriod = 5 * time.Second
	flags := mergeFlags(BlocksStorageFlags(), BlocksStorageS3Flags(), map[string]string{
		"-blocks-storage.tsdb.block-ranges-period":   blockRangePeriod.String(),
		"-blocks-storage.tsdb.ship-interval":         "1s",
		"-blocks-storage.bucket-store.sync-interval": "1s",
		"-blocks-storage.tsdb.retention-period":      ((blockRangePeriod * 2) - 1).String(),
		"-querier.max-fetched-series-per-query":      "3",
		"-querier.query-engine":                      queryEngine,
	})

	// Start Mimir components.
	distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags)
	ingester := e2emimir.NewIngester("ingester", consul.NetworkHTTPEndpoint(), flags)
	storeGateway := e2emimir.NewStoreGateway("store-gateway", consul.NetworkHTTPEndpoint(), flags)
	require.NoError(t, s.StartAndWaitReady(distributor, ingester, storeGateway))

	querier := e2emimir.NewQuerier("querier", consul.NetworkHTTPEndpoint(), flags)
	require.NoError(t, s.StartAndWaitReady(querier)) // Wait until distributor has updated the ring.

	client, err := e2emimir.NewClient(distributor.HTTPEndpoint(), querier.HTTPEndpoint(), "", "", userID)
	require.NoError(t, err)

	now := time.Now().Truncate(time.Millisecond)

	// Initialize mimir with some test data:
	res, err := client.PushRW2(&promRW2.Request{
		Timeseries: []promRW2.TimeSeries{
			{
				LabelsRefs: []uint32{0, 1, 2, 3},
				Samples:    []promRW2.Sample{{Timestamp: now.UnixMilli(), Value: 100}},
			},
		},
		Symbols: []string{
			"__name__", "legacy_metricC_total",
			"label_name", "label_value",
		},
	})
	require.NoError(t, err)
	require.Equal(t, res.StatusCode, http.StatusOK)

	testCases := []struct {
		name    string
		query   string
		wantErr string
	}{
		{
			// this passes always, no matter model.NameValidationScheme
			name:  "utf8 metric name",
			query: `{"invalid_metric😀C_total"}`,
		},
		{
			// this passes always, no matter model.NameValidationScheme
			name:  "utf8 label name",
			query: `legacy_metricC_total{"😀"="test"}`,
		},
		{
			// Invalid promql:
			// see: https://prometheus.io/docs/guides/utf8/
			name:    "invalid promql syntax",
			query:   `invalid_metric😀C_total`,
			wantErr: `bad_data: invalid parameter "query": 1:15: parse error: unexpected character: '😀'`,
		},
		{
			name:    "utf8 label_join",
			query:   `label_join(legacy_metricC_total, "invalid😀", "-", "label_name")`,
			wantErr: `execution: invalid destination label name in label_join(): invalid😀`,
		},
		{
			name:    "utf8 label_replace",
			query:   `label_replace(legacy_metricC_total, "invalid😀", "$1", "label_name", "(.*):.*")`,
			wantErr: `execution: invalid destination label name in label_replace(): invalid😀`,
		},
		{
			name:    "utf8 count_values",
			query:   `count_values("invalid😀", series)`,
			wantErr: `execution: invalid label name "invalid😀"`,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := client.Query(tc.query, now)
			if tc.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tc.wantErr)
			}
		})
	}
}

// TestLegacyNameValidation_Ruler ensures rule only accepts rules that are
// adhere to legacy naming conventions
func TestLegacyNameValidation_Ruler(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, mimirBucketName, rulesBucketName)
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	// Configure the ruler.
	rulerFlags := mergeFlags(CommonStorageBackendFlags(), RulerFlags(), RulerStorageS3Flags(), BlocksStorageFlags())

	// Start Mimir components.
	ruler := e2emimir.NewRuler("ruler", consul.NetworkHTTPEndpoint(), rulerFlags)
	require.NoError(t, s.StartAndWaitReady(ruler))

	// Create a client with the ruler address configured
	c, err := e2emimir.NewClient("", "", "", ruler.HTTPEndpoint(), "user-1")
	require.NoError(t, err)

	const namespace = "test_/encoded_+namespace/?"
	testCases := []struct {
		name string
		rg   rulefmt.RuleGroup
	}{
		{
			name: "utf8 rule group label nanme",
			rg: rulefmt.RuleGroup{
				Name: "test",
				Rules: []rulefmt.Rule{
					{
						Record: "test",
					},
				},
				Labels: map[string]string{
					"invalid😀": "test",
				},
			},
		},
		{
			name: "utf8 recording rule metric name",
			rg: rulefmt.RuleGroup{
				Name: "test",
				Rules: []rulefmt.Rule{
					{
						Record: "invalid😀",
					},
				},
			},
		},
		{
			name: "utf8 rule label name",
			rg: rulefmt.RuleGroup{
				Name: "test",
				Rules: []rulefmt.Rule{
					{
						Record: "valid",
						Labels: map[string]string{
							"invalid😀": "test",
						},
					},
				},
			},
		},
		{
			name: "utf8 rule annotations",
			rg: rulefmt.RuleGroup{
				Name: "test",
				Rules: []rulefmt.Rule{
					{
						Record: "valid",
						Annotations: map[string]string{
							"invalid😀": "test",
						},
					},
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := c.SetRuleGroup(tc.rg, namespace)
			require.EqualError(t, err, `unexpected status code: 400`)
		})
	}

}
