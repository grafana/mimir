// SPDX-License-Identifier: AGPL-3.0-only
//go:build requires_docker

package integration

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/grafana/dskit/tenant"
	"github.com/grafana/e2e"
	e2edb "github.com/grafana/e2e/db"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/protobuf/proto"

	"github.com/grafana/mimir/integration/e2emimir"
)

const metadataKeySource = "source"

// TestTenantWithMetadataLimits tests that tenants providing metadata can have
// their own limits that are independent of, but inherit from the main tenant's
// limits. This enables tenants providing metadata to have ingestion spikes
// without affecting the main tenant's quotas.
//
// The orgID format is: tenantID:key=value (e.g., "tenant:test-run=123456")
//
// There are three cases to consider:
// - no metadata, should use the regular limit (orgID="tenant")
// - tenant ID and metadata "placeholder" override match, but there's no explicit override (orgID="tenant:test-run=789")
// - tenant ID and explicit metadata override (orgID="tenant:test-run=123456")
//
// This tests ensures that we can write at least the limit defined in overrides
// and that writes after the limit are rejected. Because usage-tracker works
// asynchronously we permit up to 10% written series beyond the defined limit.
// The tests also ensures that OTEL related limits are correctly merged and
// applied by distributors.
func TestTenantWithMetadataLimits(t *testing.T) {
	const (
		mainTenantID = "tenant"
		testSource   = "foo"
		testRunKey   = "test-run"
		knownTestID  = "123456"

		// Main tenant has a low series limit
		mainTenantSeriesLimit = 50
		// Default limit for any test-run (used for unknown test IDs)
		testRunDefaultSeriesLimit = 100
		// Specific limit for the known test ID
		testRunSpecificSeriesLimit = 200
	)

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	md := tenant.NewMetadata()
	md.Set(metadataKeySource, testSource)
	sourcePlaceholderKey := md.WithTenant("")

	md.Set(testRunKey, knownTestID)
	knownTestKey := md.WithTenant(mainTenantID)

	runtimeConfig := fmt.Sprintf(`
overrides:
  %q:
    max_active_series_per_user: %d
    ingestion_burst_size: 123
    name_validation_scheme: legacy
    otel_metric_suffixes_enabled: true
    otel_translation_strategy: UnderscoreEscapingWithSuffixes
  %q:
    max_active_series_per_user: %d
    name_validation_scheme: legacy
    otel_metric_suffixes_enabled: false
    otel_translation_strategy: UnderscoreEscapingWithoutSuffixes
  %q:
    ingestion_rate: 456
    max_active_series_per_user: %d
`, mainTenantID, mainTenantSeriesLimit,
		sourcePlaceholderKey, testRunDefaultSeriesLimit,
		knownTestKey, testRunSpecificSeriesLimit,
	)

	require.NoError(t, writeFileToSharedDir(s, "runtime.yaml", []byte(runtimeConfig)))

	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, blocksBucketName)
	kafka := e2edb.NewKafka()
	require.NoError(t, s.StartAndWaitReady(consul, minio, kafka))

	baseFlags := mergeFlags(
		BlocksStorageFlags(),
		BlocksStorageS3Flags(),
		map[string]string{
			"-runtime-config.file":          filepath.Join(e2e.ContainerSharedDir, "runtime.yaml"),
			"-runtime-config.reload-period": "100ms",
		},
	)

	usageTrackerFlags := mergeFlags(baseFlags, map[string]string{
		"-usage-tracker.enabled":                                                        "true",
		"-usage-tracker.partitions":                                                     "4",
		"-usage-tracker.instance-ring.store":                                            "consul",
		"-usage-tracker.instance-ring.consul.hostname":                                  consul.NetworkHTTPEndpoint(),
		"-usage-tracker.partition-ring.store":                                           "consul",
		"-usage-tracker.partition-ring.consul.hostname":                                 consul.NetworkHTTPEndpoint(),
		"-usage-tracker.events-storage.writer.address":                                  kafka.NetworkEndpoint(9092),
		"-usage-tracker.events-storage.reader.address":                                  kafka.NetworkEndpoint(9092),
		"-usage-tracker.events-storage.writer.topic":                                    "usage-tracker-events",
		"-usage-tracker.events-storage.reader.topic":                                    "usage-tracker-events",
		"-usage-tracker.snapshots-metadata.writer.address":                              kafka.NetworkEndpoint(9092),
		"-usage-tracker.snapshots-metadata.reader.address":                              kafka.NetworkEndpoint(9092),
		"-usage-tracker.snapshots-metadata.writer.topic":                                "usage-tracker-snapshots",
		"-usage-tracker.snapshots-metadata.reader.topic":                                "usage-tracker-snapshots",
		"-usage-tracker.snapshots-storage.backend":                                      "filesystem",
		"-usage-tracker.snapshots-storage.filesystem.dir":                               filepath.Join(e2e.ContainerSharedDir, "usage-tracker-snapshots"),
		"-usage-tracker.partition-reconcile-interval":                                   "1s",
		"-usage-tracker.max-partitions-to-create-per-reconcile":                         "4",
		"-usage-tracker.events-storage.writer.auto-create-topic-enabled":                "true",
		"-usage-tracker.events-storage.writer.auto-create-topic-default-partitions":     "4",
		"-usage-tracker.snapshots-metadata.writer.auto-create-topic-enabled":            "true",
		"-usage-tracker.snapshots-metadata.writer.auto-create-topic-default-partitions": "4",
	})
	usageTracker := e2emimir.NewSingleBinary("usage-tracker-0", mergeFlags(usageTrackerFlags, map[string]string{
		"-target": "usage-tracker",
		"-usage-tracker.instance-ring.instance-id":   "usage-tracker-0",
		"-usage-tracker.instance-ring.instance-addr": "usage-tracker-0",
	}))
	require.NoError(t, s.StartAndWaitReady(usageTracker))
	require.NoError(t, usageTracker.WaitSumMetricsWithOptions(e2e.Equals(4), []string{"cortex_partition_ring_partitions"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "usage-tracker-partitions"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "Active"))))

	distributorFlags := mergeFlags(baseFlags, map[string]string{
		"-usage-tracker.enabled":                        "true",
		"-usage-tracker.instance-ring.store":            "consul",
		"-usage-tracker.instance-ring.consul.hostname":  consul.NetworkHTTPEndpoint(),
		"-usage-tracker.partition-ring.store":           "consul",
		"-usage-tracker.partition-ring.consul.hostname": consul.NetworkHTTPEndpoint(),
	})

	distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), distributorFlags)
	ingester := e2emimir.NewIngester("ingester-1", consul.NetworkHTTPEndpoint(), baseFlags)
	querier := e2emimir.NewQuerier("querier-1", consul.NetworkHTTPEndpoint(), baseFlags)
	require.NoError(t, s.StartAndWaitReady(distributor, ingester, querier))

	require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	require.NoError(t, querier.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "usage-tracker-instances"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(4), []string{"cortex_partition_ring_partitions"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "usage-tracker-partitions"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "Active"))))

	// Wait for runtime config to be loaded by all components
	require.NoError(t, distributor.WaitSumMetricsWithOptions(
		e2e.Greater(0),
		[]string{"cortex_runtime_config_hash"},
		e2e.WaitMissingMetrics,
	))
	require.NoError(t, usageTracker.WaitSumMetricsWithOptions(
		e2e.Greater(0),
		[]string{"cortex_runtime_config_hash"},
		e2e.WaitMissingMetrics,
	))

	t.Run("main tenant uses default tenant limit", func(t *testing.T) {
		t.Logf("using tenant ID %q", mainTenantID)
		client, err := e2emimir.NewClient(distributor.HTTPEndpoint(), "", "", "", mainTenantID)
		require.NoError(t, err)
		writtenAfterLimit := pushSeriesUntilLimitOTLP(t, client, "main_tenant_metric", mainTenantSeriesLimit)
		t.Logf("wrote %d series after limit", writtenAfterLimit)
		require.Less(t, writtenAfterLimit, mainTenantSeriesLimit/10, "should not be able to write beyond the limit")
	})

	t.Run("unknown source uses default tenant limit", func(t *testing.T) {
		md.Set(metadataKeySource, "unknown-src")
		md.Set(testRunKey, knownTestID)
		orgID := md.WithTenant(mainTenantID)
		t.Logf("using tenant ID %q", orgID)
		client, err := e2emimir.NewClient(distributor.HTTPEndpoint(), "", "", "", orgID)
		require.NoError(t, err)
		writtenAfterLimit := pushSeriesUntilLimitOTLP(t, client, "unknown_test_metric", testRunDefaultSeriesLimit)
		t.Logf("wrote %d series after limit", writtenAfterLimit)
		require.Less(t, writtenAfterLimit, mainTenantSeriesLimit/10, "should not be able to write beyond the limit")
	})

	t.Run("unknown test ID uses default test-run limit", func(t *testing.T) {
		md.Set(metadataKeySource, testSource)
		md.Set(testRunKey, "unknown-789")
		orgID := md.WithTenant(mainTenantID)
		t.Logf("using tenant ID %q", orgID)
		client, err := e2emimir.NewClient(distributor.HTTPEndpoint(), "", "", "", orgID)
		require.NoError(t, err)
		writtenAfterLimit := pushSeriesUntilLimitOTLP(t, client, "unknown_test_metric", testRunDefaultSeriesLimit)
		t.Logf("wrote %d series after limit", writtenAfterLimit)
		require.Less(t, writtenAfterLimit, testRunDefaultSeriesLimit/10, "should not be able to write beyond the limit")
	})

	t.Run("known test ID uses exact specified limit", func(t *testing.T) {
		md.Set(metadataKeySource, testSource)
		md.Set(testRunKey, knownTestID)
		orgID := md.WithTenant(mainTenantID)
		t.Logf("using tenant ID %q", orgID)
		client, err := e2emimir.NewClient(distributor.HTTPEndpoint(), "", "", "", orgID)
		require.NoError(t, err)
		writtenAfterLimit := pushSeriesUntilLimitOTLP(t, client, "known_test_metric", testRunSpecificSeriesLimit)
		t.Logf("wrote %d series after limit", writtenAfterLimit)
		require.Less(t, writtenAfterLimit, testRunSpecificSeriesLimit/10, "should not be able to write beyond the limit")
	})

	t.Run("OTLP metric suffixes are disabled for source=foo", func(t *testing.T) {
		queryClient, err := e2emimir.NewClient(distributor.HTTPEndpoint(), querier.HTTPEndpoint(), "", "", mainTenantID)
		require.NoError(t, err)

		result, err := queryClient.Query("known_test_metric_0", time.Now())
		require.NoError(t, err)
		require.Equal(t, model.ValVector, result.Type())
		require.NotEmpty(t, result.(model.Vector), "expected results for known_test_metric_0")

		result, err = queryClient.Query("known_test_metric_0_bytes", time.Now())
		require.NoError(t, err)
		require.Equal(t, model.ValVector, result.Type())
		require.Empty(t, result.(model.Vector), "expected no results for known_test_metric_0_bytes (suffixes should be disabled)")
	})
}

// pushSeriesUntilLimitOTLP pushes OTLP counter metrics to the given client until
// the limit is hit. Each metric is a cumulative monotonic sum with unit "By" (bytes).
// It pushes `limit` series expecting HTTP 200, waits 3s for async tracking to
// complete, then verifies that pushing more series is rejected with HTTP 429.
// Returns the number of series that could be pushed after writing limit series.
func pushSeriesUntilLimitOTLP(t *testing.T, client *e2emimir.Client, metricPrefix string, limit int) int {
	t.Helper()
	now := time.Now()
	nowUnix := uint64(now.UnixNano())
	startUnix := uint64(now.Add(-time.Minute).UnixNano())

	for i := 0; i < limit; i++ {
		payload := marshalOTLPCounter(t, fmt.Sprintf("%s_%d", metricPrefix, i), float64(i), startUnix, nowUnix)
		res, _, err := client.PushOTLPPayload(payload, "application/x-protobuf")
		require.NoError(t, err)
		require.Equal(t, 200, res.StatusCode, "series %d should be accepted", i)
	}

	time.Sleep(3 * time.Second)

	const maxAttempts = 30
	for attempt := 0; attempt < maxAttempts; attempt++ {
		ts := time.Now()
		payload := marshalOTLPCounter(t, fmt.Sprintf("%s_over_limit_%d", metricPrefix, attempt), 999, uint64(ts.Add(-time.Minute).UnixNano()), uint64(ts.UnixNano()))
		res, _, err := client.PushOTLPPayload(payload, "application/x-protobuf")
		require.NoError(t, err)
		if res.StatusCode == 429 {
			return attempt
		}
		time.Sleep(time.Second)
	}
	return maxAttempts
}

func marshalOTLPCounter(t *testing.T, name string, value float64, startUnixNano, timeUnixNano uint64) []byte {
	t.Helper()
	req := &metricspb.MetricsData{
		ResourceMetrics: []*metricspb.ResourceMetrics{{
			ScopeMetrics: []*metricspb.ScopeMetrics{{
				Metrics: []*metricspb.Metric{{
					Name: name,
					Unit: "By",
					Data: &metricspb.Metric_Sum{
						Sum: &metricspb.Sum{
							AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
							IsMonotonic:            true,
							DataPoints: []*metricspb.NumberDataPoint{{
								Value:             &metricspb.NumberDataPoint_AsDouble{AsDouble: value},
								StartTimeUnixNano: startUnixNano,
								TimeUnixNano:      timeUnixNano,
							}},
						},
					},
				}},
			}},
		}},
	}
	payload, err := proto.Marshal(req)
	require.NoError(t, err)
	return payload
}
