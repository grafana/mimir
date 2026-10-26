// SPDX-License-Identifier: AGPL-3.0-only

//go:build !native_resources_ablation

package ingester

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest/kmeta"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/util/validation"
)

// The native_resources_ablation build keeps resources in #17740's store, which has no limits.

// The seriesstore engine coalesces a series' resource changes over the limits, and counts them as discarded metadata.
func TestSeriesstoreCountsResourcesOverTheLimits(t *testing.T) {
	skipIfSeriesstore(t, "it runs the seriesstore engine itself")
	limits := defaultLimitsTestConfig()
	limits.NativeResourcesEnabled = true
	limits.NativeResourcesMaxIdentityChanges = 1
	limits.NativeResourcesMaxVersionsPerIdentity = 1
	cfg := defaultIngesterTestConfig(t)
	cfg.IngestStorageConfig.KafkaConfig.ConsumeFromPositionAtStartup = "start"
	cfg.BlocksStorageConfig.TSDB.Engine = mimir_tsdb.EngineSeriesstore
	reg := prometheus.NewPedanticRegistry()
	ingester, _, _ := createTestIngesterWithIngestStorage(t, &cfg, validation.NewOverrides(limits, nil), nil, reg, nil)

	// In one window, the first series' identity changes three times, and the second series' resource's host does.
	base := time.Now().Truncate(time.Hour).Add(-2 * time.Hour).UnixMilli()
	write, last := compatProducer(t, cfg.IngestStorageConfig.KafkaConfig)
	for step, change := range []string{"a", "b", "c", "d"} {
		first := compatSeries("requests_total", mimirpb.Sample{TimestampMs: base + int64(step)*60_000, Value: 1})
		first.ResourceRef = 1
		second := compatSeries("errors_total", mimirpb.Sample{TimestampMs: base + int64(step)*60_000, Value: 1})
		second.ResourceRef = 2
		write("tenant", &mimirpb.WriteRequest{
			Timeseries:    []mimirpb.PreallocTimeseries{first, second},
			ResourceTable: []mimirpb.ResourceEntry{testResourceEntry(change, "node-1"), testResourceEntry("backend", "node-"+change)},
		})
	}
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), ingester))
	t.Cleanup(func() { require.NoError(t, services.StopAndAwaitTerminated(context.Background(), ingester)) })
	waitCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	require.NoError(t, ingester.ingestReader.WaitReadConsistencyUntilOffsets(waitCtx, kmeta.NewSingleClusterPartitionOffsets(last())))

	result, err := ingester.getTSDB("tenant").nativeResources.nativeResources(context.Background(), [][]*labels.Matcher{{labels.MustNewMatcher(labels.MatchRegexp, model.MetricNameLabel, ".+")}}, math.MinInt64, math.MaxInt64, 0)
	require.NoError(t, err)
	require.Len(t, result.Series, 2)
	for _, series := range result.Series {
		require.True(t, series.Coalesced, series.Labels)
	}
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_discarded_metadata_total The total number of metadata that were discarded.
		# TYPE cortex_discarded_metadata_total counter
		cortex_discarded_metadata_total{reason="per_identity_resource_version_limit",user="tenant"} 1
		cortex_discarded_metadata_total{reason="per_series_metadata_change_limit",user="tenant"} 0
		cortex_discarded_metadata_total{reason="per_series_resource_identity_change_limit",user="tenant"} 1
	`), "cortex_discarded_metadata_total"))
}
