// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
)

// labelValueOfLength builds a distinct value of exactly the given length.
func labelValueOfLength(prefix string, length int) string {
	return prefix + strings.Repeat("x", length-len(prefix))
}

// pushSeriesWithLabelValues pushes one series per value, all sharing the same label name.
func pushSeriesWithLabelValues(t *testing.T, i *Ingester, ctx context.Context, metric, labelName string, values ...string) {
	t.Helper()

	for _, value := range values {
		lbls := labels.FromStrings(model.MetricNameLabel, metric, labelName, value)
		req := mimirpb.ToWriteRequest(
			[][]mimirpb.LabelAdapter{mimirpb.FromLabelsToLabelAdapters(lbls)},
			[]mimirpb.Sample{{Value: 1, TimestampMs: 1_000}},
			nil, nil, mimirpb.API,
		)
		require.NoError(t, i.PushWithCleanup(ctx, req, func() { req.FreeBuffer() }))
	}
}

func TestIngester_LabelValueBytesMetrics(t *testing.T) {
	const (
		userID = "test"
		// With a single ingester the local limit equals the global one.
		globalLimit = 2000
		valueLength = 200
	)

	registry := prometheus.NewRegistry()
	cfg := defaultIngesterTestConfig(t)
	cfg.IngesterRing.ReplicationFactor = 1
	limits := defaultLimitsTestConfig()
	limits.MaxGlobalLabelValueBytesPerLabelName = globalLimit

	i, r, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, limits, nil, "", registry)
	require.NoError(t, err)
	startAndWaitHealthy(t, i, r)

	ctx := user.InjectOrgID(context.Background(), userID)

	values := make([]string, 0, 10)
	for n := range cap(values) {
		values = append(values, labelValueOfLength(strconv.Itoa(n), valueLength))
	}

	// Values at or below the minimum length never count: these would be far over the limit otherwise.
	short := make([]string, 0, 200)
	for n := range cap(short) {
		short = append(short, labelValueOfLength("s"+strconv.Itoa(n), index.LabelValueBytesMinLength))
	}
	pushSeriesWithLabelValues(t, i, ctx, "quiet", "short", short...)
	i.updateLimitMetrics()
	requireLabelNamesOverLimit(t, registry, 0)

	// Nine distinct values are 1800 bytes, under the limit.
	pushSeriesWithLabelValues(t, i, ctx, "noisy", "big", values[:9]...)
	i.updateLimitMetrics()
	requireLabelNamesOverLimit(t, registry, 0)
	require.Equal(t, 0, testutil.CollectAndCount(registry, "cortex_ingester_label_value_bytes_over_limit"))

	// Repeating an existing value on another series must not count it again, or it would reach the limit.
	pushSeriesWithLabelValues(t, i, ctx, "noisy_again", "big", values[0])
	i.updateLimitMetrics()
	requireLabelNamesOverLimit(t, registry, 0)

	// A tenth distinct value reaches the limit, which counts as over it.
	pushSeriesWithLabelValues(t, i, ctx, "noisy", "big", values[9])
	i.updateLimitMetrics()
	requireLabelNamesOverLimit(t, registry, 1)
	requireLabelValueBytesOverLimit(t, registry, `cortex_ingester_label_value_bytes_over_limit{label="big",user="test"} 1`)
}

func TestIngester_LabelValueBytesMetrics_LimitDisabled(t *testing.T) {
	const userID = "test"

	registry := prometheus.NewRegistry()
	cfg := defaultIngesterTestConfig(t)
	cfg.IngesterRing.ReplicationFactor = 1
	limits := defaultLimitsTestConfig()
	require.Zero(t, limits.MaxGlobalLabelValueBytesPerLabelName, "the limit is expected to default to disabled")

	i, r, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, limits, nil, "", registry)
	require.NoError(t, err)
	startAndWaitHealthy(t, i, r)

	ctx := user.InjectOrgID(context.Background(), userID)
	pushSeriesWithLabelValues(t, i, ctx, "noisy", "big", labelValueOfLength("a", 5_000))
	i.updateLimitMetrics()

	requireLabelNamesOverLimit(t, registry, 0)
	require.Equal(t, 0, testutil.CollectAndCount(registry, "cortex_ingester_label_value_bytes_over_limit"))
}

// Metrics updates only visit open TSDBs, so a closed tenant's label names must be dropped on close.
func TestIngester_LabelValueBytesMetrics_ClearedWhenTSDBIsClosed(t *testing.T) {
	const userID = "test"

	registry := prometheus.NewRegistry()
	cfg := defaultIngesterTestConfig(t)
	cfg.IngesterRing.ReplicationFactor = 1
	cfg.BlocksStorageConfig.TSDB.CloseIdleTSDBTimeout = 0
	limits := defaultLimitsTestConfig()
	limits.MaxGlobalLabelValueBytesPerLabelName = 1000

	i, r, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, limits, nil, "", registry)
	require.NoError(t, err)
	startAndWaitHealthy(t, i, r)

	ctx := user.InjectOrgID(context.Background(), userID)
	pushSeriesWithLabelValues(t, i, ctx, "noisy", "big", labelValueOfLength("a", 600), labelValueOfLength("b", 600))
	i.updateLimitMetrics()
	require.Equal(t, 1, testutil.CollectAndCount(registry, "cortex_ingester_label_value_bytes_over_limit"))

	i.compactBlocks(context.Background(), true, math.MaxInt64, nil)
	i.shipBlocks(context.Background(), nil)
	require.Equal(t, tsdbIdleClosed, i.closeAndDeleteUserTSDBIfIdle(userID))

	require.Equal(t, 0, testutil.CollectAndCount(registry, "cortex_ingester_label_value_bytes_over_limit"))
}

func requireLabelNamesOverLimit(t *testing.T, g prometheus.Gatherer, expected int) {
	t.Helper()

	require.NoError(t, testutil.GatherAndCompare(g, strings.NewReader(`
		# HELP cortex_ingester_label_names_over_value_bytes_limit Number of (user, label name) pairs whose distinct label values exceed the local per-label-name bytes limit on this ingester.
		# TYPE cortex_ingester_label_names_over_value_bytes_limit gauge
		cortex_ingester_label_names_over_value_bytes_limit `+strconv.Itoa(expected)+`
	`), "cortex_ingester_label_names_over_value_bytes_limit"))
}

func requireLabelValueBytesOverLimit(t *testing.T, g prometheus.Gatherer, expected string) {
	t.Helper()

	require.NoError(t, testutil.GatherAndCompare(g, strings.NewReader(`
		# HELP cortex_ingester_label_value_bytes_over_limit Set to 1 for each (user, label name) pair whose distinct label values currently exceed the local per-label-name bytes limit on this ingester.
		# TYPE cortex_ingester_label_value_bytes_over_limit gauge
		`+expected+`
	`), "cortex_ingester_label_value_bytes_over_limit"))
}
