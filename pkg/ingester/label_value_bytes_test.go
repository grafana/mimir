// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
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
		lbls := labels.FromStrings(labels.MetricName, metric, labelName, value)
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
		// With a single ingester the local limit equals the global one, so the reporting
		// threshold is 1000 bytes and the limit itself is 2000.
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

	values := make([]string, 0, 11)
	for n := range cap(values) {
		values = append(values, labelValueOfLength(strconv.Itoa(n), valueLength))
	}

	// Values at or below the minimum length never count, however many of them there are.
	short := make([]string, 0, 200)
	for n := range cap(short) {
		short = append(short, labelValueOfLength("s"+strconv.Itoa(n), index.LabelValueBytesMinLength))
	}
	pushSeriesWithLabelValues(t, i, ctx, "quiet", "short", short...)
	i.updateLimitMetrics()
	require.Equal(t, 0, testutil.CollectAndCount(registry, "cortex_ingester_label_value_bytes"))

	// Four long values are 800 bytes, still under the 1000-byte reporting threshold.
	pushSeriesWithLabelValues(t, i, ctx, "noisy", "big", values[:4]...)
	i.updateLimitMetrics()
	require.Equal(t, 0, testutil.CollectAndCount(registry, "cortex_ingester_label_value_bytes"))
	requireLabelNamesOverLimit(t, registry, 0)

	// Two more cross the reporting threshold while staying under the limit.
	pushSeriesWithLabelValues(t, i, ctx, "noisy", "big", values[4:6]...)
	i.updateLimitMetrics()
	requireLabelValueBytes(t, registry, `cortex_ingester_label_value_bytes{label="big",user="test"} 1200`)
	requireLabelNamesOverLimit(t, registry, 0)
	require.Equal(t, 0, testutil.CollectAndCount(registry, "cortex_ingester_label_value_bytes_over_limit"))

	// Repeating an existing value must not move the counter: values are counted once.
	pushSeriesWithLabelValues(t, i, ctx, "noisy_again", "big", values[0])
	i.updateLimitMetrics()
	requireLabelValueBytes(t, registry, `cortex_ingester_label_value_bytes{label="big",user="test"} 1200`)

	// Five more distinct values push the label over the limit.
	pushSeriesWithLabelValues(t, i, ctx, "noisy", "big", values[6:11]...)
	i.updateLimitMetrics()
	requireLabelValueBytes(t, registry, `cortex_ingester_label_value_bytes{label="big",user="test"} 2200`)
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

	require.Equal(t, 0, testutil.CollectAndCount(registry, "cortex_ingester_label_value_bytes"))
	require.Equal(t, 0, testutil.CollectAndCount(registry, "cortex_ingester_label_value_bytes_over_limit"))
}

func requireLabelValueBytes(t *testing.T, g prometheus.Gatherer, expected string) {
	t.Helper()

	require.NoError(t, testutil.GatherAndCompare(g, strings.NewReader(`
		# HELP cortex_ingester_label_value_bytes Total size in bytes of the distinct values held in memory for a label name, counting each distinct value once. Only reported for label names approaching or exceeding the per-label-name limit.
		# TYPE cortex_ingester_label_value_bytes gauge
		`+expected+`
	`), "cortex_ingester_label_value_bytes"))
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
		# HELP cortex_ingester_label_value_bytes_over_limit Set to 1 for each (user, label name) pair whose distinct label values currently exceed the local per-label-name bytes limit on this ingester. Absent otherwise.
		# TYPE cortex_ingester_label_value_bytes_over_limit gauge
		`+expected+`
	`), "cortex_ingester_label_value_bytes_over_limit"))
}
