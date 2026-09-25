// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	util_test "github.com/grafana/mimir/pkg/util/test"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestIngester_DelayedSeriesAreNotAppendedToHead(t *testing.T) {
	const userID = "user-1"

	tenantLimits := defaultLimitsTestConfig()
	tenantLimits.DelayedSeries = validation.DelayedSeriesConfig{
		{Match: `{__name__="delayed_metric"}`, Except: []string{`{cluster="hot"}`}},
	}
	require.NoError(t, tenantLimits.DelayedSeries.Validate())
	overrides := validation.NewOverrides(defaultLimitsTestConfig(), validation.NewMockTenantLimits(map[string]*validation.Limits{
		userID: &tenantLimits,
	}))

	reg := prometheus.NewPedanticRegistry()
	cfg := defaultIngesterTestConfig(t)
	ing, _, _ := createTestIngesterWithIngestStorage(t, &cfg, overrides, nil, reg, util_test.NewTestingLogger(t))
	require.NoError(t, services.StartAndAwaitRunning(t.Context(), ing))
	t.Cleanup(func() {
		// t.Context() is already canceled when cleanups run.
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), ing))
	})

	series := func(lbls ...string) []mimirpb.LabelAdapter {
		out := make([]mimirpb.LabelAdapter, 0, len(lbls)/2)
		for i := 0; i < len(lbls); i += 2 {
			out = append(out, mimirpb.LabelAdapter{Name: lbls[i], Value: lbls[i+1]})
		}
		return out
	}
	now := time.Now()
	req := &mimirpb.WriteRequest{
		Timeseries: []mimirpb.PreallocTimeseries{
			{TimeSeries: &mimirpb.TimeSeries{Labels: series("__name__", "delayed_metric", "cluster", "cold-1"), Samples: []mimirpb.Sample{{TimestampMs: now.UnixMilli(), Value: 1}}}},
			{TimeSeries: &mimirpb.TimeSeries{Labels: series("__name__", "delayed_metric", "cluster", "cold-2"), Samples: []mimirpb.Sample{{TimestampMs: now.UnixMilli(), Value: 1}}}},
			{TimeSeries: &mimirpb.TimeSeries{Labels: series("__name__", "delayed_metric", "cluster", "hot"), Samples: []mimirpb.Sample{{TimestampMs: now.UnixMilli(), Value: 1}}}},
			{TimeSeries: &mimirpb.TimeSeries{Labels: series("__name__", "other_metric", "cluster", "cold-1"), Samples: []mimirpb.Sample{{TimestampMs: now.UnixMilli(), Value: 1}}}},
		},
	}
	require.NoError(t, ing.PushToStorageAndReleaseRequest(user.InjectOrgID(t.Context(), userID), req))

	db := ing.getTSDB(userID)
	require.NotNil(t, db)
	require.Equal(t, uint64(2), db.Head().NumSeries())

	ing.delayedSeries.purge(now)
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_ingester_delayed_series Number of series kept out of the TSDB head by the delayed_series limit and seen within the active series idle timeout.
		# TYPE cortex_ingester_delayed_series gauge
		cortex_ingester_delayed_series{user="user-1"} 2
		# HELP cortex_ingester_delayed_samples_total Total number of samples not appended to the TSDB head because their series matched the delayed_series limit.
		# TYPE cortex_ingester_delayed_samples_total counter
		cortex_ingester_delayed_samples_total{user="user-1"} 2
	`), "cortex_ingester_delayed_series", "cortex_ingester_delayed_samples_total"))

	ing.delayedSeries.purge(now.Add(cfg.ActiveSeriesMetrics.IdleTimeout + time.Minute))
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_ingester_delayed_samples_total Total number of samples not appended to the TSDB head because their series matched the delayed_series limit.
		# TYPE cortex_ingester_delayed_samples_total counter
		cortex_ingester_delayed_samples_total{user="user-1"} 2
	`), "cortex_ingester_delayed_series", "cortex_ingester_delayed_samples_total"))
}

func TestIngester_DelayedSeriesOnlyDelayedSeriesInRequest(t *testing.T) {
	const userID = "user-1"

	tenantLimits := defaultLimitsTestConfig()
	tenantLimits.DelayedSeries = validation.DelayedSeriesConfig{{Match: `{__name__="delayed_metric"}`}}
	require.NoError(t, tenantLimits.DelayedSeries.Validate())
	overrides := validation.NewOverrides(defaultLimitsTestConfig(), validation.NewMockTenantLimits(map[string]*validation.Limits{
		userID: &tenantLimits,
	}))

	cfg := defaultIngesterTestConfig(t)
	ing, _, _ := createTestIngesterWithIngestStorage(t, &cfg, overrides, nil, prometheus.NewPedanticRegistry(), util_test.NewTestingLogger(t))
	require.NoError(t, services.StartAndAwaitRunning(t.Context(), ing))
	t.Cleanup(func() {
		// t.Context() is already canceled when cleanups run.
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), ing))
	})

	req := &mimirpb.WriteRequest{
		Timeseries: []mimirpb.PreallocTimeseries{
			{TimeSeries: &mimirpb.TimeSeries{
				Labels:  []mimirpb.LabelAdapter{{Name: "__name__", Value: "delayed_metric"}},
				Samples: []mimirpb.Sample{{TimestampMs: time.Now().UnixMilli(), Value: 1}},
			}},
		},
	}
	require.NoError(t, ing.PushToStorageAndReleaseRequest(user.InjectOrgID(t.Context(), userID), req))
	require.Nil(t, ing.getTSDB(userID), "a request with only delayed series must not create a TSDB")
}
