// SPDX-License-Identifier: AGPL-3.0-only

package readcache

import (
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/util/validation"
)

func TestReadcacheTSDBCollectorIncludesActiveAndFrozenStores(t *testing.T) {
	cfg := newTestConfig(t, false, 0)
	limits := validation.NewOverrides(validation.Limits{}, nil)

	active, err := openPartitionTSDB(
		"active-tenant", 1, 0, cfg.DataDir, cfg.BlocksStorage.TSDB,
		cfg.LocalBlockRetention, limits, 0, nil, nil, nil, newTestLookupPlanMetrics(),
		prometheus.NewRegistry(), log.NewNopLogger(),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, active.Close()) })

	frozen, err := openPartitionTSDB(
		"frozen-tenant", 2, 0, cfg.DataDir, cfg.BlocksStorage.TSDB,
		cfg.LocalBlockRetention, limits, 0, nil, nil, nil, newTestLookupPlanMetrics(),
		prometheus.NewRegistry(), log.NewNopLogger(),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, frozen.Close()) })

	appendOneSeries := func(db *partitionTSDB, metric string) {
		t.Helper()
		app := db.Appender(t.Context())
		_, err := app.Append(0, labels.FromStrings("__name__", metric), 1, 1)
		require.NoError(t, err)
		require.NoError(t, app.Commit())
	}
	appendOneSeries(active, "active_metric")
	appendOneSeries(frozen, "frozen_metric")

	activePartition := newPartitionState(1)
	activePartition.tenants["active-tenant"] = active
	r := &Readcache{
		partitions: map[int32]*partitionState{1: activePartition},
		frozen: map[int32][]*frozenEpoch{
			2: {{partitionID: 2, tenants: map[string]*partitionTSDB{"frozen-tenant": frozen}}},
		},
	}

	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(newReadcacheTSDBCollector(r))
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
# HELP cortex_readcache_managed_tsdb_head_series Number of in-memory series across physical TSDBs managed by this readcache.
# TYPE cortex_readcache_managed_tsdb_head_series gauge
cortex_readcache_managed_tsdb_head_series{state="active"} 1
cortex_readcache_managed_tsdb_head_series{state="frozen"} 1
# HELP cortex_readcache_managed_tsdbs Number of physical TSDBs managed by this readcache, including active and frozen stores.
# TYPE cortex_readcache_managed_tsdbs gauge
cortex_readcache_managed_tsdbs{state="active"} 1
cortex_readcache_managed_tsdbs{state="frozen"} 1
`),
		"cortex_readcache_managed_tsdb_head_series",
		"cortex_readcache_managed_tsdbs",
	))
}

func TestReadcacheTSDBCollectorDoesNotDoubleCountStoreDuringFreeze(t *testing.T) {
	cfg := newTestConfig(t, false, 0)
	limits := validation.NewOverrides(validation.Limits{}, nil)
	db, err := openPartitionTSDB(
		"tenant", 1, 0, cfg.DataDir, cfg.BlocksStorage.TSDB,
		cfg.LocalBlockRetention, limits, 0, nil, nil, nil, newTestLookupPlanMetrics(),
		prometheus.NewRegistry(), log.NewNopLogger(),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	partition := newPartitionState(1)
	partition.tenants["tenant"] = db
	r := &Readcache{
		partitions: map[int32]*partitionState{1: partition},
		frozen: map[int32][]*frozenEpoch{
			1: {{partitionID: 1, tenants: map[string]*partitionTSDB{"tenant": db}}},
		},
	}

	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(newReadcacheTSDBCollector(r))
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
# HELP cortex_readcache_managed_tsdbs Number of physical TSDBs managed by this readcache, including active and frozen stores.
# TYPE cortex_readcache_managed_tsdbs gauge
cortex_readcache_managed_tsdbs{state="active"} 0
cortex_readcache_managed_tsdbs{state="frozen"} 1
`), "cortex_readcache_managed_tsdbs"))
}

func TestPartitionTSDBMutationMetrics(t *testing.T) {
	cfg := newTestConfig(t, false, 0)
	limits := validation.NewOverrides(validation.Limits{}, nil)
	db, err := openPartitionTSDB(
		"tenant", 1, 0, cfg.DataDir, cfg.BlocksStorage.TSDB,
		cfg.LocalBlockRetention, limits, 0, nil, nil, nil, newTestLookupPlanMetrics(),
		prometheus.NewRegistry(), log.NewNopLogger(),
	)
	require.NoError(t, err)

	reg := prometheus.NewPedanticRegistry()
	wait := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "test_mutation_wait_seconds",
		Help: "Test mutation wait.",
	}, []string{"operation"})
	hold := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "test_mutation_hold_seconds",
		Help: "Test mutation hold.",
	}, []string{"operation"})
	reg.MustRegister(wait, hold)
	for operation := tsdbMutationOperation(0); operation < tsdbMutationOperationCount; operation++ {
		db.mutationObservers[operation] = tsdbMutationObservers{
			wait: wait.WithLabelValues(operation.String()),
			hold: hold.WithLabelValues(operation.String()),
		}
	}

	unlock := db.lockForMutation(tsdbMutationAppend)
	unlock()
	require.NoError(t, db.Close())

	require.Equal(t, uint64(1), histogramSampleCount(t, wait.WithLabelValues("append")))
	require.Equal(t, uint64(1), histogramSampleCount(t, hold.WithLabelValues("append")))
	require.Equal(t, uint64(1), histogramSampleCount(t, wait.WithLabelValues("close")))
	require.Equal(t, uint64(1), histogramSampleCount(t, hold.WithLabelValues("close")))
}

func histogramSampleCount(t *testing.T, observer prometheus.Observer) uint64 {
	t.Helper()
	metric, ok := observer.(prometheus.Metric)
	require.True(t, ok)
	var value dto.Metric
	require.NoError(t, metric.Write(&value))
	return value.GetHistogram().GetSampleCount()
}
