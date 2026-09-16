// SPDX-License-Identifier: AGPL-3.0-only

package readcache

import (
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester/lookupplan"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestPartitionTSDB_IndexLookupPlanning(t *testing.T) {
	const (
		tenantID    = "tenant-a"
		partitionID = int32(7)
	)

	cfg := newTestConfig(t, false, 0)
	cfg.BlocksStorage.TSDB.IndexLookupPlanning.Enabled = true
	cfg.BlocksStorage.TSDB.IndexLookupPlanning.MinSeriesPerBlockForQueryPlanning = 1

	reg := prometheus.NewPedanticRegistry()
	r, err := New(cfg, validation.NewOverrides(validation.Limits{}, nil), nil, log.NewNopLogger(), reg)
	require.NoError(t, err)

	p := newPartitionState(partitionID)
	p.warm.Store(true)
	r.partitions[partitionID] = p

	db, err := r.getOrOpenTSDB(tenantID, partitionID)
	require.NoError(t, err)
	require.NotNil(t, db)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	// Opening an empty TSDB generates and caches a no-op Head planner.
	indexReader, err := db.Head().Index()
	require.NoError(t, err)
	assert.IsType(t, lookupplan.NoopPlanner{}, indexReader.IndexLookupPlanner())
	require.NoError(t, indexReader.Close())

	appendSeries(t, db,
		labels.FromStrings(labels.MetricName, "target", "instance", "one"),
		labels.FromStrings(labels.MetricName, "target", "instance", "two"),
	)
	r.generateHeadStatistics()

	indexReader, err = db.Head().Index()
	require.NoError(t, err)
	assert.IsType(t, &lookupplan.CostBasedPlanner{}, indexReader.IndexLookupPlanner())
	require.NoError(t, indexReader.Close())

	target := labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "target")
	require.Len(t, selectChunkSeries(t, db, target), 2)

	families, err := reg.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() == "cortex_readcache_lookup_planning_duration_seconds" {
			require.Len(t, family.Metric, 1)
			assert.Greater(t, family.Metric[0].GetHistogram().GetSampleCount(), uint64(0))
			return
		}
	}
	t.Fatal("cortex_readcache_lookup_planning_duration_seconds not found")
}
