// SPDX-License-Identifier: AGPL-3.0-only

package readcache

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestPartitionEpochDir(t *testing.T) {
	// Epoch 0 keeps the legacy layout so the common single-epoch case
	// is unchanged; later epochs get a suffix to avoid colliding with
	// a still-open frozen epoch of the same partition.
	assert.Equal(t, filepath.Join("/data", "tenant-1", "partition-3"),
		partitionEpochDir("/data", "tenant-1", 3, 0))
	assert.Equal(t, filepath.Join("/data", "tenant-1", "partition-3-epoch-1"),
		partitionEpochDir("/data", "tenant-1", 3, 1))
	assert.Equal(t, filepath.Join("/data", "tenant-1", "partition-3-epoch-2"),
		partitionEpochDir("/data", "tenant-1", 3, 2))
}

// TestReadcache_FreezeKeepsSliceQueryableThenReaps is the storage-side
// counterpart of the distributor's interval fan-out: after a partition
// moves away, the previous owner must keep serving the slice it
// ingested until the slice ages out of the readcache horizon.
func TestReadcache_FreezeKeepsSliceQueryableThenReaps(t *testing.T) {
	const tenantID = "tenant-1"
	const pid = int32(3)

	cfg := newTestConfig(t, false, 0)
	cfg.LocalBlockRetention = time.Hour
	limits := validation.NewOverrides(validation.Limits{}, nil)

	r := &Readcache{
		logger:     log.NewNopLogger(),
		cfg:        cfg,
		limits:     limits,
		partitions: map[int32]*partitionState{},
		frozen:     map[int32][]*frozenEpoch{},
		epochSeq:   map[int32]int{},
	}

	// Stand up a live partition with one committed sample two minutes
	// in the past, then publish it into r.partitions as epoch 0.
	db, err := openPartitionTSDB(tenantID, pid, 0, cfg.DataDir, cfg.BlocksStorage.TSDB,
		cfg.LocalBlockRetention, limits, 0, nil, nil, nil, prometheus.NewRegistry(), log.NewNopLogger())
	require.NoError(t, err)

	sampleTS := time.Now().Add(-2 * time.Minute).UnixMilli()
	app := db.Appender(context.Background())
	_, err = app.Append(0, labels.FromStrings(labels.MetricName, "up"), sampleTS, 1)
	require.NoError(t, err)
	require.NoError(t, app.Commit())

	p := newPartitionState(pid)
	p.tenants[tenantID] = db
	r.partitions[pid] = p

	hint := &client.QueryAttributionHint{PartitionId: pid}

	// Freeze (the partition moved to another readcache). The caller
	// (removePartition) removes it from the live map first.
	r.partitionMu.Lock()
	delete(r.partitions, pid)
	r.partitionMu.Unlock()
	require.NoError(t, r.freezePartition(pid, p))

	// The slice is no longer a live partition but is still queryable
	// from the frozen epoch.
	dbs, err := r.listTSDBsForTenant(tenantID, hint)
	require.NoError(t, err)
	require.Len(t, dbs, 1, "frozen epoch must stay queryable after the partition moved away")
	require.Same(t, db, dbs[0])

	// A reap at "now" keeps it: its newest sample is well within
	// LocalBlockRetention.
	r.reapFrozenEpochs(time.Now())
	dbs, err = r.listTSDBsForTenant(tenantID, hint)
	require.NoError(t, err)
	require.Len(t, dbs, 1, "epoch within retention must be kept")

	// Advancing the clock past maxT + retention + grace reaps it.
	r.reapFrozenEpochs(time.Now().Add(cfg.LocalBlockRetention + frozenEpochReapGrace + 10*time.Minute))
	dbs, err = r.listTSDBsForTenant(tenantID, hint)
	require.NoError(t, err)
	require.Empty(t, dbs, "aged-out frozen epoch must be reaped")
}
