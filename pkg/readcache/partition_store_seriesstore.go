// SPDX-License-Identifier: AGPL-3.0-only

package readcache

import (
	"context"
	"time"

	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/seriesstore/store"
)

var _ partitionHead = (*store.Engine)(nil)

// Store shards of a partition's seriesstore engine. A partition holds whole metrics of a tenant, and a push the tenant's
// series of a few metrics, so a small tenant's shards would stay mostly empty: it gets few of them, a large one enough for
// the Kafka reader's parallel appends not to wait on each other's commits.
const (
	seriesstoreSmallTenantShards = 4
	seriesstoreLargeTenantShards = 16
	seriesstoreLargeTenantSeries = 300_000
)

func seriesstoreShardsFor(maxSeriesPerTenant int) int {
	if maxSeriesPerTenant == 0 || maxSeriesPerTenant >= seriesstoreLargeTenantSeries {
		return seriesstoreLargeTenantShards
	}
	return seriesstoreSmallTenantShards
}

// seriesstoreStore is a seriesstore engine. It keeps no write-ahead log: a clean close snapshots its head, and the
// readcache checkpoints it while ingesting, rewinding the partition's Kafka offset to the oldest checkpoint after a crash.
type seriesstoreStore struct {
	*store.Engine
}

type seriesstoreStoreOptions struct {
	shards                 int
	retentionMs            int64
	outOfOrderTimeWindowMs int64
	maxExemplars           int64
	timelyCompaction       bool
	idleEvictionMs         int64
	callback               tsdb.SeriesLifecycleCallback
}

func openSeriesstoreStore(dir, tenantID string, opts seriesstoreStoreOptions) (seriesstoreStore, error) {
	engine, err := store.OpenEngine(dir, tenantID, store.EngineOptions{
		Shards:                  opts.shards,
		RetentionMs:             opts.retentionMs,
		OutOfOrderTimeWindowMs:  opts.outOfOrderTimeWindowMs,
		MaxExemplars:            opts.maxExemplars,
		TimelyCompaction:        opts.timelyCompaction,
		IdleEvictionMs:          opts.idleEvictionMs,
		Checkpoints:             true,
		SeriesLifecycleCallback: opts.callback,
		// The hash ranges the rebalancer assigns are of the metric name locality hash, which the series statistics walk.
		SecondaryHashFunction: mimirpb.ShardByMetricNameLocalityLabelsFunc(tenantID),
	})
	if err != nil {
		return seriesstoreStore{}, err
	}
	return seriesstoreStore{Engine: engine}, nil
}

func (s seriesstoreStore) Appender(ctx context.Context) storage.Appender {
	return s.Engine.Appender(ctx)
}

func (s seriesstoreStore) Head() partitionHead {
	return s.Engine
}

func (s seriesstoreStore) Blocks() []*tsdb.Block {
	return nil
}

func (s seriesstoreStore) CompactHeadRange(mint, maxt int64) error {
	return s.Engine.CompactHead(mint, maxt)
}

func (s seriesstoreStore) ApplySettings(outOfOrderTimeWindowMs, maxExemplars int64) error {
	s.Engine.SetOutOfOrderTimeWindow(outOfOrderTimeWindowMs)
	s.Engine.SetMaxExemplars(maxExemplars)
	return nil
}

// seriesstoreIdleEviction is how long a partition's seriesstore keeps series that stopped in memory: like the
// ingester's, they leave the head at a compaction, with their data still queryable.
const seriesstoreIdleEviction = time.Hour
