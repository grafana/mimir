// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/go-kit/log/level"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"

	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/storage/seriesstore/store"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
)

// seriesstoreEngine is pkg/storage/seriesstore as a tenantEngine. It writes no Prometheus blocks:
// series leaving the head go to its own cold blocks, which it queries and expires itself.
type seriesstoreEngine struct {
	*store.Engine
	// Head compactions that moved the head's min time, which is when Prometheus writes a block.
	compactions prometheus.Counter
}

// seriesstoreShards is how many store shards a tenant's series spread over: appends to a tenant
// come from the pusher's parallel shards, and each store shard has its own lock.
const seriesstoreShards = 16

func openSeriesstoreEngine(dir, userID string, reg prometheus.Registerer, opts *tsdb.Options) (tenantEngine, error) {
	// The ingester's memory series metrics are the TSDB head's, which the engine has to provide.
	callback := &countingSeriesCallback{
		SeriesLifecycleCallback: opts.SeriesLifecycleCallback,
		created: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "prometheus_tsdb_head_series_created_total",
			Help: "Total number of series created in the head",
		}),
		removed: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "prometheus_tsdb_head_series_removed_total",
			Help: "Total number of series removed in the head",
		}),
	}
	engine, err := store.OpenEngine(dir, userID, store.EngineOptions{
		Shards:                  seriesstoreShards,
		RetentionMs:             opts.RetentionDuration,
		OutOfOrderTimeWindowMs:  opts.OutOfOrderTimeWindow,
		MaxExemplars:            opts.MaxExemplars,
		TimelyCompaction:        opts.TimelyCompaction,
		SeriesLifecycleCallback: callback,
		SecondaryHashFunction:   opts.SecondaryHashFunction,
	})
	if err != nil {
		return nil, err
	}
	promauto.With(reg).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "prometheus_tsdb_head_series",
		Help: "Total number of series in the head block.",
	}, func() float64 { return float64(engine.NumSeries()) })
	compactions := promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "prometheus_tsdb_compactions_total",
		Help: "Total number of compactions that were executed for the partition.",
	})
	return seriesstoreEngine{Engine: engine, compactions: compactions}, nil
}

// countingSeriesCallback counts the series the engine creates and removes, like the TSDB head's metrics.
type countingSeriesCallback struct {
	tsdb.SeriesLifecycleCallback
	created, removed prometheus.Counter
}

func (c *countingSeriesCallback) PostCreation(lset labels.Labels) {
	c.created.Inc()
	c.SeriesLifecycleCallback.PostCreation(lset)
}

func (c *countingSeriesCallback) PostDeletion(deleted map[chunks.HeadSeriesRef]labels.Labels) {
	c.removed.Add(float64(len(deleted)))
	c.SeriesLifecycleCallback.PostDeletion(deleted)
}

func (e seriesstoreEngine) Appender(ctx context.Context) storage.Appender {
	return e.Engine.Appender(ctx)
}

func (e seriesstoreEngine) Compact(ctx context.Context) error {
	defer e.countCompaction(e.MinTime())
	return e.Engine.Compact(ctx)
}

func (e seriesstoreEngine) CompactHead(mint, maxt int64) error {
	defer e.countCompaction(e.MinTime())
	return e.Engine.CompactHead(mint, maxt)
}

func (e seriesstoreEngine) countCompaction(minTimeBefore int64) {
	if e.MinTime() != minTimeBefore {
		e.compactions.Inc()
	}
}

func (e seriesstoreEngine) Head() engineHead {
	return seriesstoreHead{e.Engine}
}

func (e seriesstoreEngine) Blocks() []*tsdb.Block {
	return nil
}

func (e seriesstoreEngine) BlocksToDelete([]*tsdb.Block) map[ulid.ULID]struct{} {
	return nil
}

// DisableCompactions is a no-op: the engine only compacts when the ingester asks it to.
func (e seriesstoreEngine) DisableCompactions() {}

func (e seriesstoreEngine) ApplyConfig(conf *config.Config) error {
	if conf.StorageConfig.ExemplarsConfig != nil {
		e.SetMaxExemplars(conf.StorageConfig.ExemplarsConfig.MaxExemplars)
	}
	if conf.StorageConfig.TSDBConfig != nil {
		e.SetOutOfOrderTimeWindow(conf.StorageConfig.TSDBConfig.OutOfOrderTimeWindow)
	}
	return nil
}

func (e seriesstoreEngine) StartTime() (int64, error) {
	return e.MinTime(), nil
}

type seriesstoreHead struct {
	*store.Engine
}

// PostingsForMatchersCache is nil: the engine's own caches follow its series as they're created.
func (h seriesstoreHead) PostingsForMatchersCache() *tsdb.PostingsForMatchersCache {
	return nil
}

// ValidateSeriesstoreEngine refuses configurations the seriesstore engine can't serve as the
// Prometheus TSDB would. It only runs in ingesters that select the engine.
func ValidateSeriesstoreEngine(tsdbCfg mimir_tsdb.TSDBConfig, ingesterCfg Config, ingestCfg ingest.Config) error {
	if tsdbCfg.Engine != mimir_tsdb.EngineSeriesstore {
		return nil
	}
	var errs []error
	// The engine has no WAL: after an unclean shutdown only Kafka has the data, from the file-stored
	// offset the ingester removes, and a pushed sample would be lost.
	if !ingestCfg.Enabled {
		errs = append(errs, errors.New("requires ingest storage (-ingest-storage.enabled=true)"))
	}
	if ingesterCfg.PushGrpcMethodEnabled {
		errs = append(errs, errors.New("requires the gRPC Push method disabled (-ingester.push-grpc-method-enabled=false)"))
	}
	if !ingestCfg.KafkaConfig.ConsumerGroupOffsetCommitFileEnforced {
		errs = append(errs, errors.New("requires the file-stored Kafka offset (-ingest-storage.kafka.consumer-group-offset-commit-file-enforced=true), which it resets after an unclean shutdown"))
	}
	// It writes no Prometheus blocks.
	if tsdbCfg.IsBlocksShippingEnabled() {
		errs = append(errs, errors.New("doesn't write blocks to ship (-blocks-storage.tsdb.ship-interval=0)"))
	}
	if tsdbCfg.FlushBlocksOnShutdown {
		errs = append(errs, errors.New("doesn't write blocks to flush (-blocks-storage.tsdb.flush-blocks-on-shutdown=false)"))
	}
	if tsdbCfg.OffsetCatalogue.Enabled {
		errs = append(errs, errors.New("has no blocks to catalogue Kafka offsets for (-blocks-storage.tsdb.offset-catalogue.enabled=false)"))
	}
	// It always snapshots its head on shutdown; the Prometheus option would suggest a choice.
	if tsdbCfg.MemorySnapshotOnShutdown {
		errs = append(errs, errors.New("always snapshots its head on shutdown (-blocks-storage.tsdb.memory-snapshot-on-shutdown=false)"))
	}
	if len(errs) > 0 {
		return fmt.Errorf("the %s TSDB engine %w", mimir_tsdb.EngineSeriesstore, errors.Join(errs...))
	}
	return nil
}

// resetKafkaOffsetsAfterUncleanShutdown removes the file-stored Kafka offsets when a seriesstore
// tenant wasn't restored from a clean shutdown: its head would miss what was consumed since, so the
// ingester replays Kafka from the retention period instead of resuming from the stored offset.
func (i *Ingester) resetKafkaOffsetsAfterUncleanShutdown(unclean []string) error {
	if len(unclean) == 0 {
		return nil
	}
	files, err := filepath.Glob(filepath.Join(i.cfg.BlocksStorageConfig.TSDB.Dir, "kafka-offset*.json"))
	if err != nil {
		return err
	}
	for _, file := range files {
		if err := os.Remove(file); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	level.Warn(i.logger).Log("msg", "tenants weren't restored from a clean shutdown, replaying Kafka from the retention period", "tenants", len(unclean), "example_tenant", unclean[0], "removed_offset_files", len(files))
	return nil
}

// uncleanlyRestored reports whether db is a seriesstore engine that opened without a clean
// shutdown's snapshot.
func uncleanlyRestored(db tenantEngine) bool {
	engine, ok := db.(seriesstoreEngine)
	return ok && !engine.Restored()
}
