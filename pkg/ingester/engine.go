// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"log/slog"

	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
)

// tenantEngine is the per-tenant storage behind userTSDB: everything the ingester does around it (limits through
// the series lifecycle callbacks, active series, owned series, compaction scheduling, idle close, shipping) stays
// in userTSDB, so another engine only has to store and serve samples like the Prometheus TSDB does.
type tenantEngine interface {
	Appender(ctx context.Context) storage.Appender
	Querier(mint, maxt int64) (storage.Querier, error)
	ChunkQuerier(mint, maxt int64) (storage.ChunkQuerier, error)
	UnorderedChunkQuerier(mint, maxt int64) (storage.ChunkQuerier, error)
	ExemplarQuerier(ctx context.Context) (storage.ExemplarQuerier, error)

	Head() engineHead

	Compact(ctx context.Context) error
	// CompactHead compacts the head's samples in [mint, maxt] into a block and truncates the head.
	CompactHead(mint, maxt int64) error
	CompactOOOHead(ctx context.Context) error
	CompactSelectedSeries(refs []storage.SeriesRef) error
	DisableCompactions()

	ApplyConfig(conf *config.Config) error
	StartTime() (int64, error)
	Dir() string
	Close() error
}

// engineHead is the in-memory part of a tenantEngine, which the ingester reads for series counts, time bounds,
// the head index, and series hashes.
type engineHead interface {
	NumSeries() uint64
	MinTime() int64
	MaxTime() int64
	MinOOOTime() int64
	MaxOOOTime() int64
	AppendableMinValidTime() (int64, bool)

	Index() (tsdb.IndexReader, error)
	MustIndex() tsdb.IndexReader
	ForEachSecondaryHash(fn func(refs []chunks.HeadSeriesRef, secondaryHashes []uint32))
	ForEachShardHash(fn func(refs []storage.SeriesRef, shardHashes []uint64))

	// Sync makes what was appended so far durable, before the ingester commits its Kafka offset.
	Sync() error
}

// tsdbEngine is what only the Prometheus TSDB engine has, which the ingester reaches by asserting it: the
// compacted data it keeps on disk, which the shipper uploads and its retention deletes. Another engine has none.
type tsdbEngine interface {
	Blocks() []*tsdb.Block
	// BlocksToDelete returns the blocks the engine's own retention would delete.
	BlocksToDelete(blocks []*tsdb.Block) map[ulid.ULID]struct{}
}

// tsdbHead is the part of the Prometheus TSDB's head that only it has: the metadata of the block it would
// become, and the postings cache it shares with the ingester, which may be nil.
type tsdbHead interface {
	Meta() tsdb.BlockMeta
	PostingsForMatchersCache() *tsdb.PostingsForMatchersCache
}

// prometheusEngine is the Prometheus TSDB as a tenantEngine.
type prometheusEngine struct {
	*tsdb.DB
}

// openPrometheusEngine opens the tenant's TSDB in dir, replaying its WAL.
func openPrometheusEngine(dir string, logger *slog.Logger, reg prometheus.Registerer, opts *tsdb.Options) (tenantEngine, error) {
	db, err := tsdb.Open(dir, logger, reg, opts, nil)
	if err != nil {
		return nil, err
	}
	return prometheusEngine{db}, nil
}

func (e prometheusEngine) Head() engineHead {
	return prometheusHead{e.DB.Head()}
}

// prometheusHead is the Prometheus TSDB's head as an engineHead.
type prometheusHead struct {
	*tsdb.Head
}

func (h prometheusHead) Sync() error {
	return h.Head.FsyncWLSegments()
}

func (e prometheusEngine) CompactHead(mint, maxt int64) error {
	return e.DB.CompactHead(tsdb.NewRangeHead(e.DB.Head(), mint, maxt))
}

func (e prometheusEngine) BlocksToDelete(blocks []*tsdb.Block) map[ulid.ULID]struct{} {
	return tsdb.DefaultBlocksToDelete(e.DB)(blocks)
}
