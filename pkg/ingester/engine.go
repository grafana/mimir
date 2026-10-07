// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"slices"

	"github.com/grafana/dskit/ring"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"

	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
)

// tenantEngine is the per-tenant storage behind userTSDB: everything the ingester does around it (limits through
// the series lifecycle callbacks, active series, owned series, compaction scheduling, idle close, shipping) stays
// in userTSDB, so another engine only has to store and serve samples. The ingester says what it wants, and when:
// how the engine does it is the engine's.
type tenantEngine interface {
	// Ingest stores the series of a write request, and makes them visible. Soft failures are reported to the sink as
	// they happen, and a hard error undoes the whole batch.
	Ingest(ctx context.Context, batch ingestBatch, sink ingestSink) (ingestOutcome, error)
	Querier(mint, maxt int64) (storage.Querier, error)
	// ChunkQuerier returns the chunks of the series in the range; with unordered, those of a series may overlap
	// and come in any order, which the engine may serve more cheaply.
	ChunkQuerier(mint, maxt int64, unordered bool) (storage.ChunkQuerier, error)
	ExemplarQuerier(ctx context.Context) (storage.ExemplarQuerier, error)

	Head() engineHead

	// Compact is the routine upkeep the ingester asks for on its schedule. What it does, if anything, is up to the
	// engine.
	Compact(ctx context.Context) error
	// Flush moves the samples up to upTo out of memory, and the series that have nothing newer with them: when the
	// ingester shuts down or goes idle, or has too many series in memory.
	Flush(ctx context.Context, upTo int64) error
	// Evict drops the series from memory, whatever they hold, which stays queryable: series the ingester no longer
	// owns.
	Evict(ctx context.Context, refs []storage.SeriesRef) error

	// Configure applies settings that change while the engine runs.
	Configure(settings engineSettings) error
	Close() error
}

// engineSettings are the settings of an engine that the ingester changes at runtime.
type engineSettings struct {
	// OutOfOrderTimeWindow is how far behind the newest sample a sample may be and still be accepted, in
	// milliseconds, or 0 to accept none.
	OutOfOrderTimeWindow int64
	MaxExemplars         int64
	// FloatChunkEncoding is how new chunks of floats are encoded, by the name the limit uses.
	FloatChunkEncoding string
}

// timeBounds are the times of the samples in the head, in milliseconds.
type timeBounds struct {
	MinTime, MaxTime       int64
	MinOOOTime, MaxOOOTime int64
}

// engineHead is the in-memory part of a tenantEngine, which the ingester reads for series counts, time bounds,
// the head index, and series ownership.
type engineHead interface {
	NumSeries() uint64
	TimeBounds() timeBounds
	// OldestAppendableTime returns the oldest time a sample can still be appended at, when it is known.
	OldestAppendableTime() (int64, bool)

	Index() (tsdb.IndexReader, error)
	// Ownership returns how many of the series fall in the token ranges, and the refs of those that don't.
	Ownership(owned ring.TokenRanges) (ownedCount int, nonOwned []storage.SeriesRef)
	// ShardSeries returns the series in shard shardIndex of shardCount, by ref.
	ShardSeries(shardIndex, shardCount uint64) index.Postings

	// Sync makes what was appended so far durable, before the ingester commits its Kafka offset.
	Sync() error
}

// mustIndex returns the head's index, for callers that can't continue without it.
func mustIndex(head engineHead) tsdb.IndexReader {
	idx, err := head.Index()
	if err != nil {
		panic(err)
	}
	return idx
}

// testEngine lets tests run the whole package on another engine (MIMIR_TEST_TSDB_ENGINE); it's
// always empty outside tests.
var testEngine string

// testNoNativeActive makes tests run an engine that keeps the active series itself with the ingester's tracker instead,
// to compare them (MIMIR_TEST_NO_NATIVE_ACTIVE); it's always false outside tests.
var testNoNativeActive bool

// openTenantEngine opens the tenant's TSDB in dir with the engine the ingester is configured with.
func (i *Ingester) openTenantEngine(dir, userID string, logger *slog.Logger, reg prometheus.Registerer, opts *tsdb.Options) (tenantEngine, error) {
	if i.cfg.BlocksStorageConfig.TSDB.Engine == mimir_tsdb.EngineSeriesstore || testEngine == mimir_tsdb.EngineSeriesstore {
		return openSeriesstoreEngine(dir, userID, reg, opts, seriesstoreShardsFor(i.limits.MaxGlobalSeriesPerUser(userID)))
	}
	return openPrometheusEngine(dir, logger, reg, opts)
}

// prometheusEngine is the Prometheus TSDB as a tenantEngine.
type prometheusEngine struct {
	*tsdb.DB
	// The time range of a block, which a flush compacts the head by.
	blockDuration int64
}

func (e prometheusEngine) ChunkQuerier(mint, maxt int64, unordered bool) (storage.ChunkQuerier, error) {
	if unordered {
		return e.UnorderedChunkQuerier(mint, maxt)
	}
	return e.DB.ChunkQuerier(mint, maxt)
}

// openPrometheusEngine opens the tenant's TSDB in dir, replaying its WAL.
func openPrometheusEngine(dir string, logger *slog.Logger, reg prometheus.Registerer, opts *tsdb.Options) (tenantEngine, error) {
	db, err := tsdb.Open(dir, logger, reg, opts, nil)
	if err != nil {
		return nil, err
	}
	// The ingester compacts on its own schedule.
	db.DisableCompactions()
	return prometheusEngine{DB: db, blockDuration: opts.MinBlockDuration}, nil
}

func (e prometheusEngine) Head() engineHead {
	return prometheusHead{e.DB.Head()}
}

// prometheusHead is the Prometheus TSDB's head as an engineHead.
type prometheusHead struct {
	*tsdb.Head
}

func (h prometheusHead) Sync() error {
	return h.FsyncWLSegments()
}

func (h prometheusHead) TimeBounds() timeBounds {
	return timeBounds{MinTime: h.MinTime(), MaxTime: h.MaxTime(), MinOOOTime: h.MinOOOTime(), MaxOOOTime: h.MaxOOOTime()}
}

func (h prometheusHead) OldestAppendableTime() (int64, bool) {
	return h.AppendableMinValidTime()
}

func (h prometheusHead) Ownership(owned ring.TokenRanges) (int, []storage.SeriesRef) {
	var (
		count    int
		nonOwned []storage.SeriesRef
	)
	h.ForEachSecondaryHash(func(refs []chunks.HeadSeriesRef, secondaryHashes []uint32) {
		for i, hash := range secondaryHashes {
			if owned.IncludesKey(hash) {
				count++
			} else {
				nonOwned = append(nonOwned, storage.SeriesRef(refs[i]))
			}
		}
	})
	return count, nonOwned
}

func (h prometheusHead) ShardSeries(shardIndex, shardCount uint64) index.Postings {
	out := make([]storage.SeriesRef, 0, 128)
	h.ForEachShardHash(func(refs []storage.SeriesRef, shardHashes []uint64) {
		for i := range refs {
			if shardHashes[i]%shardCount == shardIndex {
				out = append(out, refs[i])
			}
		}
	})
	slices.Sort(out) // The postings of an index are sorted.
	return index.NewListPostings(out)
}

// Flush compacts the head block by block up to upTo, then the out-of-order head.
func (e prometheusEngine) Flush(ctx context.Context, upTo int64) error {
	head := e.DB.Head()
	for {
		blockMinTime, blockMaxTime, isValid, isLast := nextForcedHeadCompactionRange(e.blockDuration, head.MinTime(), head.MaxTime(), upTo)
		if !isValid {
			break
		}

		if err := e.CompactHead(tsdb.NewRangeHead(head, blockMinTime, blockMaxTime)); err != nil {
			return err
		}

		// Do not check again if it was the last range.
		if isLast {
			break
		}
	}

	return e.CompactOOOHead(ctx)
}

// Evict compacts the out-of-order head first, which persists the out-of-order data of the series before they
// leave, then the series themselves. The first failure doesn't stop the second: series without out-of-order data
// can still leave.
func (e prometheusEngine) Evict(ctx context.Context, refs []storage.SeriesRef) error {
	oooErr := e.CompactOOOHead(ctx)
	if oooErr != nil {
		oooErr = fmt.Errorf("compact the out-of-order head: %w", oooErr)
	}
	selectedErr := e.CompactSelectedSeries(refs)
	if selectedErr != nil {
		selectedErr = fmt.Errorf("compact the selected series: %w", selectedErr)
	}
	return errors.Join(oooErr, selectedErr)
}

func (e prometheusEngine) Configure(settings engineSettings) error {
	// DB.ApplyConfig only looks at the TSDB settings of the config; the rest of its fields are for things like
	// rules and scrapes.
	return e.ApplyConfig(&config.Config{
		StorageConfig: config.StorageConfig{
			ExemplarsConfig: &config.ExemplarsConfig{MaxExemplars: settings.MaxExemplars},
			TSDBConfig: &config.TSDBConfig{
				OutOfOrderTimeWindow: settings.OutOfOrderTimeWindow,
				// ApplyConfig reads the empty string as "keep the encoding resolved at startup", so a
				// tenant clearing the limit needs an explicit value to fall back to the default.
				ChunkEncoding: config.ChunkEncodingConfig{Floats: settings.FloatChunkEncoding},
			},
		},
	})
}

func (e prometheusEngine) BlocksToDelete(blocks []*tsdb.Block) map[ulid.ULID]struct{} {
	return tsdb.DefaultBlocksToDelete(e.DB)(blocks)
}

// nextForcedHeadCompactionRange computes the next TSDB head range to compact when a forced compaction
// is triggered. If the returned isValid is false, then the returned range should not be compacted.
func nextForcedHeadCompactionRange(blockDuration, headMinTime, headMaxTime, forcedMaxTime int64) (minTime, maxTime int64, isValid, isLast bool) {
	// Nothing to compact if the head is empty.
	if headMinTime == math.MaxInt64 || headMaxTime == math.MinInt64 {
		return 0, 0, false, true
	}

	// By default we try to compact the whole head, honoring the forcedMaxTime.
	minTime = headMinTime
	maxTime = min(headMaxTime, forcedMaxTime)

	// Due to the forcedMaxTime, the range may be empty. In that case we just skip it.
	if maxTime < minTime {
		return 0, 0, false, true
	}

	// Check whether the head compaction range would span across multiple block ranges.
	// If so, we break it to honor the block range period.
	if (minTime/blockDuration)*blockDuration != (maxTime/blockDuration)*blockDuration {
		// Block max time is exclusive, so we do a -1 here.
		maxTime = ((minTime/blockDuration)+1)*blockDuration - 1
		return minTime, maxTime, true, false
	}

	return minTime, maxTime, true, true
}
