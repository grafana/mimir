// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"time"

	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/index"

	"github.com/grafana/mimir/pkg/ingester/activeseries"
	"github.com/grafana/mimir/pkg/mimirpb"
	mimir_storage "github.com/grafana/mimir/pkg/storage"
	util_log "github.com/grafana/mimir/pkg/util/log"
)

type tenantEngine interface {
	Head() engineHead

	Querier(mint, maxt int64) (storage.Querier, error)
	// ChunkQuerier may return overlapping chunks in any order when unordered is true.
	ChunkQuerier(mint, maxt int64, unordered bool) (storage.ChunkQuerier, error)
	ExemplarQuerier(ctx context.Context) (storage.ExemplarQuerier, error)

	// Ingest commits valid samples and exemplars, reporting soft errors through ctx.
	// A hard append error rolls back the batch.
	Ingest(ctx *ingestContext, timeseries []mimirpb.PreallocTimeseries) error

	// Configure applies tenant settings while the ingester is running.
	Configure(engineSettings) error

	// Compact lets the engine apply its normal compaction criteria.
	Compact(ctx context.Context) error

	// Close releases the store's resources while preserving its on-disk data for reopening.
	Close() error

	// Flush moves in-order samples through upTo (inclusive, in milliseconds) and all
	// out-of-order samples out of memory, along with series that have nothing newer.
	Flush(ctx context.Context, upTo int64) error

	// Evict drops the selected series from memory while keeping their samples queryable.
	Evict(ctx context.Context, refs []storage.SeriesRef) error
}

// ingestContext must not be retained after Ingest returns because its bookkeeping belongs to the request.
type ingestContext struct {
	context.Context

	i                  *Ingester
	userID             string
	startAppend        time.Time
	stats              *pushStats
	errProcessor       *mimir_storage.SoftAppendErrorProcessor
	updateFirstPartial func(sampler *util_log.Sampler, errFn softErrorFunction)
	activeSeries       *activeseries.ActiveSeries
	isOTLP             bool
}

type engineSettings struct {
	// OutOfOrderTimeWindow is in milliseconds.
	OutOfOrderTimeWindow int64
	MaxExemplars         int64
	FloatChunkEncoding   string
}

type prometheusEngine struct {
	db            *tsdb.DB
	blockDuration int64
}

// timeBounds are the times of the samples in the head, in milliseconds.
type timeBounds struct {
	MinTime, MaxTime       int64
	MinOOOTime, MaxOOOTime int64
}

// engineHead is the in-memory part of a tenantEngine, which the ingester reads for series counts, time bounds,
// and the head index.
type engineHead interface {
	NumSeries() uint64
	TimeBounds() timeBounds
	// MinInOrderAppendTime returns the minimum timestamp for in-order appends,
	// in milliseconds, when known. Older out-of-order samples may still be accepted.
	MinInOrderAppendTime() (int64, bool)

	Index() (tsdb.IndexReader, error)
	// ShardSeries returns the series in shard shardIndex of shardCount, by ref.
	ShardSeries(shardIndex, shardCount uint64) index.Postings
	// LabelValuesBytes returns the bytes of the distinct values of each label name.
	LabelValuesBytes() map[string]uint64

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
