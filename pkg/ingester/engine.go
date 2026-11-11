// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"time"

	"github.com/grafana/dskit/ring"
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

	// Ingest commits valid samples and exemplars, reporting soft errors through ctx.
	// A hard append error rolls back the batch.
	Ingest(ctx *ingestContext, timeseries []mimirpb.PreallocTimeseries) error

	// Compact lets the engine apply its normal compaction criteria.
	Compact(ctx context.Context) error

	// Flush moves in-order samples through upTo (inclusive, in milliseconds) and all
	// out-of-order samples out of memory, along with series that have nothing newer.
	Flush(ctx context.Context, upTo int64) error
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
// the head index, and series ownership.
type engineHead interface {
	NumSeries() uint64
	TimeBounds() timeBounds
	// OldestAppendableTime returns the oldest time a sample can still be appended at, when it is known.
	OldestAppendableTime() (int64, bool)

	Index() (tsdb.IndexReader, error)
	// Ownership counts owned series and visits non-owned refs without allocating an intermediate list.
	// The callback must not be retained because callers may close their index after Ownership returns.
	Ownership(owned ring.TokenRanges, visitNonOwned func(storage.SeriesRef)) int
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
