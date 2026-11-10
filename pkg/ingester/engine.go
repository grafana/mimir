// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"time"

	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"

	"github.com/grafana/mimir/pkg/ingester/activeseries"
	"github.com/grafana/mimir/pkg/mimirpb"
	mimir_storage "github.com/grafana/mimir/pkg/storage"
	util_log "github.com/grafana/mimir/pkg/util/log"
)

type tenantEngine interface {
	// Ingest commits valid samples and exemplars, reporting soft errors through ctx.
	// A hard append error rolls back the batch.
	Ingest(ctx *ingestContext, timeseries []mimirpb.PreallocTimeseries) error

	// Compact runs maintenance when the engine's normal compaction criteria are met.
	Compact(ctx context.Context) error
	// Flush forces head compaction through upTo (milliseconds), even when normal compaction criteria are not met.
	Flush(ctx context.Context, upTo int64) error
	// Evict persists the selected series before removing them from memory; their samples remain queryable.
	// It may make partial progress even when it returns an error.
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

type prometheusEngine struct {
	db            *tsdb.DB
	blockDuration int64
}
