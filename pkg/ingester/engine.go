// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"errors"
	"time"

	"github.com/grafana/mimir/pkg/mimirpb"
)

// tenantEngine is the per-tenant storage behind userTSDB, as the ingester asks it for what it wants rather than how:
// the ingester keeps the limits, stats, active series and cost attribution, so another engine only has to store and
// serve samples.
type tenantEngine interface {
	// Ingest stores the series of a write request, and makes them visible. Soft failures are reported to the sink as
	// they happen, and a hard error undoes the whole batch.
	Ingest(ctx context.Context, batch ingestBatch, sink *pushSink) (ingestOutcome, error)
}

// ingestBatch is what the ingester asks an engine to store: the series of one write request.
type ingestBatch struct {
	Series []mimirpb.PreallocTimeseries
	// Samples, histograms and exemplars outside [MinTimestampMs, MaxTimestampMs] are rejected.
	MinTimestampMs, MaxTimestampMs int64
	// Histograms are ingested only if NativeHistograms is set, and exemplars only if Exemplars is.
	NativeHistograms bool
	Exemplars        bool
}

// The exemplar errors that don't come from an engine.
var (
	errExemplarMissingSeries  = errors.New("exemplars can only be added to a series that exists")
	errExemplarTooFarInFuture = errors.New("the exemplar is too far in the future")
	errExemplarTooFarInPast   = errors.New("the exemplar is too far in the past")
)

// ingestOutcome is what an engine ingested of a batch.
type ingestOutcome struct {
	// Samples counts floats, histograms and created-timestamp zero samples.
	Samples   int
	Exemplars int
	// CommitDuration is how long making the batch visible took.
	CommitDuration time.Duration
}
