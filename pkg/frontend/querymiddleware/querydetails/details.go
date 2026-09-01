// SPDX-License-Identifier: AGPL-3.0-only

package querydetails

import (
	"context"
	"time"

	"github.com/grafana/mimir/pkg/querier/stats"
)

type QueryDetails struct {
	QuerierStats *stats.SafeStats

	// Start and End are the parsed start and end times of the unmodified user request.
	Start, End time.Time
	// MinT and MaxT are the earliest and latest points in time which the query might try to use.
	// For example, they account for range selectors and @ modifiers.
	// MinT and MaxT may be zero-valued if the query doesn't process samples.
	MinT, MaxT    time.Time
	Step          time.Duration
	LookbackDelta time.Duration

	// ResultsCacheMissBytes is an estimate of the size of the results that were not found in the cache and were freshly evaluated.
	ResultsCacheMissBytes int

	// ResultsCacheMissCount is the number of cache entries requested from the cache that were not found.
	ResultsCacheMissCount int

	// ResultsCacheHitBytes is an estimate of the size of the results that were used from the cache.
	ResultsCacheHitBytes int

	// ResultsCacheHitCount is the number of cache entries requested from the cache that were found.
	ResultsCacheHitCount int

	// ResultsCacheSetCount is the number of cache entries that were attempted to be written during this request.
	ResultsCacheSetCount int

	// ResponseSeriesCount is the number of series in the query response.
	ResponseSeriesCount int
	// ResponseSamplesCount is the total number of samples (floats or histograms) across all series in the query response.
	ResponseSamplesCount int
}

type contextKey int

var ctxKey = contextKey(0)

// ContextWithEmptyDetails returns a context with empty QueryDetails.
// The returned context also has querier stats.Stats injected. The stats pointer in the context
// and the stats pointer in the QueryDetails are the same.
func ContextWithEmptyDetails(ctx context.Context) (*QueryDetails, context.Context) {
	s, ctx := stats.ContextWithEmptyStats(ctx)
	details := &QueryDetails{QuerierStats: s}
	ctx = context.WithValue(ctx, ctxKey, details)
	return details, ctx
}

// QueryDetailsFromContext gets the QueryDetails out of the Context. Returns nil if stats have not
// been initialised in the context.
func QueryDetailsFromContext(ctx context.Context) *QueryDetails {
	o := ctx.Value(ctxKey)
	if o == nil {
		return nil
	}
	return o.(*QueryDetails)
}

func (d *QueryDetails) Merge(other *QueryDetails) {
	if d == nil || other == nil {
		return
	}

	d.QuerierStats.Merge(other.QuerierStats)
	if !other.Start.IsZero() && (d.Start.IsZero() || other.Start.Before(d.Start)) {
		d.Start = other.Start
	}
	if !other.End.IsZero() && (d.End.IsZero() || other.End.After(d.End)) {
		d.End = other.End
	}
	if !other.MinT.IsZero() && (d.MinT.IsZero() || other.MinT.Before(d.MinT)) {
		d.MinT = other.MinT
	}
	if !other.MaxT.IsZero() && (d.MaxT.IsZero() || other.MaxT.After(d.MaxT)) {
		d.MaxT = other.MaxT
	}
	if d.LookbackDelta < other.LookbackDelta {
		d.LookbackDelta = other.LookbackDelta
	}

	d.ResultsCacheMissBytes += other.ResultsCacheMissBytes
	d.ResultsCacheMissCount += other.ResultsCacheMissCount
	d.ResultsCacheHitBytes += other.ResultsCacheHitBytes
	d.ResultsCacheHitCount += other.ResultsCacheHitCount
	d.ResultsCacheSetCount += other.ResultsCacheSetCount
	d.ResponseSeriesCount += other.ResponseSeriesCount
	d.ResponseSamplesCount += other.ResponseSamplesCount
}
