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

	ResultsCacheMissBytes int
	ResultsCacheHitBytes  int

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
