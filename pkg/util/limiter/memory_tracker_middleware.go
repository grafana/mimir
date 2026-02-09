// SPDX-License-Identifier: AGPL-3.0-only

package limiter

import (
	"net/http"
)

// UnlimitedMemoryTrackerMiddleware adds the following to the context: 1) an unlimited MemoryConsumptionTracker which will not
// enforce memory limit to the request context and 2) SeriesLabelsDeduplicator which will deduplicate series
// from queriers and increase memory consumption for the labels.
type UnlimitedMemoryTrackerMiddleware struct{}

// Wrap implements middleware.Interface.
func (m UnlimitedMemoryTrackerMiddleware) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := ContextWithNewUnlimitedMemoryConsumptionTracker(r.Context())
		// Add per-request deduplicator for non-MQE queries. This deduplicator will track
		// all unique series across the entire request, ensuring labels are deduplicated
		// and memory is accurately tracked at the request level.
		ctx = AddSeriesDeduplicatorToContext(ctx, NewSeriesLabelsDeduplicator())
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
