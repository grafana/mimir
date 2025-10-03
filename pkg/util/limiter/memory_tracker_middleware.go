// SPDX-License-Identifier: AGPL-3.0-only

package limiter

import (
	"net/http"
)

// MemoryTrackerMiddleware adds an unlimited MemoryConsumptionTracker to the request context.
type MemoryTrackerMiddleware struct{}

// Wrap implements middleware.Interface.
func (m MemoryTrackerMiddleware) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := ContextWithNewUnlimitedMemoryConsumptionTracker(r.Context())
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
