// SPDX-License-Identifier: AGPL-3.0-only

package limiter

import (
	"net/http"
)

// UnlimitedMemoryTrackerMiddleware adds an unlimited MemoryConsumptionTracker which will not
// enforce memory limit to the request context.
type UnlimitedMemoryTrackerMiddleware struct{}

// Wrap implements middleware.Interface.
func (m UnlimitedMemoryTrackerMiddleware) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := ContextWithNewUnlimitedMemoryConsumptionTracker(r.Context())
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
