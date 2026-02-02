// SPDX-License-Identifier: AGPL-3.0-only

package limiter

import (
	"net/http"
)

type SeriesDeduplicatorMiddleware struct{}

func (m SeriesDeduplicatorMiddleware) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := AddSeriesDeduplicatorToContext(r.Context(), NewSeriesDeduplicator())
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
