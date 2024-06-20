// SPDX-License-Identifier: AGPL-3.0-only

package compat

import (
	"context"
	"fmt"
	"net/http"
)

type engineFallbackContextKey int

const forceFallbackEnabledContextKey = engineFallbackContextKey(0)
const forceFallbackHeaderName = "X-Mimir-Force-Prometheus-Engine"

type EngineFallbackInjector struct{}

func (i EngineFallbackInjector) Wrap(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if value := r.Header.Get(forceFallbackHeaderName); value != "" {
			if value != "true" {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(fmt.Sprintf("invalid value '%s' for '%s' header, must be exactly 'true' or not set", value, forceFallbackHeaderName)))
				return
			}

			r = r.WithContext(withForceFallbackEnabled(r.Context()))
		}

		handler.ServeHTTP(w, r)
	})
}

func withForceFallbackEnabled(ctx context.Context) context.Context {
	return context.WithValue(ctx, forceFallbackEnabledContextKey, true)
}

func isForceFallbackEnabled(ctx context.Context) bool {
	return ctx.Value(forceFallbackEnabledContextKey) != nil
}
