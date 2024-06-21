// SPDX-License-Identifier: AGPL-3.0-only

package compat

import (
	"context"
	"net/http"

	apierror "github.com/grafana/mimir/pkg/api/error"
)

type engineFallbackContextKey int

const forceFallbackEnabledContextKey = engineFallbackContextKey(0)
const ForceFallbackHeaderName = "X-Mimir-Force-Prometheus-Engine"

type EngineFallbackInjector struct{}

func (i EngineFallbackInjector) Wrap(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if value := r.Header.Get(ForceFallbackHeaderName); value != "" {
			if value != "true" {
				// Send a Prometheus API-style JSON error response.
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusBadRequest)
				e := apierror.Newf(apierror.TypeBadData, "invalid value '%s' for '%s' header, must be exactly 'true' or not set", value, ForceFallbackHeaderName)

				if body, err := e.EncodeJSON(); err == nil {
					_, _ = w.Write(body)
				}

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
