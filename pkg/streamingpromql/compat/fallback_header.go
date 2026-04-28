// SPDX-License-Identifier: AGPL-3.0-only

package compat

import (
	"context"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/util/propagation"
)

type engineFallbackContextKey int

const forceFallbackEnabledContextKey = engineFallbackContextKey(0)
const ForceFallbackHeaderName = "X-Mimir-Force-Prometheus-Engine"

type EngineFallbackExtractor struct{}

func (e *EngineFallbackExtractor) ExtractFromCarrier(ctx context.Context, carrier propagation.Carrier) (context.Context, error) {
	if value := carrier.Get(ForceFallbackHeaderName); value != "" {
		if value != "true" {
			return nil, apierror.Newf(apierror.TypeBadData, "invalid value '%s' for '%s' header, must be exactly 'true' or not set", value, ForceFallbackHeaderName)
		}

		ctx = withForceFallbackEnabled(ctx)
	}

	return ctx, nil
}

func withForceFallbackEnabled(ctx context.Context) context.Context {
	return context.WithValue(ctx, forceFallbackEnabledContextKey, true)
}

func isForceFallbackEnabled(ctx context.Context) bool {
	return ctx.Value(forceFallbackEnabledContextKey) != nil
}
