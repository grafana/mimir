// SPDX-License-Identifier: AGPL-3.0-only

package compat

import (
	"context"
	"fmt"

	"github.com/grafana/mimir/pkg/querier/api"
)

const ForceFallbackHeaderName = "X-Mimir-Force-Prometheus-Engine"

func ForceFallbackHeaderNameValidator(v string) error {
	if v == "true" {
		return nil
	}
	return fmt.Errorf("must be exactly 'true' or not set")
}

func withForceFallbackEnabled(ctx context.Context) context.Context {
	return api.ContextWithAHeaderOption(ctx, ForceFallbackHeaderName, "true")
}

func isForceFallbackEnabled(ctx context.Context) bool {
	v, ok := api.HeaderOptionFromContext(ctx, ForceFallbackHeaderName)
	return ok && v == "true"
}
