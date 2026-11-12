// SPDX-License-Identifier: AGPL-3.0-only
//go:build !rustengine

package ingester

import (
	"fmt"
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/tsdb"
)

func openTenantEngine(engine, dir string, logger *slog.Logger, reg prometheus.Registerer, opts *tsdb.Options, _ bool) (tenantEngine, error) {
	if engine != "" && engine != "prometheus" {
		return nil, fmt.Errorf("engine %q requires CGO and a binary built with -tags=rustengine", engine)
	}
	return openPrometheusEngine(dir, logger, reg, opts)
}
