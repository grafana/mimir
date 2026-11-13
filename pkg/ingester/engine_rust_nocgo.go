// SPDX-License-Identifier: AGPL-3.0-only
//go:build rustengine && !cgo

package ingester

import (
	"errors"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/tsdb"
)

func openTenantEngine(engine, dir string, logger *slog.Logger, reg prometheus.Registerer, opts *tsdb.Options, _ bool, _ time.Duration, _ bool) (tenantEngine, error) {
	if engine == "" || engine == "prometheus" {
		return openPrometheusEngine(dir, logger, reg, opts)
	}
	return nil, errors.New("Rust engine requires CGO_ENABLED=1 and the Rust static library")
}
