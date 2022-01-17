// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/flusher/flusher.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package flusher

import (
	"context"
	"flag"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/modules"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/grafana/mimir/pkg/ingester"
	"github.com/grafana/mimir/pkg/util/validation"
)

// Config for an Ingester.
type Config struct {
	ExitAfterFlush bool `yaml:"exit_after_flush"`
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&cfg.ExitAfterFlush, "flusher.exit-after-flush", true, "Stop after flush has finished. If false, process will keep running, doing nothing.")
}

// Flusher is designed to be used as a job to flush the data from the WAL on disk.
// Flusher works with both chunks-based and blocks-based ingesters.
type Flusher struct {
	services.Service

	cfg            Config
	ingesterConfig ingester.Config
	limits         *validation.Overrides
	registerer     prometheus.Registerer
	logger         log.Logger
}

const (
	postFlushSleepTime = 1 * time.Minute
)

// New constructs a new Flusher and flushes the data from the WAL.
// The returned Flusher has no other operations.
func New(
	cfg Config,
	ingesterConfig ingester.Config,
	limits *validation.Overrides,
	registerer prometheus.Registerer,
	logger log.Logger,
) (*Flusher, error) {

	f := &Flusher{
		cfg:            cfg,
		ingesterConfig: ingesterConfig,
		limits:         limits,
		registerer:     registerer,
		logger:         logger,
	}
	f.Service = services.NewBasicService(nil, f.running, nil)
	return f, nil
}

func (f *Flusher) running(ctx context.Context) error {
	ing, err := ingester.NewForFlusher(f.ingesterConfig, f.limits, f.registerer, f.logger)
	if err != nil {
		return errors.Wrap(err, "create ingester")
	}

	if err := services.StartAndAwaitRunning(ctx, ing); err != nil {
		return errors.Wrap(err, "start and await running ingester")
	}

	ing.Flush()

	// Sleeping to give a chance to Prometheus
	// to collect the metrics.
	level.Info(f.logger).Log("msg", "sleeping to give chance for collection of metrics", "duration", postFlushSleepTime.String())
	time.Sleep(postFlushSleepTime)

	if err := services.StopAndAwaitTerminated(ctx, ing); err != nil {
		return errors.Wrap(err, "stop and await terminated ingester")
	}

	if f.cfg.ExitAfterFlush {
		return modules.ErrStopProcess
	}

	// Return normally -- this keeps Mimir running.
	return nil
}
