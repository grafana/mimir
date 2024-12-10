// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/log"
	"github.com/grafana/dskit/modules"
	"github.com/grafana/dskit/tracing"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	jaegercfg "github.com/uber/jaeger-client-go/config"

	"github.com/grafana/mimir/pkg/continuoustest"
	"github.com/grafana/mimir/pkg/util/instrumentation"
	util_log "github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/pkg/util/version"
)

func main() {
	// Parse CLI arguments.
	cfg := &continuoustest.Config{}
	var (
		serverMetricsPort int
		logLevel          log.Level
	)
	flag.CommandLine.IntVar(&serverMetricsPort, "server.metrics-port", 9900, "The port where metrics are exposed.")
	cfg.RegisterFlags(flag.CommandLine)
	logLevel.RegisterFlags(flag.CommandLine)

	if err := flagext.ParseFlagsWithoutArguments(flag.CommandLine); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	util_log.InitLogger(log.LogfmtFormat, logLevel, false, util_log.RateLimitedLoggerCfg{})
	level.Warn(util_log.Logger).Log("msg", "The mimir-continuous-test binary you are using is deprecated. Please use the Mimir binary module `mimir -target=continuous-test`.")

	// Setting the environment variable JAEGER_AGENT_HOST enables tracing.
	if trace, err := tracing.NewFromEnv("mimir-continuous-test", jaegercfg.MaxTagValueLength(16e3)); err != nil {
		level.Error(util_log.Logger).Log("msg", "Failed to setup tracing", "err", err.Error())
	} else {
		defer trace.Close()
	}

	logger := util_log.Logger

	// Run the instrumentation server.
	registry := prometheus.NewRegistry()
	registry.MustRegister(version.NewCollector("mimir_continuous_test"))
	registry.MustRegister(collectors.NewGoCollector())

	i := instrumentation.NewMetricsServer(serverMetricsPort, registry, util_log.Logger)
	if err := i.Start(); err != nil {
		level.Error(logger).Log("msg", "Unable to start instrumentation server", "err", err.Error())
		util_log.Flush()
		os.Exit(1)
	}

	// Init the client used to write/read to/from Mimir.
	client, err := continuoustest.NewClient(cfg.Client, logger)
	if err != nil {
		level.Error(logger).Log("msg", "Failed to initialize client", "err", err.Error())
		util_log.Flush()
		os.Exit(1)
	}

	// Run continuous testing.
	m := continuoustest.NewManager(cfg.Manager, logger)
	m.AddTest(continuoustest.NewWriteReadSeriesTest(cfg.WriteReadSeriesTest, client, logger, registry))
	if err := m.Run(context.Background()); err != nil {
		if !errors.Is(err, modules.ErrStopProcess) {
			level.Error(logger).Log("msg", "Failed to run continuous test", "err", err.Error())
			util_log.Flush()
			os.Exit(1)
		}
		util_log.Flush()
	}
}
