// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"flag"
	"os"

	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/weaveworks/common/logging"
	"github.com/weaveworks/common/server"
	"github.com/weaveworks/common/tracing"

	"github.com/grafana/mimir/pkg/continuoustest"
	"github.com/grafana/mimir/pkg/util/instrumentation"
	util_log "github.com/grafana/mimir/pkg/util/log"
)

type Config struct {
	ServerMetricsPort   int
	LogLevel            logging.Level
	Client              continuoustest.ClientConfig
	Manager             continuoustest.ManagerConfig
	WriteReadSeriesTest continuoustest.WriteReadSeriesTestConfig
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.IntVar(&cfg.ServerMetricsPort, "server.metrics-port", 9900, "The port where metrics are exposed.")
	cfg.LogLevel.RegisterFlags(f)
	cfg.Client.RegisterFlags(f)
	cfg.Manager.RegisterFlags(f)
	cfg.WriteReadSeriesTest.RegisterFlags(f)
}

func main() {
	// Parse CLI flags.
	cfg := &Config{}
	cfg.RegisterFlags(flag.CommandLine)
	flag.Parse()

	util_log.InitLogger(&server.Config{
		LogLevel: cfg.LogLevel,
	}, false)

	// Setting the environment variable JAEGER_AGENT_HOST enables tracing.
	if trace, err := tracing.NewFromEnv("mimir-continuous-test"); err != nil {
		level.Error(util_log.Logger).Log("msg", "Failed to setup tracing", "err", err.Error())
	} else {
		defer trace.Close()
	}

	logger := util_log.Logger

	// Run the instrumentation server.
	registry := prometheus.NewRegistry()
	registry.MustRegister(collectors.NewGoCollector())

	i := instrumentation.NewMetricsServer(cfg.ServerMetricsPort, registry)
	if err := i.Start(); err != nil {
		level.Error(logger).Log("msg", "Unable to start instrumentation server", "err", err.Error())
		os.Exit(1)
	}

	// Init the client used to write/read to/from Mimir.
	client, err := continuoustest.NewClient(cfg.Client, logger)
	if err != nil {
		level.Error(logger).Log("msg", "Failed to initialize client", "err", err.Error())
		os.Exit(1)
	}

	// Run continuous testing.
	m := continuoustest.NewManager(cfg.Manager, logger)
	m.AddTest(continuoustest.NewWriteReadSeriesTest(cfg.WriteReadSeriesTest, client, logger, registry))
	if err := m.Run(context.Background()); err != nil {
		level.Error(logger).Log("msg", "Failed to run continuous test", "err", err.Error())
		os.Exit(1)
	}
}
