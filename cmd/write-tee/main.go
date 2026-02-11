// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/log"
	"github.com/grafana/dskit/signals"
	"github.com/grafana/dskit/tracing"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/grafana/mimir/pkg/util/instrumentation"
	util_log "github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/tools/writetee"
)

type Config struct {
	ServerMetricsPort int
	ProxyConfig       writetee.ProxyConfig
}

func main() {
	// Parse CLI flags.
	cfg := Config{}
	flag.IntVar(&cfg.ServerMetricsPort, "server.metrics-port", 9900, "The port where metrics are exposed.")
	cfg.ProxyConfig.RegisterFlags(flag.CommandLine)

	// Parse CLI arguments.
	if err := flagext.ParseFlagsWithoutArguments(flag.CommandLine); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	util_log.InitLogger(log.LogfmtFormat, cfg.ProxyConfig.Server.LogLevel, false, util_log.RateLimitedLoggerCfg{})

	if closer := initTracing(); closer != nil {
		defer closer.Close()
	}

	// Run the instrumentation server.
	registry := prometheus.NewRegistry()
	registry.MustRegister(collectors.NewGoCollector())

	i := instrumentation.NewMetricsServer(cfg.ServerMetricsPort, registry, util_log.Logger)
	if err := i.Start(); err != nil {
		level.Error(util_log.Logger).Log("msg", "Unable to start instrumentation server", "err", err.Error())
		util_log.Flush()
		os.Exit(1)
	}

	// Run the proxy.
	proxy, err := writetee.NewProxy(cfg.ProxyConfig, util_log.Logger, mimirWriteRoutes(), registry)
	if err != nil {
		level.Error(util_log.Logger).Log("msg", "Unable to initialize the proxy", "err", err.Error())
		util_log.Flush()
		os.Exit(1)
	}

	if err := proxy.Start(); err != nil {
		level.Error(util_log.Logger).Log("msg", "Unable to start the proxy", "err", err.Error())
		util_log.Flush()
		os.Exit(1)
	}

	// Setup signal handler for graceful shutdown
	handler := signals.NewHandler(util_log.Logger)
	go func() {
		handler.Loop()
		level.Info(util_log.Logger).Log("msg", "Received shutdown signal, stopping proxy")
		if err := proxy.Stop(); err != nil {
			level.Error(util_log.Logger).Log("msg", "Error stopping proxy", "err", err)
		}
	}()

	proxy.Await()
}

func initTracing() io.Closer {
	var name string
	if otelEnvName := os.Getenv("OTEL_SERVICE_NAME"); otelEnvName != "" {
		name = otelEnvName
	} else if jaegerEnvName := os.Getenv("JAEGER_SERVICE_NAME"); jaegerEnvName != "" {
		name = jaegerEnvName
	} else {
		name = "write-tee"
	}

	trace, err := tracing.NewOTelOrJaegerFromEnv(name, util_log.Logger)
	if err != nil {
		level.Error(util_log.Logger).Log("msg", "Failed to setup tracing", "err", err.Error())
		return nil
	}

	return trace
}

func mimirWriteRoutes() []writetee.Route {
	return []writetee.Route{
		{
			Path:      "/api/v1/push",
			RouteName: "api_v1_push",
			Methods:   []string{"POST"},
		},
	}
}
