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
	"github.com/grafana/mimir/tools/readtee"
)

type Config struct {
	ServerMetricsPort int
	ProxyConfig       readtee.ProxyConfig
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
	proxy, err := readtee.NewProxy(cfg.ProxyConfig, util_log.Logger, mimirReadRoutes(), registry)
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
		name = "read-tee"
	}

	trace, err := tracing.NewOTelOrJaegerFromEnv(name, util_log.Logger)
	if err != nil {
		level.Error(util_log.Logger).Log("msg", "Failed to setup tracing", "err", err.Error())
		return nil
	}

	return trace
}

// mimirReadRoutes returns the amplifiable read routes. The rewritten copies suffix the query /
// match[] params so they hit the _amp{k} series write-tee created.
//
// The gateway forwards the full request path, which in our setup carries a /prometheus prefix.
// To be robust we register BOTH the bare and /prometheus-prefixed forms; everything else falls
// through to the catch-all passthrough (preferred backend only).
func mimirReadRoutes() []readtee.Route {
	methods := []string{"GET", "POST"}

	type spec struct {
		path      string
		routeName string
	}
	specs := []spec{
		{"/api/v1/query", "api_v1_query"},
		{"/api/v1/query_range", "api_v1_query_range"},
		{"/api/v1/series", "api_v1_series"},
		{"/api/v1/labels", "api_v1_labels"},
		{"/api/v1/label/{name}/values", "api_v1_label_values"},
	}

	var routes []readtee.Route
	for _, s := range specs {
		// Bare form.
		routes = append(routes, readtee.Route{
			Path:      s.path,
			RouteName: s.routeName,
			Methods:   methods,
		})
		// /prometheus-prefixed form (the path the gateway forwards in our setup).
		routes = append(routes, readtee.Route{
			Path:      "/prometheus" + s.path,
			RouteName: "prometheus_" + s.routeName,
			Methods:   methods,
		})
	}
	return routes
}
