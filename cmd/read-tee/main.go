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
	readtee "github.com/grafana/mimir/tools/read-tee"
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

// mimirReadRoutes returns the amplifiable read routes: every Mimir read endpoint that reaches
// ingesters or store-gateways. The rewritten copies suffix the query / match[] / selector params (or
// the remote read matchers) so they hit the _amp{k} series write-tee created.
//
// The gateway forwards the full request path, which in our setup carries a /prometheus prefix.
// To be robust we register BOTH the bare and /prometheus-prefixed forms; everything else falls
// through to the catch-all passthrough (preferred backend only).
func mimirReadRoutes() []readtee.Route {
	type spec struct {
		path       string
		routeName  string
		methods    []string
		instant    bool
		remoteRead bool
	}
	getPost := []string{"GET", "POST"}
	specs := []spec{
		{path: "/api/v1/query", routeName: "api_v1_query", methods: getPost, instant: true},
		{path: "/api/v1/query_range", routeName: "api_v1_query_range", methods: getPost},
		{path: "/api/v1/query_exemplars", routeName: "api_v1_query_exemplars", methods: getPost},
		{path: "/api/v1/series", routeName: "api_v1_series", methods: getPost},
		{path: "/api/v1/labels", routeName: "api_v1_labels", methods: getPost},
		{path: "/api/v1/label/{name}/values", routeName: "api_v1_label_values", methods: getPost},
		// Metadata has no selector: metric names aren't suffixed, so copies are identical requests.
		{path: "/api/v1/metadata", routeName: "api_v1_metadata", methods: getPost},
		{path: "/api/v1/cardinality/label_names", routeName: "api_v1_cardinality_label_names", methods: getPost},
		{path: "/api/v1/cardinality/label_values", routeName: "api_v1_cardinality_label_values", methods: getPost},
		{path: "/api/v1/cardinality/active_series", routeName: "api_v1_cardinality_active_series", methods: getPost},
		{path: "/api/v1/cardinality/active_native_histogram_metrics", routeName: "api_v1_cardinality_active_native_histogram_metrics", methods: getPost},
		{path: "/api/v1/search/metric_names", routeName: "api_v1_search_metric_names", methods: getPost},
		{path: "/api/v1/search/label_names", routeName: "api_v1_search_label_names", methods: getPost},
		{path: "/api/v1/search/label_values", routeName: "api_v1_search_label_values", methods: getPost},
		{path: "/api/v1/read", routeName: "api_v1_read", methods: []string{"POST"}, remoteRead: true},
	}

	var routes []readtee.Route
	for _, s := range specs {
		route := readtee.Route{
			Path:       s.path,
			RouteName:  s.routeName,
			Methods:    s.methods,
			Instant:    s.instant,
			RemoteRead: s.remoteRead,
		}
		// Bare form.
		routes = append(routes, route)
		// /prometheus-prefixed form (the path the gateway forwards in our setup).
		route.Path = "/prometheus" + s.path
		route.RouteName = "prometheus_" + s.routeName
		routes = append(routes, route)
	}
	return routes
}
