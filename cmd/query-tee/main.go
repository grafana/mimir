// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/cmd/query-tee/main.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package main

import (
	"flag"
	"os"

	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/weaveworks/common/logging"
	"github.com/weaveworks/common/server"

	"github.com/grafana/mimir/pkg/util/instrumentation"
	util_log "github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/tools/querytee"
)

type Config struct {
	ServerMetricsPort int
	LogLevel          logging.Level
	ProxyConfig       querytee.ProxyConfig
	PathPrefix        string
}

func main() {
	// Parse CLI flags.
	cfg := Config{}
	flag.IntVar(&cfg.ServerMetricsPort, "server.metrics-port", 9900, "The port where metrics are exposed.")
	flag.StringVar(&cfg.PathPrefix, "server.path-prefix", "", "Path prefix for API paths (query-tee will accept Prometheus API calls at <prefix>/api/v1/...). Example: -server.path-prefix=/prometheus")
	cfg.LogLevel.RegisterFlags(flag.CommandLine)
	cfg.ProxyConfig.RegisterFlags(flag.CommandLine)
	flag.Parse()

	util_log.InitLogger(&server.Config{
		LogLevel: cfg.LogLevel,
	}, false)

	// Run the instrumentation server.
	registry := prometheus.NewRegistry()
	registry.MustRegister(collectors.NewGoCollector())

	i := instrumentation.NewMetricsServer(cfg.ServerMetricsPort, registry)
	if err := i.Start(); err != nil {
		level.Error(util_log.Logger).Log("msg", "Unable to start instrumentation server", "err", err.Error())
		os.Exit(1)
	}

	// Run the proxy.
	proxy, err := querytee.NewProxy(cfg.ProxyConfig, util_log.Logger, mimirReadRoutes(cfg), registry)
	if err != nil {
		level.Error(util_log.Logger).Log("msg", "Unable to initialize the proxy", "err", err.Error())
		os.Exit(1)
	}

	if err := proxy.Start(); err != nil {
		level.Error(util_log.Logger).Log("msg", "Unable to start the proxy", "err", err.Error())
		os.Exit(1)
	}

	proxy.Await()
}

func mimirReadRoutes(cfg Config) []querytee.Route {
	prefix := cfg.PathPrefix

	// Strip trailing slashes.
	for len(prefix) > 0 && prefix[len(prefix)-1] == '/' {
		prefix = prefix[:len(prefix)-1]
	}

	samplesComparator := querytee.NewSamplesComparator(querytee.SampleComparisonOptions{
		Tolerance:         cfg.ProxyConfig.ValueComparisonTolerance,
		UseRelativeError:  cfg.ProxyConfig.UseRelativeError,
		SkipRecentSamples: cfg.ProxyConfig.SkipRecentSamples,
	})
	return []querytee.Route{
		{Path: prefix + "/api/v1/query", RouteName: "api_v1_query", Methods: []string{"GET", "POST"}, ResponseComparator: samplesComparator},
		{Path: prefix + "/api/v1/query_range", RouteName: "api_v1_query_range", Methods: []string{"GET", "POST"}, ResponseComparator: samplesComparator},
		{Path: prefix + "/api/v1/query_exemplars", RouteName: "api_v1_query_exemplars", Methods: []string{"GET", "POST"}, ResponseComparator: nil},
		{Path: prefix + "/api/v1/labels", RouteName: "api_v1_labels", Methods: []string{"GET", "POST"}, ResponseComparator: nil},
		{Path: prefix + "/api/v1/label/{name}/values", RouteName: "api_v1_label_name_values", Methods: []string{"GET", "POST"}, ResponseComparator: nil},
		{Path: prefix + "/api/v1/series", RouteName: "api_v1_series", Methods: []string{"GET", "POST"}, ResponseComparator: nil},
		{Path: prefix + "/api/v1/metadata", RouteName: "api_v1_metadata", Methods: []string{"GET", "POST"}, ResponseComparator: nil},
		{Path: prefix + "/prometheus/config/v1/rules", RouteName: "prometheus_config_v1_rules", Methods: []string{"GET", "POST"}, ResponseComparator: nil},
		{Path: prefix + "/api/v1/alerts", RouteName: "api_v1_alerts", Methods: []string{"GET", "POST"}, ResponseComparator: nil},
	}
}
