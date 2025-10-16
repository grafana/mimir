// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/cmd/query-tee/main.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/log"
	"github.com/grafana/dskit/tracing"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/common/model"
	"gopkg.in/yaml.v3"

	"github.com/grafana/mimir/pkg/util/instrumentation"
	util_log "github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/tools/querytee"
)

const (
	configFileOption = "config.file"
	configExpandEnv  = "config.expand-env"
)

type Config struct {
	ServerMetricsPort int
	LogLevel          log.Level
	ProxyConfig       querytee.ProxyConfig
	PathPrefix        string
	ConfigFile        string
	ConfigExpandEnv   bool
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.ConfigFile, configFileOption, "", "Configuration file to load.")
	f.BoolVar(&cfg.ConfigExpandEnv, configExpandEnv, false, "Expands ${var} or $var in config according to the values of the environment variables.")
	f.IntVar(&cfg.ServerMetricsPort, "server.metrics-port", 9900, "The port where metrics are exposed.")
	f.StringVar(&cfg.PathPrefix, "server.path-prefix", "", "Path prefix for API paths (query-tee will accept Prometheus API calls at <prefix>/api/v1/...). Example: -server.path-prefix=/prometheus")

	cfg.LogLevel.RegisterFlags(f)
	cfg.ProxyConfig.RegisterFlags(f)

	err := f.Parse(os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "error parsing command line arguments: %v\n", err)
		os.Exit(1)
	}
}

func main() {
	cfg := Config{}

	cfg.RegisterFlags(flag.CommandLine)

	if cfg.ConfigFile != "" {
		if _, err := os.Stat(cfg.ConfigFile); err == nil {
			if err := loadConfig(cfg.ConfigFile, cfg.ConfigExpandEnv, &cfg); err != nil {
				fmt.Fprintf(os.Stderr, "error loading config from %s: %v\n", cfg.ConfigFile, err)
				os.Exit(1)
			}
		} else if cfg.ConfigFile != "query-tee.yaml" {
			fmt.Fprintf(os.Stderr, "config file not found: %s\n", cfg.ConfigFile)
			os.Exit(1)
		}
	}

	util_log.InitLogger(log.LogfmtFormat, cfg.LogLevel, false, util_log.RateLimitedLoggerCfg{})

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
	proxy, err := querytee.NewProxy(cfg.ProxyConfig, util_log.Logger, mimirReadRoutes(cfg), registry)
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

	proxy.Await()
}

func initTracing() io.Closer {
	var name string
	if otelEnvName := os.Getenv("OTEL_SERVICE_NAME"); otelEnvName != "" {
		name = otelEnvName
	} else if jaegerEnvName := os.Getenv("JAEGER_SERVICE_NAME"); jaegerEnvName != "" {
		name = jaegerEnvName
	} else {
		name = "query-tee"
	}

	trace, err := tracing.NewOTelOrJaegerFromEnv(name, util_log.Logger)
	if err != nil {
		level.Error(util_log.Logger).Log("msg", "Failed to setup tracing", "err", err.Error())
		return nil
	}

	return trace
}

func mimirReadRoutes(cfg Config) []querytee.Route {
	prefix := cfg.PathPrefix

	// Strip trailing slashes.
	for len(prefix) > 0 && prefix[len(prefix)-1] == '/' {
		prefix = prefix[:len(prefix)-1]
	}

	samplesComparator := querytee.NewSamplesComparator(querytee.SampleComparisonOptions{
		Tolerance:              cfg.ProxyConfig.ValueComparisonTolerance,
		UseRelativeError:       cfg.ProxyConfig.UseRelativeError,
		SkipRecentSamples:      cfg.ProxyConfig.SkipRecentSamples,
		SkipSamplesBefore:      model.Time(time.Time(cfg.ProxyConfig.SkipSamplesBefore).UnixMilli()),
		RequireExactErrorMatch: cfg.ProxyConfig.RequireExactErrorMatch,
	})

	var instantQueryTransformers []querytee.RequestTransformer

	if cfg.ProxyConfig.AddMissingTimeParamToInstantQueries {
		instantQueryTransformers = append(instantQueryTransformers, querytee.AddMissingTimeParam)
	}

	return []querytee.Route{
		{Path: prefix + "/api/v1/query", RouteName: "api_v1_query", Methods: []string{"GET", "POST"}, ResponseComparator: samplesComparator, RequestTransformers: instantQueryTransformers},
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

func loadConfig(filename string, expandEnv bool, cfg *Config) error {
	buf, err := os.ReadFile(filename)
	if err != nil {
		return errors.Wrap(err, "Error reading config file")
	}

	if expandEnv {
		buf = expandEnvironmentVariables(buf)
	}

	dec := yaml.NewDecoder(bytes.NewReader(buf))
	dec.KnownFields(true)

	yamlConfig := &querytee.YAMLConfig{}
	if err := dec.Decode(yamlConfig); err != nil {
		return errors.Wrap(err, "Error parsing config file")
	}

	if err := cfg.ProxyConfig.ApplyYAMLConfig(yamlConfig); err != nil {
		return errors.Wrap(err, "Error applying YAML configuration")
	}

	return nil
}

func expandEnvironmentVariables(config []byte) []byte {
	return []byte(os.Expand(string(config), func(key string) string {
		if value, exists := os.LookupEnv(key); exists {
			return value
		}
		return "${" + key + "}"
	}))
}
