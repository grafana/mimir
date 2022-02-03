// SPDX-License-Identifier: AGPL-3.0-only

package api

import (
	"flag"
)

type Config struct {
	ResponseCompression bool `yaml:"response_compression_enabled"`

	AlertmanagerHTTPPrefix string `yaml:"alertmanager_http_prefix"`
	PrometheusHTTPPrefix   string `yaml:"prometheus_http_prefix"`

	// The following configs are injected by the upstream caller.

	// This allows downstream projects to wrap the distributor push function
	// and access the deserialized write requests before/after they are pushed.

	// The CustomConfigHandler allows for providing a different handler for the
	// `/config` endpoint. If this field is set _before_ the API module is
	// initialized, the custom config handler will be used instead of
	// DefaultConfigHandler.
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&cfg.ResponseCompression, "api.response-compression-enabled", false, "Use GZIP compression for API responses. Some endpoints serve large YAML or JSON blobs which can benefit from compression.")
	cfg.RegisterFlagsWithPrefix("", f)
}
func (cfg *Config) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.StringVar(&cfg.AlertmanagerHTTPPrefix, prefix+"http.alertmanager-http-prefix", "/alertmanager", "HTTP URL path under which the Alertmanager ui and api will be served.")
	f.StringVar(&cfg.PrometheusHTTPPrefix, prefix+"http.prometheus-http-prefix", "/prometheus", "HTTP URL path under which the Prometheus api will be served.")
}
