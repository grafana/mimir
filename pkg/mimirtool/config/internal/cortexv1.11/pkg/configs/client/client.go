// SPDX-License-Identifier: AGPL-3.0-only

package client

import (
	"errors"
	"flag"
	"time"

	"github.com/grafana/dskit/flagext"

	tls_cfg "github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/util/tls"
)

type Config struct {
	ConfigsAPIURL flagext.URLValue     `yaml:"configs_api_url"`
	ClientTimeout time.Duration        `yaml:"client_timeout"` // HTTP timeout duration for requests made to the Weave Cloud configs service.
	TLS           tls_cfg.ClientConfig `yaml:",inline"`
}

var (
	errBadURL = errors.New("configs_api_url is not set or valid")
)

func (cfg *Config) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.Var(&cfg.ConfigsAPIURL, prefix+"configs.url", "URL of configs API server.")
	f.DurationVar(&cfg.ClientTimeout, prefix+"configs.client-timeout", 5*time.Second, "Timeout for requests to Weave Cloud configs service.")
	cfg.TLS.RegisterFlagsWithPrefix(prefix+"configs", f)
}
