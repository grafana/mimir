// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/frontend/config.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package frontend

import (
	"flag"

	"github.com/go-kit/log"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware"
	"github.com/grafana/mimir/pkg/frontend/transport"
	v1 "github.com/grafana/mimir/pkg/frontend/v1"
	v2 "github.com/grafana/mimir/pkg/frontend/v2"
)

// CombinedFrontendConfig combines several configuration options together to preserve backwards compatibility.
type CombinedFrontendConfig struct {
	Handler    transport.HandlerConfig `yaml:",inline"`
	FrontendV1 v1.Config               `yaml:",inline"`
	FrontendV2 v2.Config               `yaml:",inline"`

	QueryMiddleware querymiddleware.Config `yaml:",inline"`

	DownstreamURL string `yaml:"downstream_url" category:"advanced"`
}

func (cfg *CombinedFrontendConfig) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	cfg.Handler.RegisterFlags(f)
	cfg.FrontendV1.RegisterFlags(f)
	cfg.FrontendV2.RegisterFlags(f, logger)
	cfg.QueryMiddleware.RegisterFlags(f)

	f.StringVar(&cfg.DownstreamURL, "query-frontend.downstream-url", "", "URL of downstream Prometheus.")
}

func (cfg *CombinedFrontendConfig) Validate(log log.Logger) error {
	if err := cfg.FrontendV2.Validate(log); err != nil {
		return err
	}
	if err := cfg.QueryMiddleware.Validate(); err != nil {
		return err
	}
	return nil
}
