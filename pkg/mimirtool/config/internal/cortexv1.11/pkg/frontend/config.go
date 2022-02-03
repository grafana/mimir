// SPDX-License-Identifier: AGPL-3.0-only

package frontend

import (
	"flag"

	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/frontend/transport"
	v1 "github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/frontend/v1"
	v2 "github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/frontend/v2"
)

type CombinedFrontendConfig struct {
	Handler    transport.HandlerConfig `yaml:",inline"`
	FrontendV1 v1.Config               `yaml:",inline"`
	FrontendV2 v2.Config               `yaml:",inline"`

	DownstreamURL string `yaml:"downstream_url"`
}

func (cfg *CombinedFrontendConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.Handler.RegisterFlags(f)
	cfg.FrontendV1.RegisterFlags(f)
	cfg.FrontendV2.RegisterFlags(f)

	f.StringVar(&cfg.DownstreamURL, "frontend.downstream-url", "", "URL of downstream Prometheus.")
}
