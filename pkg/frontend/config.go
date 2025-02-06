// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/frontend/config.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package frontend

import (
	"flag"
	"net/http"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/netutil"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware"
	"github.com/grafana/mimir/pkg/frontend/transport"
	v1 "github.com/grafana/mimir/pkg/frontend/v1"
	v2 "github.com/grafana/mimir/pkg/frontend/v2"
	"github.com/grafana/mimir/pkg/scheduler/schedulerdiscovery"
)

// CombinedFrontendConfig combines several configuration options together to preserve backwards compatibility.
type CombinedFrontendConfig struct {
	Handler    transport.HandlerConfig `yaml:",inline"`
	FrontendV1 v1.Config               `yaml:",inline"`
	FrontendV2 v2.Config               `yaml:",inline"`

	QueryMiddleware querymiddleware.Config `yaml:",inline"`

	DownstreamURL string `yaml:"downstream_url" category:"advanced"`

	Cluster string `yaml:"-"`
}

func (cfg *CombinedFrontendConfig) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	cfg.Handler.RegisterFlags(f)
	cfg.FrontendV1.RegisterFlags(f)
	cfg.FrontendV2.RegisterFlags(f, logger)
	cfg.QueryMiddleware.RegisterFlags(f)

	f.StringVar(&cfg.DownstreamURL, "query-frontend.downstream-url", "", "URL of downstream Prometheus.")
}

func (cfg *CombinedFrontendConfig) Validate() error {
	if err := cfg.FrontendV2.Validate(); err != nil {
		return err
	}
	if err := cfg.QueryMiddleware.Validate(); err != nil {
		return err
	}
	return nil
}

// InitFrontend initializes frontend (either V1 -- without scheduler, or V2 -- with scheduler) or no frontend at
// all if downstream Prometheus URL is used instead.
//
// Returned RoundTripper can be wrapped in more round-tripper middlewares, and then eventually registered
// into HTTP server using the Handler from this package. Returned RoundTripper is always non-nil
// (if there are no errors), and it uses the returned frontend (if any).
func InitFrontend(cfg CombinedFrontendConfig, v1Limits v1.Limits, v2Limits v2.Limits, grpcListenPort int, log log.Logger, reg prometheus.Registerer, codec querymiddleware.Codec, cluster string) (http.RoundTripper, *v1.Frontend, *v2.Frontend, error) {
	switch {
	case cfg.DownstreamURL != "":
		// If the user has specified a downstream Prometheus, then we should use that.
		rt, err := NewDownstreamRoundTripper(cfg.DownstreamURL)
		return rt, nil, nil, err

	case cfg.FrontendV2.SchedulerAddress != "" || cfg.FrontendV2.QuerySchedulerDiscovery.Mode == schedulerdiscovery.ModeRing:
		// Query-scheduler is enabled when its address is configured or ring-based service discovery is configured.
		if cfg.FrontendV2.Addr == "" {
			addr, err := netutil.GetFirstAddressOf(cfg.FrontendV2.InfNames, log, cfg.FrontendV2.EnableIPv6)
			if err != nil {
				return nil, nil, nil, errors.Wrap(err, "failed to get frontend address")
			}

			cfg.FrontendV2.Addr = addr
		}

		if cfg.FrontendV2.Port == 0 {
			cfg.FrontendV2.Port = grpcListenPort
		}

		fr, err := v2.NewFrontend(cfg.FrontendV2, v2Limits, log, reg, codec, cluster)
		return transport.AdaptGrpcRoundTripperToHTTPRoundTripper(fr, cfg.Cluster), nil, fr, err

	default:
		// No scheduler = use original frontend.
		fr, err := v1.New(cfg.FrontendV1, v1Limits, log, reg)
		if err != nil {
			return nil, nil, nil, err
		}
		return transport.AdaptGrpcRoundTripperToHTTPRoundTripper(fr, cfg.Cluster), fr, nil, nil
	}
}
