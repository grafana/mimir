// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/frontend/config.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package frontend

import (
	"flag"
	"fmt"
	"net/http"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/clusterutil"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/netutil"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware"
	"github.com/grafana/mimir/pkg/frontend/transport"
	v1 "github.com/grafana/mimir/pkg/frontend/v1"
	v2 "github.com/grafana/mimir/pkg/frontend/v2"
	"github.com/grafana/mimir/pkg/querier"
	"github.com/grafana/mimir/pkg/scheduler/schedulerdiscovery"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/httpgrpcutil"
)

// CombinedFrontendConfig combines several configuration options together to preserve backwards compatibility.
type CombinedFrontendConfig struct {
	Handler    transport.HandlerConfig `yaml:",inline"`
	FrontendV1 v1.Config               `yaml:",inline"`
	FrontendV2 v2.Config               `yaml:",inline"`

	QueryMiddleware querymiddleware.Config `yaml:",inline"`

	DownstreamURL string `yaml:"downstream_url" category:"advanced"`

	ClusterValidationConfig clusterutil.ClusterValidationConfig `yaml:"client_cluster_validation" category:"experimental"`

	QueryEngine               string `yaml:"query_engine" category:"experimental"`
	EnableQueryEngineFallback bool   `yaml:"enable_query_engine_fallback" category:"experimental"`
}

func (cfg *CombinedFrontendConfig) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	cfg.Handler.RegisterFlags(f)
	cfg.FrontendV1.RegisterFlags(f)
	cfg.FrontendV2.RegisterFlags(f, logger)
	cfg.QueryMiddleware.RegisterFlags(f)
	cfg.ClusterValidationConfig.RegisterFlagsWithPrefix("query-frontend.client-cluster-validation.", f)

	f.StringVar(&cfg.DownstreamURL, "query-frontend.downstream-url", "", "URL of downstream Prometheus.")
	f.StringVar(&cfg.QueryEngine, "query-frontend.query-engine", querier.PrometheusEngine, fmt.Sprintf("Query engine to use, either '%v' or '%v'", querier.PrometheusEngine, querier.MimirEngine))
	f.BoolVar(&cfg.EnableQueryEngineFallback, "query-frontend.enable-query-engine-fallback", true, "If set to true and the Mimir query engine is in use, fall back to using the Prometheus query engine for any queries not supported by the Mimir query engine.")
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
func InitFrontend(
	cfg CombinedFrontendConfig,
	v1Limits v1.Limits,
	v2Limits v2.Limits,
	grpcListenPort int,
	log log.Logger,
	reg prometheus.Registerer,
	codec querymiddleware.Codec,
) (http.RoundTripper, *v1.Frontend, *v2.Frontend, error) {
	switch {
	case cfg.DownstreamURL != "":
		// If the user has specified a downstream Prometheus, then we should use that.
		rt, err := NewDownstreamRoundTripper(cfg.DownstreamURL)
		return httpRoundTripper(cfg, rt, reg, log), nil, nil, err

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

		fr, err := v2.NewFrontend(cfg.FrontendV2, v2Limits, log, reg, codec)
		return grpcToHTTPRoundTripper(cfg, fr, reg, log), nil, fr, err

	default:
		// No scheduler = use original frontend.
		fr, err := v1.New(cfg.FrontendV1, v1Limits, log, reg)
		if err != nil {
			return nil, nil, nil, err
		}
		return grpcToHTTPRoundTripper(cfg, fr, reg, log), fr, nil, nil
	}
}

func invalidClusterValidationReporter(cfg CombinedFrontendConfig, reg prometheus.Registerer, logger log.Logger) middleware.InvalidClusterValidationReporter {
	invalidClusterValidation := util.NewRequestInvalidClusterValidationLabelsTotalCounter(reg, "querier", util.HTTPProtocol)
	return util.NewInvalidClusterValidationReporter(cfg.ClusterValidationConfig.Label, invalidClusterValidation, logger)
}

func httpRoundTripper(cfg CombinedFrontendConfig, rt http.RoundTripper, reg prometheus.Registerer, logger log.Logger) http.RoundTripper {
	if rt == nil {
		return nil
	}
	if cfg.ClusterValidationConfig.Label != "" {
		return middleware.ClusterValidationRoundTripper(cfg.ClusterValidationConfig.Label, invalidClusterValidationReporter(cfg, reg, logger), rt)
	}
	return rt
}

func grpcToHTTPRoundTripper(cfg CombinedFrontendConfig, grpcRoundTripper httpgrpcutil.GrpcRoundTripper, reg prometheus.Registerer, logger log.Logger) http.RoundTripper {
	if grpcRoundTripper == nil {
		return nil
	}
	if cfg.ClusterValidationConfig.Label != "" {
		return middleware.ClusterValidationRoundTripper(cfg.ClusterValidationConfig.Label, invalidClusterValidationReporter(cfg, reg, logger), httpgrpcutil.AdaptGrpcRoundTripperToHTTPRoundTripper(grpcRoundTripper))
	}
	return httpgrpcutil.AdaptGrpcRoundTripperToHTTPRoundTripper(grpcRoundTripper)
}
