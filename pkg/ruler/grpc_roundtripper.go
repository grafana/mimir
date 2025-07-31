// SPDX-License-Identifier: AGPL-3.0-only

package ruler

import (
	"net/http"
	"net/url"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/middleware"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"

	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/httpgrpcutil"
)

// dialQueryFrontend creates and initializes a new httpgrpc.HTTPClient taking a QueryFrontendConfig configuration.
func dialQueryFrontendGRPC(cfg QueryFrontendConfig, prometheusHTTPPrefix string, reg prometheus.Registerer, logger log.Logger) (http.RoundTripper, *url.URL, error) {
	invalidClusterValidation := util.NewRequestInvalidClusterValidationLabelsTotalCounter(reg, "ruler-query-frontend", util.GRPCProtocol)
	opts, err := cfg.GRPCClientConfig.DialOption(
		[]grpc.UnaryClientInterceptor{
			middleware.ClientUserHeaderInterceptor,
		},
		nil,
		util.NewInvalidClusterValidationReporter(cfg.GRPCClientConfig.ClusterValidation.Label, invalidClusterValidation, logger),
	)
	if err != nil {
		return nil, nil, err
	}
	opts = append(opts, grpc.WithDefaultServiceConfig(serviceConfig))
	opts = append(opts, grpc.WithStatsHandler(otelgrpc.NewClientHandler()))

	// nolint:staticcheck // grpc.Dial() has been deprecated; we'll address it before upgrading to gRPC 2.
	conn, err := grpc.Dial(cfg.Address, opts...)
	if err != nil {
		return nil, nil, err
	}
	// GRPC adapter only uses the Path of the URL; no need to set other fields
	return httpgrpcutil.AdaptHTTPGrpcClientToHTTPRoundTripper(httpgrpc.NewHTTPClient(conn)), &url.URL{Path: prometheusHTTPPrefix}, nil
}
