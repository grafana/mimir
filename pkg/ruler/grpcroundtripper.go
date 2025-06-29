package ruler

import (
	"bytes"
	"io"
	"net/http"
	"net/url"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/mimir/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
)

// buffer exists to avoid extra copies of body data when adaping httpgrpc.HTTPClient to an http.RoundTripper
type buffer struct {
	buff []byte
	io.ReadCloser
}

func wrapBuffer(b []byte) io.ReadCloser {
	return &buffer{buff: b, ReadCloser: io.NopCloser(bytes.NewReader(b))}
}

func readAll(r io.Reader) ([]byte, error) {
	if b, ok := r.(*buffer); ok {
		return b.buff, nil
	}

	return io.ReadAll(r)
}

type grpcRoundTripper struct {
	client httpgrpc.HTTPClient
}

func newGrpcRoundTripper(client httpgrpc.HTTPClient) *grpcRoundTripper {
	return &grpcRoundTripper{client: client}
}

func (g *grpcRoundTripper) RoundTrip(httpReq *http.Request) (*http.Response, error) {
	body, err := readAll(httpReq.Body)
	if err != nil {
		return nil, err
	}

	grpcRequest := &httpgrpc.HTTPRequest{
		Method:  httpReq.Method,
		Url:     httpReq.URL.Path,
		Body:    body,
		Headers: httpgrpc.FromHeader(httpReq.Header),
	}

	grpcResp, err := g.client.Handle(httpReq.Context(), grpcRequest)
	if err != nil {
		return nil, err
	}

	httpResp := &http.Response{
		StatusCode: int(grpcResp.Code),
		Body:       wrapBuffer(grpcResp.Body),
		Header:     make(http.Header),
	}
	httpgrpc.ToHeader(grpcResp.Headers, httpResp.Header)

	return httpResp, nil
}

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
	return newGrpcRoundTripper(httpgrpc.NewHTTPClient(conn)), &url.URL{Path: prometheusHTTPPrefix}, nil
}
