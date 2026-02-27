// SPDX-License-Identifier: AGPL-3.0-only

package writetee

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/user"
	"github.com/pkg/errors"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
)

// GRPCBackendConfig holds configuration for gRPC backends.
type GRPCBackendConfig struct {
	MaxRecvMsgSize int
	MaxSendMsgSize int
}

// grpcProxyBackend implements ProxyBackend for HTTPgRPC backends.
type grpcProxyBackend struct {
	name        string
	endpoint    *url.URL
	timeout     time.Duration
	backendType BackendType
	preferred   bool

	conn   *grpc.ClientConn
	client httpgrpc.HTTPClient
}

// NewGRPCProxyBackend creates a new gRPC backend.
func NewGRPCProxyBackend(name string, endpoint *url.URL, timeout time.Duration, preferred bool, backendType BackendType, cfg GRPCBackendConfig) (ProxyBackend, error) {
	// Build the target address from the endpoint.
	// For dns:// scheme, use the host directly with dns resolver.
	target := "dns:///" + endpoint.Host

	dialOptions := []grpc.DialOption{
		grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"round_robin"}`),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                10 * time.Second,
			Timeout:             5 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(cfg.MaxRecvMsgSize),
			grpc.MaxCallSendMsgSize(cfg.MaxSendMsgSize),
		),
		grpc.WithUnaryInterceptor(middleware.ClientUserHeaderInterceptor),
	}

	conn, err := grpc.NewClient(target, dialOptions...)
	if err != nil {
		return nil, errors.Wrap(err, "creating gRPC client connection")
	}

	return &grpcProxyBackend{
		name:        name,
		endpoint:    endpoint,
		timeout:     timeout,
		backendType: backendType,
		preferred:   preferred,
		conn:        conn,
		client:      httpgrpc.NewHTTPClient(conn),
	}, nil
}

func (b *grpcProxyBackend) Name() string {
	return b.name
}

func (b *grpcProxyBackend) Endpoint() *url.URL {
	return b.endpoint
}

func (b *grpcProxyBackend) Preferred() bool {
	return b.preferred
}

func (b *grpcProxyBackend) SetPreferred(preferred bool) {
	b.preferred = preferred
}

func (b *grpcProxyBackend) BackendType() BackendType {
	return b.backendType
}

func (b *grpcProxyBackend) ForwardRequest(ctx context.Context, orig *http.Request, body io.ReadCloser) (time.Duration, int, []byte, http.Header, error) {
	req, err := b.createGRPCRequest(orig, body)
	if err != nil {
		return 0, 0, nil, nil, err
	}

	start := time.Now()
	status, responseBody, headers, err := b.doGRPCRequest(ctx, req)
	elapsed := time.Since(start)

	return elapsed, status, responseBody, headers, err
}

func (b *grpcProxyBackend) createGRPCRequest(orig *http.Request, body io.ReadCloser) (*httpgrpc.HTTPRequest, error) {
	// Read the body.
	bodyBytes, err := io.ReadAll(body)
	if err != nil {
		return nil, errors.Wrap(err, "reading request body")
	}
	if err := body.Close(); err != nil {
		return nil, errors.Wrap(err, "closing request body")
	}

	// Build the URL path with endpoint path prefix and preserve query parameters.
	reqPath := path.Join(b.endpoint.Path, orig.URL.Path)
	if orig.URL.RawQuery != "" {
		reqPath = reqPath + "?" + orig.URL.RawQuery
	}

	// Clone headers for the gRPC request (deep copy to avoid sharing slices with original).
	headers := orig.Header.Clone()

	// Remove headers that are not relevant for HTTPgRPC backends.
	headers.Del("Authorization")
	headers.Del("Accept-Encoding")
	headers.Del("Content-Length")

	return &httpgrpc.HTTPRequest{
		Method:  orig.Method,
		Url:     reqPath,
		Headers: httpgrpc.FromHeader(headers),
		Body:    bodyBytes,
	}, nil
}

func (b *grpcProxyBackend) doGRPCRequest(ctx context.Context, req *httpgrpc.HTTPRequest) (int, []byte, http.Header, error) {
	// Honor the read timeout.
	ctx, cancel := context.WithTimeout(ctx, b.timeout)
	defer cancel()

	// Extract headers that gRPC server-side middleware reads from gRPC metadata and inject
	// them into the outgoing context. These headers are carried in the HTTPRequest protobuf
	// but also need to be present in gRPC metadata for middleware interceptors to read them.
	for _, h := range req.Headers {
		if len(h.Values) == 0 {
			continue
		}
		switch {
		case strings.EqualFold(h.Key, "X-Scope-OrgID"):
			// Inject org ID into context so ClientUserHeaderInterceptor propagates it.
			ctx = user.InjectOrgID(ctx, h.Values[0])
		case strings.EqualFold(h.Key, "X-Cluster"):
			// Inject cluster validation label into outgoing gRPC metadata.
			ctx = metadata.AppendToOutgoingContext(ctx, "x-cluster", h.Values[0])
		}
	}

	// Append HTTPgRPC metadata to the context.
	ctx = httpgrpc.AppendRequestMetadataToContext(ctx, req)

	// Execute the request.
	resp, err := b.client.Handle(ctx, req)
	if err != nil {
		// Try to extract HTTP response from gRPC error.
		resp, ok := httpgrpc.HTTPResponseFromError(err)
		if !ok {
			return 0, nil, nil, errors.Wrap(err, "executing gRPC backend request")
		}
		// Got an HTTP response embedded in the error.
		headers := make(http.Header)
		httpgrpc.ToHeader(resp.Headers, headers)
		return int(resp.Code), resp.Body, headers, nil
	}

	headers := make(http.Header)
	httpgrpc.ToHeader(resp.Headers, headers)
	return int(resp.Code), resp.Body, headers, nil
}

func (b *grpcProxyBackend) Close() error {
	if b.conn != nil {
		return b.conn.Close()
	}
	return nil
}
