// SPDX-License-Identifier: AGPL-3.0-only

package httpgrpcutil

import (
	"bytes"
	"io"
	"net/http"

	otgrpc "github.com/opentracing-contrib/go-grpc"
	ot "github.com/opentracing/opentracing-go"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/middleware"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// NewGRPCClient returns a new gRPC client for the address, with insecure credentials and an opentracing interceptor configures.
func NewGRPCClient(address string) (httpgrpc.HTTPClient, error) {
	dialOptions := []grpc.DialOption{
		grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"round_robin"}`),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithChainUnaryInterceptor(
			otgrpc.OpenTracingClientInterceptor(ot.GlobalTracer()),
			middleware.ClientUserHeaderInterceptor,
		),
	}

	conn, err := grpc.Dial(address, dialOptions...)
	if err != nil {
		return nil, err
	}

	return httpgrpc.NewHTTPClient(conn), nil
}

// GrpcToHTTPResponse returns an http.Response for the grpcResponse.
func GrpcToHTTPResponse(grpcResponse *httpgrpc.HTTPResponse) *http.Response {
	header := make(map[string][]string)
	for _, h := range grpcResponse.Headers {
		header[h.Key] = h.Values
	}
	return &http.Response{
		Header:     header,
		StatusCode: int(grpcResponse.GetCode()),
		Body:       io.NopCloser(bytes.NewReader(grpcResponse.Body)),
	}
}
