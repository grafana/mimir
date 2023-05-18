// SPDX-License-Identifier: AGPL-3.0-only

package httpgrpcutil

import (
	"bytes"
	"io"
	"net/http"

	otgrpc "github.com/opentracing-contrib/go-grpc"
	ot "github.com/opentracing/opentracing-go"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/httpgrpc/server"
	"github.com/weaveworks/common/middleware"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// NewGRPCClient returns a new gRPC client for the address, with insecure credentials and an opentracing interceptor configured.
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

func ToGRPCRequest(req *http.Request) (*httpgrpc.HTTPRequest, error) {
	grpcReq, err := server.HTTPRequest(req)
	if err != nil {
		return nil, err
	}
	grpcReq.Url = req.URL.String()
	return grpcReq, nil
}

// ToHTTPResponse returns an *http.Response for the resp.
func ToHTTPResponse(resp *httpgrpc.HTTPResponse) *http.Response {
	return &http.Response{
		Header:     ToHTTPHeader(resp.Headers, make(http.Header)),
		StatusCode: int(resp.GetCode()),
		Body:       io.NopCloser(bytes.NewReader(resp.Body)),
	}
}

// ToHTTPHeader convert grpcHeaders to the httpHeader and returns the httpHeader.
func ToHTTPHeader(grpcHeaders []*httpgrpc.Header, httpHeader http.Header) http.Header {
	for _, h := range grpcHeaders {
		httpHeader[h.Key] = h.Values
	}
	return httpHeader
}

// ToGRPCHeaders convert the header to a []*httpgrpc.Header.
func ToGRPCHeaders(header http.Header) []*httpgrpc.Header {
	result := make([]*httpgrpc.Header, 0, len(header))
	for k, vs := range header {
		result = append(result, &httpgrpc.Header{
			Key:    k,
			Values: vs,
		})
	}
	return result
}
