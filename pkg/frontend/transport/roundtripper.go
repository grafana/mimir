// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/frontend/transport/roundtripper.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package transport

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strconv"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/middleware"
)

// GrpcRoundTripper is similar to http.RoundTripper, but works with HTTP requests converted to protobuf messages.
type GrpcRoundTripper interface {
	RoundTripGRPC(context.Context, *httpgrpc.HTTPRequest) (*httpgrpc.HTTPResponse, io.ReadCloser, error)
}

func AdaptGrpcRoundTripperToHTTPRoundTripper(r GrpcRoundTripper, cluster string, InvalidClusterValidationReporter middleware.InvalidClusterValidationReporter, logger log.Logger) http.RoundTripper {
	if r == nil {
		return nil
	}

	rt := &grpcRoundTripperAdapter{
		roundTripper: r,
		cluster:      cluster,
		logger:       logger,
	}
	if cluster == "" {
		return rt
	}

	return middleware.ClusterValidationRoundTripper(cluster, InvalidClusterValidationReporter, rt)
}

// This adapter wraps GrpcRoundTripper and converted it into http.RoundTripper
type grpcRoundTripperAdapter struct {
	roundTripper GrpcRoundTripper
	cluster      string
	logger       log.Logger
}

type buffer struct {
	buff []byte
	io.ReadCloser
}

func (b *buffer) Bytes() []byte {
	return b.buff
}

func (a *grpcRoundTripperAdapter) RoundTrip(r *http.Request) (*http.Response, error) {
	req, err := httpgrpc.FromHTTPRequestWithCluster(r, a.cluster, nil)
	if err != nil {
		level.Warn(a.logger).Log("msg", "grpcRoundTripperAdapter has failed while trying to call httpgrpc.FromHTTPRequestWithCluster", "err", err)
		return nil, err
	}

	resp, body, err := a.roundTripper.RoundTripGRPC(r.Context(), req)
	if err != nil {
		var ok bool
		resp, ok = httpgrpc.HTTPResponseFromError(err)
		level.Warn(a.logger).Log("msg", "grpcRoundTripperAdapter has failed while trying to call RoundTripGRPC()", "httpgrpc error", ok, "err", err)
		if !ok {
			return nil, err
		}
	}

	var respBody io.ReadCloser
	if body != nil {
		respBody = body
	} else {
		respBody = &buffer{buff: resp.Body, ReadCloser: io.NopCloser(bytes.NewReader(resp.Body))}
	}

	httpResp := &http.Response{
		StatusCode: int(resp.Code),
		Body:       respBody,
		Header:     http.Header{},
	}
	for _, h := range resp.Headers {
		httpResp.Header[h.Key] = h.Values
	}

	contentLength := -1
	if len(resp.Body) > 0 {
		contentLength = len(resp.Body)
	} else if l := httpResp.Header.Get("Content-Length"); l != "" {
		cl, err := strconv.Atoi(l)
		if err != nil {
			contentLength = cl
		}
	}
	httpResp.ContentLength = int64(contentLength)

	return httpResp, nil
}
