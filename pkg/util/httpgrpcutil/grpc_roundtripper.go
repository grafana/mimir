// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/frontend/transport/roundtripper.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package httpgrpcutil

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"

	"github.com/grafana/dskit/httpgrpc"
)

type Buffer interface {
	Bytes() []byte
}

// GrpcRoundTripper is similar to http.RoundTripper, but works with HTTP requests converted to protobuf messages.
type GrpcRoundTripper interface {
	RoundTripGRPC(context.Context, *httpgrpc.HTTPRequest) (*httpgrpc.HTTPResponse, io.ReadCloser, error)
}

func AdaptGrpcRoundTripperToHTTPRoundTripper(r GrpcRoundTripper) http.RoundTripper {
	return &grpcRoundTripperAdapter{roundTripper: r}
}

func AdaptHTTPGrpcClientToHTTPRoundTripper(c httpgrpc.HTTPClient) http.RoundTripper {
	return &httpGrpcClientAdapter{client: c}
}

// This adapter wraps GrpcRoundTripper and converted it into http.RoundTripper
type grpcRoundTripperAdapter struct {
	roundTripper GrpcRoundTripper
}

// This adapter wraps httpgrp.HTTPClient and converted it into GrpcRoundTripper
type httpGrpcClientAdapter struct {
	client httpgrpc.HTTPClient
}

type buffer struct {
	buff []byte
	io.ReadCloser
}

func newBuffer(b []byte) io.ReadCloser {
	return &buffer{buff: b, ReadCloser: io.NopCloser(bytes.NewReader(b))}
}

func (b *buffer) Bytes() []byte {
	return b.buff
}

func (a *grpcRoundTripperAdapter) RoundTrip(r *http.Request) (*http.Response, error) {
	req, err := httpgrpc.FromHTTPRequest(r)
	if err != nil {
		return nil, err
	}

	resp, body, err := a.roundTripper.RoundTripGRPC(r.Context(), req)
	if err != nil {
		var ok bool
		if resp, ok = httpgrpc.HTTPResponseFromError(err); !ok {
			return nil, err
		}
	}

	var respBody io.ReadCloser
	if body != nil {
		respBody = body
	} else {
		respBody = newBuffer(resp.Body)
	}

	httpResp := &http.Response{
		StatusCode: int(resp.Code),
		Body:       respBody,
		Header:     http.Header{},
	}
	httpgrpc.ToHeader(resp.Headers, httpResp.Header)

	setContentLenght(resp, httpResp)

	return httpResp, nil
}

func (a *httpGrpcClientAdapter) RoundTrip(r *http.Request) (*http.Response, error) {
	req, err := httpgrpc.FromHTTPRequest(r)
	if err != nil {
		return nil, err
	}

	// for client requests, use the request Path as GRPC Url, as RequestURI is not set
	if r.URL == nil || r.URL.Path == "" {
		return nil, errors.New("Error mapping HTTP to GRPC (missing URL or URL.Path)")
	}
	req.Url = r.URL.Path

	resp, err := a.client.Handle(r.Context(), req)
	if err != nil {
		return nil, err
	}

	httpResp := &http.Response{
		StatusCode: int(resp.Code),
		Body:       newBuffer(resp.Body),
		Header:     http.Header{},
	}
	httpgrpc.ToHeader(resp.Headers, httpResp.Header)

	setContentLenght(resp, httpResp)

	return httpResp, nil
}

func setContentLenght(resp *httpgrpc.HTTPResponse, httpResp *http.Response) {
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
}

func ReadAll(r io.Reader) ([]byte, error) {
	if b, ok := r.(Buffer); ok {
		return b.Bytes(), nil
	}

	return io.ReadAll(r)
}
