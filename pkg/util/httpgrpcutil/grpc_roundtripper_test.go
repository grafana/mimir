// SPDX-License-Identifier: AGPL-3.0-only

package httpgrpcutil

import (
	"net/http"
	"testing"

	"github.com/grafana/dskit/httpgrpc"
	"github.com/stretchr/testify/assert"
)

func TestSetContentLength(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		grpcBody           []byte
		responseHeaders    []*httpgrpc.Header
		expectedContentLen int64
	}{
		"non-empty gRPC body uses body length": {
			grpcBody:           []byte("hello"),
			expectedContentLen: 5,
		},
		"empty gRPC body with valid Content-Length header uses header value": {
			grpcBody:           nil,
			responseHeaders:    []*httpgrpc.Header{{Key: "Content-Length", Values: []string{"42"}}},
			expectedContentLen: 42,
		},
		"empty gRPC body with zero Content-Length header": {
			grpcBody:           nil,
			responseHeaders:    []*httpgrpc.Header{{Key: "Content-Length", Values: []string{"0"}}},
			expectedContentLen: 0,
		},
		"empty gRPC body with invalid Content-Length header falls back to -1": {
			grpcBody:           nil,
			responseHeaders:    []*httpgrpc.Header{{Key: "Content-Length", Values: []string{"not-a-number"}}},
			expectedContentLen: -1,
		},
		"empty gRPC body with negative Content-Length header falls back to -1": {
			grpcBody:           nil,
			responseHeaders:    []*httpgrpc.Header{{Key: "Content-Length", Values: []string{"-1000"}}},
			expectedContentLen: -1,
		},
		"empty gRPC body with no Content-Length header falls back to -1": {
			grpcBody:           nil,
			expectedContentLen: -1,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			grpcResp := &httpgrpc.HTTPResponse{
				Body:    tc.grpcBody,
				Headers: tc.responseHeaders,
			}

			httpResp := &http.Response{Header: http.Header{}}
			httpgrpc.ToHeader(grpcResp.Headers, httpResp.Header)

			setContentLength(grpcResp, httpResp)

			assert.Equal(t, tc.expectedContentLen, httpResp.ContentLength, "unexpected ContentLength")
		})
	}
}
