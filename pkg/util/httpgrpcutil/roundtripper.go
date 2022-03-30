// SPDX-License-Identifier: AGPL-3.0-only

package httpgrpcutil

import (
	"context"

	"github.com/weaveworks/common/httpgrpc"
)

// RoundTripper is similar to http.RoundTripper, but works with HTTP requests converted to protobuf messages.
type RoundTripper interface {
	RoundTrip(ctx context.Context, req *httpgrpc.HTTPRequest) (*httpgrpc.HTTPResponse, error)
}

// Transport forwards httpgrpc requests to a remote querier service.
type Transport struct {
	Client httpgrpc.HTTPClient
}

// RoundTrip satisfies RoundTripper interface.
func (h *Transport) RoundTrip(ctx context.Context, req *httpgrpc.HTTPRequest) (*httpgrpc.HTTPResponse, error) {
	return h.Client.Handle(ctx, req)
}
