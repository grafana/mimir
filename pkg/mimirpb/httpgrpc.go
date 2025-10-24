// SPDX-License-Identifier: AGPL-3.0-only

package mimirpb

import "github.com/grafana/dskit/httpgrpc"

// HTTPRequest is a wrapper around httpgrpc that also embeds BufferHolder.
//
// Use this if you're using HTTPRequest as a request message in a service.
type HTTPRequest struct {
	BufferHolder
	httpgrpc.HTTPRequest
}

// HTTPResponse is a wrapper around httpgrpc that also embeds BufferHolder.
//
// Use this if you're using HTTPResponse as a request message in a service.
type HTTPResponse struct {
	BufferHolder
	httpgrpc.HTTPResponse
}
