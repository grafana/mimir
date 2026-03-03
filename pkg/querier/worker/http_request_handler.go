// SPDX-License-Identifier: AGPL-3.0-only

package worker

import (
	"context"
	"net/http"

	"github.com/grafana/dskit/httpgrpc"
)

// HTTPRequestHandler wraps an http.Handler as a RequestHandler, supporting both
// buffered responses (via the embedded httpgrpc server) and true streaming responses
// (via ServeHTTPWithWriter, which bypasses the internal httptest.Recorder).
//
// Use NewHTTPRequestHandler in place of httpgrpc/server.NewServer() when the
// handler may produce streaming search responses.
type HTTPRequestHandler struct {
	// server handles the buffered path, preserving all existing behaviour
	// (special header handling, 4xx-as-error conversion, etc.).
	server RequestHandler
	// handler is the raw http.Handler used for the streaming path.
	handler http.Handler
}

// NewHTTPRequestHandler creates an HTTPRequestHandler that uses server for the
// normal buffered path and handler for the streaming path.
//
// Typically called as:
//
//	s := httpgrpc_server.NewServer(router, httpgrpc_server.WithReturn4XXErrors)
//	NewHTTPRequestHandler(s, router)
func NewHTTPRequestHandler(server RequestHandler, handler http.Handler) *HTTPRequestHandler {
	return &HTTPRequestHandler{server: server, handler: handler}
}

// Handle implements RequestHandler using the buffered httpgrpc path.
func (h *HTTPRequestHandler) Handle(ctx context.Context, r *httpgrpc.HTTPRequest) (*httpgrpc.HTTPResponse, error) {
	return h.server.Handle(ctx, r)
}

// ServeHTTPWithWriter serves the HTTP request directly to the provided ResponseWriter,
// bypassing the internal httptest.Recorder. This is used by the streaming path in
// the scheduler processor to allow http.Flusher.Flush() calls to send data to the
// gRPC stream incrementally.
func (h *HTTPRequestHandler) ServeHTTPWithWriter(ctx context.Context, r *httpgrpc.HTTPRequest, w http.ResponseWriter) error {
	req, err := httpgrpc.ToHTTPRequest(ctx, r)
	if err != nil {
		return err
	}
	h.handler.ServeHTTP(w, req)
	return nil
}

// streamingHTTPHandler is the optional interface that a RequestHandler may implement
// to support true HTTP response streaming without buffering the entire response.
// scheduler_processor.runHttpRequest type-asserts to this interface for search paths.
type streamingHTTPHandler interface {
	ServeHTTPWithWriter(ctx context.Context, r *httpgrpc.HTTPRequest, w http.ResponseWriter) error
}

// Ensure HTTPRequestHandler satisfies both interfaces at compile time.
var _ RequestHandler = (*HTTPRequestHandler)(nil)
var _ streamingHTTPHandler = (*HTTPRequestHandler)(nil)
