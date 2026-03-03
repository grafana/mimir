// SPDX-License-Identifier: AGPL-3.0-only

package worker

import (
	"bytes"
	"context"
	"fmt"
	"net/http"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/grpcutil"
	"github.com/grafana/dskit/httpgrpc"

	"github.com/grafana/mimir/pkg/frontend/v2/frontendv2pb"
	querier_stats "github.com/grafana/mimir/pkg/querier/stats"
)

// grpcStreamingResponseWriter is an http.ResponseWriter and http.Flusher that sends
// HTTP response data directly to a QueryResultStream gRPC stream as it is produced,
// without buffering the entire response first.
//
// On the first Flush() call, it opens the gRPC stream and sends the HTTP status code
// and headers as a QueryResultStreamRequest_Metadata message. On each subsequent
// Flush() call, any buffered body bytes are sent as a QueryResultStreamRequest_Body
// message. After the handler returns, CloseStream() must be called to flush any
// remaining data and close the gRPC stream.
//
// If the HTTP handler returns without ever calling Flush() (e.g. it wrote an error
// response and returned early), StreamOpened() returns false and the accumulated
// response can be retrieved via AsHTTPResponse() for normal (non-streaming) delivery.
type grpcStreamingResponseWriter struct {
	// Set at construction; immutable afterwards.
	frontendCtx     context.Context // WithoutCancel(reqCtx): used for gRPC operations
	reqCtx          context.Context // original request context: checked for cancellation
	queryID         uint64
	frontendAddress string
	clientPool      frontendClientPool
	logger          log.Logger
	stats           *querier_stats.SafeStats
	maxMessageSize  int // 0 means no limit

	// HTTP response accumulation. Written by WriteHeader/Write before the first Flush.
	header     http.Header
	statusCode int
	buf        bytes.Buffer

	// gRPC stream state. nil until the first Flush opens the stream.
	stream frontendv2pb.FrontendForQuerier_QueryResultStreamClient

	// streamOpened is true once the gRPC stream has been opened and metadata sent.
	streamOpened bool
	// failed is true if a gRPC send failed; no further sends are attempted.
	failed bool
}

// grpcStreamingResponseWriter implements both http.ResponseWriter and http.Flusher.
var (
	_ http.ResponseWriter = (*grpcStreamingResponseWriter)(nil)
	_ http.Flusher        = (*grpcStreamingResponseWriter)(nil)
)

func newGrpcStreamingResponseWriter(
	frontendCtx context.Context,
	reqCtx context.Context,
	queryID uint64,
	frontendAddress string,
	clientPool frontendClientPool,
	logger log.Logger,
	stats *querier_stats.SafeStats,
	maxMessageSize int,
) *grpcStreamingResponseWriter {
	return &grpcStreamingResponseWriter{
		frontendCtx:     frontendCtx,
		reqCtx:          reqCtx,
		queryID:         queryID,
		frontendAddress: frontendAddress,
		clientPool:      clientPool,
		logger:          logger,
		stats:           stats,
		maxMessageSize:  maxMessageSize,
		header:          make(http.Header),
	}
}

// Header implements http.ResponseWriter.
func (w *grpcStreamingResponseWriter) Header() http.Header {
	return w.header
}

// WriteHeader implements http.ResponseWriter.
func (w *grpcStreamingResponseWriter) WriteHeader(statusCode int) {
	if w.statusCode == 0 {
		w.statusCode = statusCode
	}
}

// Write implements http.ResponseWriter. Data is buffered until Flush() is called.
func (w *grpcStreamingResponseWriter) Write(p []byte) (int, error) {
	if w.statusCode == 0 {
		w.statusCode = http.StatusOK
	}
	return w.buf.Write(p)
}

// Flush implements http.Flusher. On the first call it opens the gRPC stream and
// sends the response metadata. On each call it drains any buffered body bytes into
// a gRPC body chunk.
func (w *grpcStreamingResponseWriter) Flush() {
	if w.failed {
		return
	}
	if w.statusCode == 0 {
		w.statusCode = http.StatusOK
	}

	if !w.streamOpened {
		if err := w.openStreamAndSendMetadata(); err != nil {
			level.Warn(w.logger).Log("msg", "failed to open gRPC stream for streaming search response", "err", err)
			w.failed = true
			return
		}
		w.streamOpened = true
	}

	if w.buf.Len() > 0 {
		if err := w.sendBodyChunkChecked(w.buf.Bytes()); err != nil {
			level.Warn(w.logger).Log("msg", "failed to send streaming search response body chunk", "err", err)
			w.failed = true
		}
		w.buf.Reset()
	}
}

// openStreamAndSendMetadata opens a QueryResultStream to the query-frontend and sends
// the HTTP metadata (status code + headers + stats) as the first gRPC message.
// Retries the initial setup with backoff, matching the pattern used by grpcStreamWriter.
func (w *grpcStreamingResponseWriter) openStreamAndSendMetadata() error {
	bof := createFrontendBackoff(w.frontendCtx)
	var lastErr error
	for bof.Ongoing() {
		c, err := w.clientPool.GetClientFor(w.frontendAddress)
		if err != nil {
			lastErr = err
			bof.Wait()
			continue
		}

		stream, err := c.(frontendv2pb.FrontendForQuerierClient).QueryResultStream(w.frontendCtx)
		if err != nil {
			lastErr = err
			bof.Wait()
			continue
		}

		headers := httpgrpc.FromHeader(w.header)
		headers, _ = removeStreamingHeader(headers)

		var statsCopy *querier_stats.SafeStats
		if w.stats != nil {
			statsCopy = w.stats.Copy()
		}

		err = stream.Send(&frontendv2pb.QueryResultStreamRequest{
			QueryID: w.queryID,
			Data: &frontendv2pb.QueryResultStreamRequest_Metadata{
				Metadata: &frontendv2pb.QueryResultMetadata{
					Code:    int32(w.statusCode),
					Headers: headers,
					Stats:   statsCopy,
				},
			},
		})
		if err != nil {
			lastErr = err
			bof.Wait()
			continue
		}

		w.stream = stream
		return nil
	}
	return fmt.Errorf("failed to open gRPC stream for streaming search response: %w", lastErr)
}

// sendBodyChunk sends chunk as a QueryResultStreamRequest_Body message. Returns an
// error if the request context has been cancelled or the gRPC send fails.
func (w *grpcStreamingResponseWriter) sendBodyChunk(chunk []byte) error {
	select {
	case <-w.reqCtx.Done():
		return fmt.Errorf("request cancelled: %w", context.Cause(w.reqCtx))
	default:
	}
	return w.stream.Send(&frontendv2pb.QueryResultStreamRequest{
		QueryID: w.queryID,
		Data: &frontendv2pb.QueryResultStreamRequest_Body{
			Body: &frontendv2pb.QueryResultBody{
				Chunk: chunk,
			},
		},
	})
}

// sendBodyChunkChecked sends data as a body chunk, returning an error if len(data)
// exceeds maxMessageSize. This mirrors the behaviour of runHttpRequest, which rejects
// oversized messages rather than silently restructuring them.
func (w *grpcStreamingResponseWriter) sendBodyChunkChecked(data []byte) error {
	if w.maxMessageSize > 0 && len(data) >= w.maxMessageSize {
		return fmt.Errorf("response chunk larger than the max message size (%d vs %d)", len(data), w.maxMessageSize)
	}
	return w.sendBodyChunk(data)
}

// CloseStream flushes any remaining buffered data and closes the gRPC stream.
// Must be called after the HTTP handler returns when StreamOpened() is true.
func (w *grpcStreamingResponseWriter) CloseStream() {
	if w.stream == nil {
		return
	}
	if w.buf.Len() > 0 && !w.failed {
		if err := w.sendBodyChunkChecked(w.buf.Bytes()); err != nil {
			level.Warn(w.logger).Log("msg", "failed to send final streaming search response chunk", "err", err)
		}
		w.buf.Reset()
	}
	if _, err := w.stream.CloseAndRecv(); err != nil {
		if grpcutil.IsCanceled(err) {
			level.Debug(w.logger).Log("msg", "streaming search response stream closed because request was cancelled", "err", err)
		} else {
			level.Warn(w.logger).Log("msg", "error closing streaming search response stream", "err", err)
		}
	}
}

// StreamOpened reports whether the gRPC stream was ever opened, i.e. whether
// Flush() was called at least once and the stream was established successfully.
func (w *grpcStreamingResponseWriter) StreamOpened() bool {
	return w.streamOpened
}

// AsHTTPResponse returns the accumulated response as an httpgrpc.HTTPResponse.
// Should only be called when StreamOpened() returns false (the handler returned
// without flushing, e.g. due to an early error response).
func (w *grpcStreamingResponseWriter) AsHTTPResponse() *httpgrpc.HTTPResponse {
	code := w.statusCode
	if code == 0 {
		code = http.StatusOK
	}
	headers := httpgrpc.FromHeader(w.header)
	headers, _ = removeStreamingHeader(headers)
	return &httpgrpc.HTTPResponse{
		Code:    int32(code),
		Headers: headers,
		Body:    w.buf.Bytes(),
	}
}
