// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"unsafe"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	jsoniter "github.com/json-iterator/go"
	"github.com/klauspost/compress/s2"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel/attribute"

	querierapi "github.com/grafana/mimir/pkg/querier/api"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

const encodingTypeSnappyFramed = "x-snappy-framed"

var (
	errShardCountTooLow = errors.New("shard count too low")

	jsoniterMaxBufferSize = os.Getpagesize()
	jsoniterBufferPool    = sync.Pool{
		New: func() any {
			buf := make([]byte, jsoniterMaxBufferSize)
			return &buf
		},
	}
)

var snappyWriterPool sync.Pool

func getSnappyWriter(w io.Writer) *s2.Writer {
	sw := snappyWriterPool.Get()
	if sw == nil {
		return s2.NewWriter(w)
	}
	enc := sw.(*s2.Writer)
	enc.Reset(w)
	return enc
}

type shardActiveSeriesMiddleware struct {
	shardBySeriesBase
}

func newShardActiveSeriesMiddleware(upstream http.RoundTripper, maxConcurrency int, requestFramedResponses bool, limits Limits, logger log.Logger) http.RoundTripper {
	return &shardActiveSeriesMiddleware{shardBySeriesBase{
		upstream:               upstream,
		limits:                 limits,
		logger:                 logger,
		maxConcurrency:         maxConcurrency,
		requestFramedResponses: requestFramedResponses,
	}}
}

func (s *shardActiveSeriesMiddleware) RoundTrip(r *http.Request) (*http.Response, error) {
	spanLog, ctx := spanlogger.New(r.Context(), s.logger, tracer, "shardActiveSeries.RoundTrip")
	defer spanLog.Finish()

	resp, err := s.shardBySeriesSelector(ctx, spanLog, r, s.mergeResponses)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func responseIsActiveSeriesFramed(r *http.Response) bool {
	return r.Header.Get("Content-Type") == querierapi.ContentTypeActiveSeriesFramed
}

func (s *shardActiveSeriesMiddleware) mergeResponses(ctx context.Context, reqs []*http.Request, encoding string) (*http.Response, error) {
	reader, writer := io.Pipe()

	streamCh := make(chan *bytes.Buffer)
	errCh := make(chan error, 1)

	// Dispatch and decode the shards from a separate goroutine so that
	// mergeResponses can return and writeMergedResponse can start draining
	// streamCh. When maxConcurrency is set, processShardedRequests blocks once
	// the limit is reached, and the running decoders block on streamCh until the
	// consumer is running, so this must not happen on the path that starts the
	// consumer. Dispatch is bounded together with decoding, so a large shard
	// count is fanned out to the queriers only as decode slots free up.
	go func() {
		errCh <- s.processShardedRequests(ctx, reqs, func(reqCtx context.Context, resp *http.Response) error {
			dec := borrowShardActiveSeriesResponseDecoder(reqCtx, resp.Body, streamCh)
			defer func() {
				dec.close()
				reuseShardActiveSeriesResponseDecoder(dec)
			}()

			return dec.stream(responseIsActiveSeriesFramed(resp))
		})
		close(streamCh)
	}()

	resp := &http.Response{Body: reader, StatusCode: http.StatusOK, Header: http.Header{}}
	resp.Header.Set("Content-Type", "application/json")
	if encoding == encodingTypeSnappyFramed {
		resp.Header.Set("Content-Encoding", encodingTypeSnappyFramed)
	}

	// The response is already committed as 200 OK by the time any shard fails, so
	// errors (including errShardCountTooLow from a shard returning 413) surface as
	// an error object embedded in the streamed body rather than an HTTP status.
	go s.writeMergedResponse(ctx, func() error { return <-errCh }, writer, streamCh, encoding)

	return resp, nil
}

func (s *shardActiveSeriesMiddleware) writeMergedResponse(ctx context.Context, check func() error, w io.WriteCloser, streamCh chan *bytes.Buffer, encoding string) {
	defer w.Close()

	_, span := tracer.Start(ctx, "shardActiveSeries.writeMergedResponse")
	defer span.End()

	var out io.Writer = w
	if encoding == encodingTypeSnappyFramed {
		span.SetAttributes(attribute.String("encoding", encodingTypeSnappyFramed))
		enc := getSnappyWriter(w)
		out = enc
		defer func() {
			enc.Close()
			// Reset the encoder before putting it back to pool to avoid it to hold the writer.
			enc.Reset(nil)
			snappyWriterPool.Put(enc)
		}()
	} else {
		span.SetAttributes(attribute.String("encoding", "none"))
	}

	stream := jsoniter.ConfigFastest.BorrowStream(out)
	defer func(stream *jsoniter.Stream) {
		_ = stream.Flush()

		if cap(stream.Buffer()) > jsoniterMaxBufferSize {
			return
		}
		jsoniter.ConfigFastest.ReturnStream(stream)
	}(stream)

	stream.WriteObjectStart()
	stream.WriteObjectField("data")
	stream.WriteArrayStart()

	firstItem := true
	for streamBuf := range streamCh {
		if firstItem {
			firstItem = false
		} else {
			stream.WriteMore()
		}
		rawStr := unsafe.String(unsafe.SliceData(streamBuf.Bytes()), streamBuf.Len())

		// Write the value as is, since it's already a JSON array.
		stream.WriteRaw(rawStr)

		// Flush the stream buffer if it's getting too large.
		if stream.Buffered() > jsoniterMaxBufferSize {
			_ = stream.Flush()
		}

		// Reuse stream buffer.
		reuseActiveSeriesDataStreamBuffer(streamBuf)
	}
	stream.WriteArrayEnd()

	if err := check(); err != nil {
		level.Error(s.logger).Log("msg", "error merging partial responses", "err", err.Error())
		span.RecordError(err)
		stream.WriteMore()
		stream.WriteObjectField("status")
		stream.WriteString("error")
		stream.WriteMore()
		stream.WriteObjectField("error")
		stream.WriteString(fmt.Sprintf("error merging partial responses: %s", err.Error()))
	}

	stream.WriteObjectEnd()
}
