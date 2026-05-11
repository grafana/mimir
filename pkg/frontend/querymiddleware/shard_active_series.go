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
	"golang.org/x/sync/errgroup"

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

func newShardActiveSeriesMiddleware(upstream http.RoundTripper, limits Limits, logger log.Logger) http.RoundTripper {
	return &shardActiveSeriesMiddleware{shardBySeriesBase{
		upstream: upstream,
		limits:   limits,
		logger:   logger,
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

func (s *shardActiveSeriesMiddleware) mergeResponses(ctx context.Context, responses []*http.Response, encoding string) *http.Response {
	reader, writer := io.Pipe()

	streamCh := make(chan *bytes.Buffer)

	g, gCtx := errgroup.WithContext(ctx)
	for _, res := range responses {
		if res == nil {
			continue
		}
		r := res
		g.Go(func() error {
			dec := borrowShardActiveSeriesResponseDecoder(gCtx, r.Body, streamCh)
			defer func() {
				dec.close()
				reuseShardActiveSeriesResponseDecoder(dec)
			}()

			if err := dec.decode(); err != nil {
				return err
			}
			return dec.streamData()
		})
	}

	go func() {
		// We ignore the error from the errgroup because it will be checked again later.
		_ = g.Wait()
		close(streamCh)
	}()

	resp := &http.Response{Body: reader, StatusCode: http.StatusOK, Header: http.Header{}}
	resp.Header.Set("Content-Type", "application/json")
	if encoding == encodingTypeSnappyFramed {
		resp.Header.Set("Content-Encoding", encodingTypeSnappyFramed)
	}

	go s.writeMergedResponse(gCtx, g.Wait, writer, streamCh, encoding)

	return resp
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
