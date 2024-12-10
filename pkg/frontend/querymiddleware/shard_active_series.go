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
	"github.com/opentracing/opentracing-go"
	otlog "github.com/opentracing/opentracing-go/log"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
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

	labelBuilderPool = sync.Pool{
		New: func() any {
			return labels.NewBuilder(labels.EmptyLabels())
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
	useZeroAllocationDecoder bool
}

func newShardActiveSeriesMiddleware(upstream http.RoundTripper, useZeroAllocationDecoder bool, limits Limits, logger log.Logger) http.RoundTripper {
	return &shardActiveSeriesMiddleware{shardBySeriesBase{
		upstream: upstream,
		limits:   limits,
		logger:   logger,
	}, useZeroAllocationDecoder}
}

func (s *shardActiveSeriesMiddleware) RoundTrip(r *http.Request) (*http.Response, error) {
	spanLog, ctx := spanlogger.NewWithLogger(r.Context(), s.logger, "shardActiveSeries.RoundTrip")
	defer spanLog.Finish()

	var (
		resp *http.Response
		err  error
	)
	if s.useZeroAllocationDecoder {
		resp, err = s.shardBySeriesSelector(ctx, spanLog, r, s.mergeResponsesWithZeroAllocationDecoder)
	} else {
		resp, err = s.shardBySeriesSelector(ctx, spanLog, r, s.mergeResponses)
	}

	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *shardActiveSeriesMiddleware) mergeResponses(ctx context.Context, responses []*http.Response, encoding string) *http.Response {
	reader, writer := io.Pipe()

	items := make(chan *labels.Builder, len(responses))

	g := new(errgroup.Group)
	for _, res := range responses {
		if res == nil {
			continue
		}
		r := res
		g.Go(func() error {
			defer func(body io.ReadCloser) {
				// drain body reader
				_, _ = io.Copy(io.Discard, body)
				_ = body.Close()
			}(r.Body)

			bufPtr := jsoniterBufferPool.Get().(*[]byte)
			defer jsoniterBufferPool.Put(bufPtr)

			it := jsoniter.ConfigFastest.BorrowIterator(*bufPtr)
			it.Reset(r.Body)
			defer func() {
				jsoniter.ConfigFastest.ReturnIterator(it)
			}()

			// Iterate over fields until we find data or error fields
			foundDataField := false
			for it.Error == nil {
				field := it.ReadObject()
				if field == "error" {
					return fmt.Errorf("error in partial response: %s", it.ReadString())
				}
				if field == "data" {
					foundDataField = true
					break
				}
				// If the field is neither data nor error, we skip it.
				it.ReadAny()
			}
			if !foundDataField {
				return fmt.Errorf("expected data field at top level, found %s", it.CurrentBuffer())
			}

			if it.WhatIsNext() != jsoniter.ArrayValue {
				err := errors.New("expected data field to contain an array")
				return err
			}

			for it.ReadArray() {
				if err := ctx.Err(); err != nil {
					if cause := context.Cause(ctx); cause != nil {
						return fmt.Errorf("aborted streaming because context was cancelled: %w", cause)
					}
					return ctx.Err()
				}

				item := labelBuilderPool.Get().(*labels.Builder)
				it.ReadMapCB(func(iterator *jsoniter.Iterator, s string) bool {
					item.Set(s, iterator.ReadString())
					return true
				})
				items <- item
			}

			return it.Error
		})
	}

	go func() {
		// We ignore the error from the errgroup because it will be checked again later.
		_ = g.Wait()
		close(items)
	}()

	resp := &http.Response{Body: reader, StatusCode: http.StatusOK, Header: http.Header{}}
	resp.Header.Set("Content-Type", "application/json")
	if encoding == encodingTypeSnappyFramed {
		resp.Header.Set("Content-Encoding", encodingTypeSnappyFramed)
	}

	go s.writeMergedResponse(ctx, g.Wait, writer, items, encoding)

	return resp
}

func (s *shardActiveSeriesMiddleware) mergeResponsesWithZeroAllocationDecoder(ctx context.Context, responses []*http.Response, encoding string) *http.Response {
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

	go s.writeMergedResponseWithZeroAllocationDecoder(gCtx, g.Wait, writer, streamCh, encoding)

	return resp
}

func (s *shardActiveSeriesMiddleware) writeMergedResponse(ctx context.Context, check func() error, w io.WriteCloser, items <-chan *labels.Builder, encoding string) {
	defer w.Close()

	span, _ := opentracing.StartSpanFromContext(ctx, "shardActiveSeries.writeMergedResponse")
	defer span.Finish()

	var out io.Writer = w
	if encoding == encodingTypeSnappyFramed {
		span.LogFields(otlog.String("encoding", encodingTypeSnappyFramed))
		enc := getSnappyWriter(w)
		out = enc
		defer func() {
			enc.Close()
			// Reset the encoder before putting it back to pool to avoid it to hold the writer.
			enc.Reset(nil)
			snappyWriterPool.Put(enc)
		}()
	} else {
		span.LogFields(otlog.String("encoding", "none"))
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
	for item := range items {
		if firstItem {
			firstItem = false
		} else {
			stream.WriteMore()
		}
		stream.WriteObjectStart()
		firstField := true

		item.Range(func(l labels.Label) {
			if firstField {
				firstField = false
			} else {
				stream.WriteMore()
			}
			stream.WriteObjectField(l.Name)
			stream.WriteString(l.Value)
		})
		stream.WriteObjectEnd()

		item.Reset(labels.EmptyLabels())
		labelBuilderPool.Put(item)

		// Flush the stream buffer if it's getting too large.
		if stream.Buffered() > jsoniterMaxBufferSize {
			_ = stream.Flush()
		}
	}
	stream.WriteArrayEnd()

	if err := check(); err != nil {
		level.Error(s.logger).Log("msg", "error merging partial responses", "err", err.Error())
		span.LogFields(otlog.Error(err))
		stream.WriteMore()
		stream.WriteObjectField("status")
		stream.WriteString("error")
		stream.WriteMore()
		stream.WriteObjectField("error")
		stream.WriteString(fmt.Sprintf("error merging partial responses: %s", err.Error()))
	}

	stream.WriteObjectEnd()
}

func (s *shardActiveSeriesMiddleware) writeMergedResponseWithZeroAllocationDecoder(ctx context.Context, check func() error, w io.WriteCloser, streamCh chan *bytes.Buffer, encoding string) {
	defer w.Close()

	span, _ := opentracing.StartSpanFromContext(ctx, "shardActiveSeries.writeMergedResponseWithZeroAllocationDecoder")
	defer span.Finish()

	var out io.Writer = w
	if encoding == encodingTypeSnappyFramed {
		span.LogFields(otlog.String("encoding", encodingTypeSnappyFramed))
		enc := getSnappyWriter(w)
		out = enc
		defer func() {
			enc.Close()
			// Reset the encoder before putting it back to pool to avoid it to hold the writer.
			enc.Reset(nil)
			snappyWriterPool.Put(enc)
		}()
	} else {
		span.LogFields(otlog.String("encoding", "none"))
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
		span.LogFields(otlog.Error(err))
		stream.WriteMore()
		stream.WriteObjectField("status")
		stream.WriteString("error")
		stream.WriteMore()
		stream.WriteObjectField("error")
		stream.WriteString(fmt.Sprintf("error merging partial responses: %s", err.Error()))
	}

	stream.WriteObjectEnd()
}
