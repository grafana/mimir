// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	jsoniter "github.com/json-iterator/go"
	"github.com/opentracing/opentracing-go"
	otlog "github.com/opentracing/opentracing-go/log"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"github.com/grafana/mimir/pkg/cardinality"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

type shardActiveNativeHistogramMetricsMiddleware struct {
	shardBySeriesBase
}

func newShardActiveNativeHistogramMetricsMiddleware(upstream http.RoundTripper, limits Limits, logger log.Logger) http.RoundTripper {
	return &shardActiveNativeHistogramMetricsMiddleware{shardBySeriesBase{
		upstream: upstream,
		limits:   limits,
		logger:   logger,
	}}
}

func (s *shardActiveNativeHistogramMetricsMiddleware) RoundTrip(r *http.Request) (*http.Response, error) {
	spanLog, ctx := spanlogger.NewWithLogger(r.Context(), s.logger, "shardActiveNativeHistogramMetrics.RoundTrip")
	defer spanLog.Finish()

	resp, err := s.shardBySeriesSelector(ctx, spanLog, r, s.mergeResponses)

	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *shardActiveNativeHistogramMetricsMiddleware) mergeResponses(ctx context.Context, responses []*http.Response, encoding string) *http.Response {
	reader, writer := io.Pipe()

	mtx := sync.Mutex{}
	metricIdx := make(map[string]int, 0)
	metricBucketCount := make([]cardinality.ActiveMetricWithBucketCount, 0)

	updateMetric := func(item *cardinality.ActiveMetricWithBucketCount) {
		if item == nil || len(item.Metric) == 0 {
			// Skip empty/unknown metrics.
			return
		}
		mtx.Lock()
		defer mtx.Unlock()
		if idx, ok := metricIdx[item.Metric]; ok {
			metricBucketCount[idx].SeriesCount += item.SeriesCount
			metricBucketCount[idx].BucketCount += item.BucketCount
			if item.MinBucketCount < metricBucketCount[idx].MinBucketCount {
				metricBucketCount[idx].MinBucketCount = item.MinBucketCount
			}
			if item.MaxBucketCount > metricBucketCount[idx].MaxBucketCount {
				metricBucketCount[idx].MaxBucketCount = item.MaxBucketCount
			}
		} else {
			metricIdx[item.Metric] = len(metricBucketCount)
			metricBucketCount = append(metricBucketCount, *item)
		}
	}

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

				item := cardinality.ActiveMetricWithBucketCount{}
				it.ReadVal(&item)
				updateMetric(&item)
			}

			return it.Error
		})
	}

	// Cannot start streaming until we merged all results.
	err := g.Wait()

	// Sort the results by metric name, unless there was an error.
	if err == nil {
		sort.Slice(metricBucketCount, func(i, j int) bool {
			return metricBucketCount[i].Metric < metricBucketCount[j].Metric
		})
	} else {
		metricBucketCount = nil
	}

	resp := &http.Response{Body: reader, StatusCode: http.StatusOK, Header: http.Header{}}
	resp.Header.Set("Content-Type", "application/json")
	if encoding == encodingTypeSnappyFramed {
		resp.Header.Set("Content-Encoding", encodingTypeSnappyFramed)
	}

	go s.writeMergedResponse(ctx, err, writer, metricBucketCount, encoding)

	return resp
}

func (s *shardActiveNativeHistogramMetricsMiddleware) writeMergedResponse(ctx context.Context, mergeError error, w io.WriteCloser, metricBucketCount []cardinality.ActiveMetricWithBucketCount, encoding string) {
	defer w.Close()

	span, _ := opentracing.StartSpanFromContext(ctx, "shardActiveNativeHistogramMetrics.writeMergedResponse")
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
	defer stream.WriteObjectEnd()

	if mergeError != nil {
		level.Error(s.logger).Log("msg", "error merging partial responses", "err", mergeError.Error())
		span.LogFields(otlog.Error(mergeError))
		stream.WriteMore()
		stream.WriteObjectField("status")
		stream.WriteString("error")
		stream.WriteMore()
		stream.WriteObjectField("error")
		stream.WriteString(fmt.Sprintf("error merging partial responses: %s", mergeError.Error()))
		return
	}

	stream.WriteObjectField("data")
	stream.WriteArrayStart()
	firstItem := true
	for idx := range metricBucketCount {
		if firstItem {
			firstItem = false
		} else {
			stream.WriteMore()
		}

		stream.WriteVal(&metricBucketCount[idx])

		// Flush the stream buffer if it's getting too large.
		if stream.Buffered() > jsoniterMaxBufferSize {
			_ = stream.Flush()
		}
	}
	stream.WriteArrayEnd()
}
