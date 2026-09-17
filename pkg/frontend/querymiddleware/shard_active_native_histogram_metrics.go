// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"

	"github.com/go-kit/log"
	"github.com/golang/snappy"
	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"

	"github.com/grafana/mimir/pkg/cardinality"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

type shardActiveNativeHistogramMetricsMiddleware struct {
	shardBySeriesBase
}

func newShardActiveNativeHistogramMetricsMiddleware(upstream http.RoundTripper, maxConcurrency int, limits Limits, logger log.Logger) http.RoundTripper {
	return &shardActiveNativeHistogramMetricsMiddleware{shardBySeriesBase{
		upstream:       upstream,
		limits:         limits,
		logger:         logger,
		maxConcurrency: maxConcurrency,
	}}
}

func (s *shardActiveNativeHistogramMetricsMiddleware) RoundTrip(r *http.Request) (*http.Response, error) {
	spanLog, ctx := spanlogger.New(r.Context(), s.logger, tracer, "shardActiveNativeHistogramMetrics.RoundTrip")
	defer spanLog.Finish()

	resp, err := s.shardBySeriesSelector(ctx, spanLog, r, s.mergeResponses)

	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (s *shardActiveNativeHistogramMetricsMiddleware) mergeResponses(ctx context.Context, reqs []*http.Request, encoding string) (*http.Response, error) {
	mtx := sync.Mutex{}
	metricIdx := make(map[string]int, 0)
	metricBucketCount := make([]*cardinality.ActiveMetricWithBucketCount, 0)

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
			metricBucketCount = append(metricBucketCount, item)
		}
	}

	// Dispatch and decode are bounded together, so a large shard count is fanned
	// out to the queriers only as decode slots free up. All shards must be merged
	// before the result can be calculated, so this blocks until they complete.
	err := s.processShardedRequests(ctx, reqs, func(reqCtx context.Context, resp *http.Response) error {
		defer func(body io.ReadCloser) {
			// drain body reader
			_, _ = io.Copy(io.Discard, body)
			_ = body.Close()
		}(resp.Body)

		bufPtr := jsoniterBufferPool.Get().(*[]byte)
		defer jsoniterBufferPool.Put(bufPtr)

		it := jsoniter.ConfigFastest.BorrowIterator(*bufPtr)
		it.Reset(resp.Body)
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
			if err := reqCtx.Err(); err != nil {
				if cause := context.Cause(reqCtx); cause != nil {
					return fmt.Errorf("aborted streaming because context was cancelled: %w", cause)
				}
				return reqCtx.Err()
			}

			item := cardinality.ActiveMetricWithBucketCount{}
			it.ReadVal(&item)
			updateMetric(&item)
		}

		return it.Error
	})

	// Dispatch and non-OK status errors fail the whole request so
	// shardBySeriesSelector can map them to an HTTP status. Errors from decoding a
	// successful response are embedded in the body below.
	var processingErr mergeShardResponseError
	if err != nil && !errors.As(err, &processingErr) {
		return nil, err
	}

	merged := cardinality.ActiveNativeHistogramMetricsResponse{}
	resp := &http.Response{StatusCode: http.StatusInternalServerError, Header: http.Header{}}
	resp.Header.Set("Content-Type", "application/json")

	if err != nil {
		merged.Status = "error"
		merged.Error = fmt.Sprintf("error merging partial responses: %s", err.Error())
	} else {
		resp.StatusCode = http.StatusOK
		slices.SortFunc(metricBucketCount, func(a, b *cardinality.ActiveMetricWithBucketCount) int {
			return strings.Compare(a.Metric, b.Metric)
		})

		for _, item := range metricBucketCount {
			item.UpdateAverage()
			merged.Data = append(merged.Data, *item)
		}
	}

	body, err := jsoniter.Marshal(merged)
	if err != nil {
		resp.StatusCode = http.StatusInternalServerError
		body = []byte(fmt.Sprintf(`{"status":"error","error":"%s"}`, err.Error()))
	}

	if encoding == "snappy" {
		resp.Header.Set("Content-Encoding", encoding)
		body = snappy.Encode(nil, body)
	}

	resp.Body = io.NopCloser(bytes.NewReader(body))
	return resp, nil
}
