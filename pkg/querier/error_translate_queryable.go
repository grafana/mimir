// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/error_translate_queryable.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querier

import (
	"context"
	"net/http"

	"github.com/grafana/dskit/grpcutil"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/validation"
)

// TranslateToPromqlAPIError converts error to one of promql.Errors for consumption in PromQL API.
// PromQL API only recognizes few errors, and converts everything else to HTTP status code 422.
//
// Specifically, it supports:
//
//	promql.ErrQueryCanceled, mapped to 499
//	promql.ErrQueryTimeout, mapped to 503
//	promql.ErrStorage mapped to 500
//	anything else is mapped to 422
//
// Querier code produces different kinds of errors, and we want to map them to above-mentioned HTTP status codes correctly.
//
// Details:
// - vendor/github.com/prometheus/prometheus/web/api/v1/api.go, respondError function only accepts *apiError types.
// - translation of error to *apiError happens in vendor/github.com/prometheus/prometheus/web/api/v1/api.go, returnAPIError method.
func TranslateToPromqlAPIError(err error) error {
	if err == nil {
		return err
	}

	var (
		errStorage        promql.ErrStorage
		errTooManySamples promql.ErrTooManySamples
		errQueryCanceled  promql.ErrQueryCanceled
		errQueryTimeout   promql.ErrQueryTimeout
	)

	switch {
	case errors.As(err, &errStorage) || errors.As(err, &errTooManySamples) || errors.As(err, &errQueryCanceled) || errors.As(err, &errQueryTimeout):
		// Don't translate those, just in case we use them internally.
		return err
	case validation.IsLimitError(err):
		// This will be returned with status code 422 by Prometheus API.
		return err
	default:
		if errors.Is(err, context.Canceled) {
			return err // 499
		}

		if s, ok := grpcutil.ErrorToStatus(err); ok {
			code := s.Code()

			if util.IsHTTPStatusCode(code) {
				// Treat these as HTTP status codes, even though they are supposed to be grpc codes.
				if code >= 400 && code < 500 {
					// Return directly, will be mapped to 422
					return err
				} else if code == http.StatusServiceUnavailable {
					return promql.ErrQueryTimeout(s.Message())
				}
			}

			details := s.Details()
			if len(details) == 1 {
				if errorDetails, ok := details[0].(*mimirpb.ErrorDetails); ok {
					switch errorDetails.GetCause() {
					case mimirpb.TOO_BUSY:
						return promql.ErrQueryTimeout(s.Message())
					}
				}
			}
		}

		// All other errors will be returned as 500.
		return promql.ErrStorage{Err: err}
	}
}

// ErrTranslateFn is used to translate or wrap error before returning it by functions in
// storage.SampleAndChunkQueryable interface.
// Input error may be nil.
type ErrTranslateFn func(err error) error

func NewErrorTranslateQueryableWithFn(q storage.Queryable, fn ErrTranslateFn) storage.Queryable {
	return errorTranslateQueryable{q: q, fn: fn}
}

func NewErrorTranslateSampleAndChunkQueryable(q storage.SampleAndChunkQueryable) storage.SampleAndChunkQueryable {
	return NewErrorTranslateSampleAndChunkQueryableWithFn(q, TranslateToPromqlAPIError)
}

func NewErrorTranslateSampleAndChunkQueryableWithFn(q storage.SampleAndChunkQueryable, fn ErrTranslateFn) storage.SampleAndChunkQueryable {
	return errorTranslateSampleAndChunkQueryable{q: q, fn: fn}
}

type errorTranslateQueryable struct {
	q  storage.Queryable
	fn ErrTranslateFn
}

func (e errorTranslateQueryable) Querier(mint, maxt int64) (storage.Querier, error) {
	q, err := e.q.Querier(mint, maxt)
	return errorTranslateQuerier{q: q, fn: e.fn}, e.fn(err)
}

type errorTranslateSampleAndChunkQueryable struct {
	q  storage.SampleAndChunkQueryable
	fn ErrTranslateFn
}

func (e errorTranslateSampleAndChunkQueryable) Querier(mint, maxt int64) (storage.Querier, error) {
	q, err := e.q.Querier(mint, maxt)
	return errorTranslateQuerier{q: q, fn: e.fn}, e.fn(err)
}

func (e errorTranslateSampleAndChunkQueryable) ChunkQuerier(mint, maxt int64) (storage.ChunkQuerier, error) {
	q, err := e.q.ChunkQuerier(mint, maxt)
	return errorTranslateChunkQuerier{q: q, fn: e.fn}, e.fn(err)
}

type errorTranslateQuerier struct {
	q  storage.Querier
	fn ErrTranslateFn
}

func (e errorTranslateQuerier) LabelValues(ctx context.Context, name string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	values, warnings, err := e.q.LabelValues(ctx, name, hints, matchers...)
	return values, warnings, e.fn(err)
}

func (e errorTranslateQuerier) LabelNames(ctx context.Context, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	values, warnings, err := e.q.LabelNames(ctx, hints, matchers...)
	return values, warnings, e.fn(err)
}

func (e errorTranslateQuerier) Close() error {
	return e.fn(e.q.Close())
}

func (e errorTranslateQuerier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	s := e.q.Select(ctx, sortSeries, hints, matchers...)
	return errorTranslateSeriesSet{s: s, fn: e.fn}
}

type errorTranslateChunkQuerier struct {
	q  storage.ChunkQuerier
	fn ErrTranslateFn
}

func (e errorTranslateChunkQuerier) LabelValues(ctx context.Context, name string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	values, warnings, err := e.q.LabelValues(ctx, name, hints, matchers...)
	return values, warnings, e.fn(err)
}

func (e errorTranslateChunkQuerier) LabelNames(ctx context.Context, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	values, warnings, err := e.q.LabelNames(ctx, hints, matchers...)
	return values, warnings, e.fn(err)
}

func (e errorTranslateChunkQuerier) Close() error {
	return e.fn(e.q.Close())
}

func (e errorTranslateChunkQuerier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.ChunkSeriesSet {
	s := e.q.Select(ctx, sortSeries, hints, matchers...)
	return errorTranslateChunkSeriesSet{s: s, fn: e.fn}
}

type errorTranslateSeriesSet struct {
	s  storage.SeriesSet
	fn ErrTranslateFn
}

func (e errorTranslateSeriesSet) Next() bool {
	return e.s.Next()
}

func (e errorTranslateSeriesSet) At() storage.Series {
	s := e.s.At()
	return errorTranslateSeries{s: s, fn: e.fn}
}

func (e errorTranslateSeriesSet) Err() error {
	return e.fn(e.s.Err())
}

func (e errorTranslateSeriesSet) Warnings() annotations.Annotations {
	return e.s.Warnings()
}

type errorTranslateSeries struct {
	s  storage.Series
	fn ErrTranslateFn
}

func (e errorTranslateSeries) Labels() labels.Labels {
	return e.s.Labels()
}

func (e errorTranslateSeries) Iterator(iterator chunkenc.Iterator) chunkenc.Iterator {
	i := e.s.Iterator(iterator)
	return errorTranslateSampleIterator{i: i, fn: e.fn}
}

type errorTranslateSampleIterator struct {
	i  chunkenc.Iterator
	fn ErrTranslateFn
}

func (e errorTranslateSampleIterator) Next() chunkenc.ValueType {
	return e.i.Next()
}

func (e errorTranslateSampleIterator) Seek(t int64) chunkenc.ValueType {
	return e.i.Seek(t)
}

func (e errorTranslateSampleIterator) At() (int64, float64) {
	return e.i.At()
}

func (e errorTranslateSampleIterator) AtHistogram(histogram *histogram.Histogram) (int64, *histogram.Histogram) {
	return e.i.AtHistogram(histogram)
}

func (e errorTranslateSampleIterator) AtFloatHistogram(histogram *histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	return e.i.AtFloatHistogram(histogram)
}

func (e errorTranslateSampleIterator) AtT() int64 {
	return e.i.AtT()
}

func (e errorTranslateSampleIterator) Err() error {
	return e.fn(e.i.Err())
}

type errorTranslateChunkSeriesSet struct {
	s  storage.ChunkSeriesSet
	fn ErrTranslateFn
}

func (e errorTranslateChunkSeriesSet) Next() bool {
	return e.s.Next()
}

func (e errorTranslateChunkSeriesSet) At() storage.ChunkSeries {
	s := e.s.At()
	return errorTranslateChunkSeries{s: s, fn: e.fn}
}

func (e errorTranslateChunkSeriesSet) Err() error {
	return e.fn(e.s.Err())
}

func (e errorTranslateChunkSeriesSet) Warnings() annotations.Annotations {
	return e.s.Warnings()
}

type errorTranslateChunkSeries struct {
	s  storage.ChunkSeries
	fn ErrTranslateFn
}

func (e errorTranslateChunkSeries) Labels() labels.Labels {
	return e.s.Labels()
}

func (e errorTranslateChunkSeries) Iterator(iterator chunks.Iterator) chunks.Iterator {
	i := e.s.Iterator(iterator)
	return errorTranslateChunksIterator{i: i, fn: e.fn}
}

func (e errorTranslateChunkSeries) ChunkCount() (int, error) {
	return e.s.ChunkCount()
}

type errorTranslateChunksIterator struct {
	i  chunks.Iterator
	fn ErrTranslateFn
}

func (e errorTranslateChunksIterator) At() chunks.Meta {
	return e.i.At()
}

func (e errorTranslateChunksIterator) Next() bool {
	return e.i.Next()
}

func (e errorTranslateChunksIterator) Err() error {
	return e.fn(e.i.Err())
}
