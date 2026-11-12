// SPDX-License-Identifier: AGPL-3.0-only
//go:build rustengine && cgo

package ingester

import (
	"context"
	"encoding/base64"
	"math"
	"slices"

	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/rustengine"
)

func (e *rustEngine) Querier(mint, maxt int64) (storage.Querier, error) {
	return &rustQuerier{engine: e, mint: mint, maxt: maxt}, nil
}
func (e *rustEngine) ChunkQuerier(mint, maxt int64, _ bool) (storage.ChunkQuerier, error) {
	return rustChunkQuerier{&rustQuerier{engine: e, mint: mint, maxt: maxt}}, nil
}

type rustQuerier struct {
	engine     *rustEngine
	mint, maxt int64
}

func (q *rustQuerier) Close() error { return nil }
func (q *rustQuerier) read(ctx context.Context, hints *storage.SelectHints, ms ...*labels.Matcher) ([]rustengine.Series, error) {
	q.engine.mu.Lock()
	defer q.engine.mu.Unlock()
	ix := &rustIndex{engine: q.engine, cold: true}
	p, err := ix.PostingsForMatchers(ctx, false, ms...)
	if err != nil {
		return nil, err
	}
	if hints != nil && hints.ShardCount > 0 {
		p = ix.ShardedPostings(p, hints.ShardIndex, hints.ShardCount)
	}
	refs, err := index.ExpandPostings(p)
	if err != nil {
		return nil, err
	}
	if len(refs) == 0 {
		return nil, nil
	}
	ids := make([]uint64, len(refs))
	for i, r := range refs {
		ids[i] = uint64(r)
	}
	mint, maxt := q.mint, q.maxt
	if hints != nil {
		mint = max(mint, hints.Start)
		maxt = min(maxt, hints.End)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	response, err := q.engine.client.Call(rustengine.Request{Op: "read", Mint: mint, Maxt: maxt, Refs: ids, Filter: true})
	if err != nil {
		return nil, err
	}
	series := response.Series[:0]
	for _, s := range response.Series {
		if len(s.Samples) > 0 {
			series = append(series, s)
		}
	}
	return series, ctx.Err()
}
func (q *rustQuerier) Select(ctx context.Context, sorted bool, hints *storage.SelectHints, ms ...*labels.Matcher) storage.SeriesSet {
	series, err := q.read(ctx, hints, ms...)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}
	if sorted {
		sortedRustSeries(series)
	}
	if hints != nil && hints.Limit > 0 && len(series) > hints.Limit {
		series = series[:hints.Limit]
	}
	result := &rustSeriesSet{at: -1}
	for _, s := range series {
		samples := make([]chunks.Sample, 0, len(s.Samples))
		times := make([]int64, 0, len(s.Samples))
		for t := range s.Samples {
			times = append(times, t)
		}
		slices.Sort(times)
		for _, t := range times {
			value, err := decodeRustSample(s.Samples[t])
			if err != nil {
				return storage.ErrSeriesSet(err)
			}
			samples = append(samples, value)
		}
		result.series = append(result.series, storage.NewListSeries(rustLabels(s.Labels), samples))
	}
	return result
}
func (q *rustQuerier) LabelNames(ctx context.Context, hints *storage.LabelHints, ms ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return q.labelStrings(ctx, "", hints, ms...)
}
func (q *rustQuerier) LabelValues(ctx context.Context, name string, hints *storage.LabelHints, ms ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return q.labelStrings(ctx, name, hints, ms...)
}
func (q *rustQuerier) labelStrings(ctx context.Context, name string, hints *storage.LabelHints, ms ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	q.engine.mu.Lock()
	defer q.engine.mu.Unlock()
	ix := &rustIndex{engine: q.engine, cold: true}
	p, err := ix.PostingsForMatchers(ctx, false, ms...)
	if err != nil {
		return nil, nil, err
	}
	refs, err := index.ExpandPostings(p)
	if err != nil {
		return nil, nil, err
	}
	ids := make([]uint64, len(refs))
	for i, r := range refs {
		ids[i] = uint64(r)
	}
	op := "label_values"
	if name == "" {
		op = "label_names"
	}
	limit := 0
	if hints != nil {
		limit = max(0, hints.Limit)
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	result, err := q.engine.client.Call(rustengine.Request{Op: op, Name: name, Refs: ids, Mint: q.mint, Maxt: q.maxt, Limit: limit})
	return result.Values, nil, err
}

type rustChunkQuerier struct{ *rustQuerier }

func (q rustChunkQuerier) Select(ctx context.Context, sorted bool, hints *storage.SelectHints, ms ...*labels.Matcher) storage.ChunkSeriesSet {
	return &rustChunkSet{SeriesSet: q.rustQuerier.Select(ctx, sorted, hints, ms...), encoding: chunkenc.Encoding(q.engine.floatEncoding.Load())}
}

type rustChunkSet struct {
	storage.SeriesSet
	encoding chunkenc.Encoding
}

func (s *rustChunkSet) At() storage.ChunkSeries {
	return storage.NewSeriesToChunkEncoderWithFloatEncoding(s.SeriesSet.At(), s.encoding)
}

type rustSeriesSet struct {
	series []storage.Series
	at     int
}

func (s *rustSeriesSet) Next() bool                        { s.at++; return s.at < len(s.series) }
func (s *rustSeriesSet) At() storage.Series                { return s.series[s.at] }
func (s *rustSeriesSet) Err() error                        { return nil }
func (s *rustSeriesSet) Warnings() annotations.Annotations { return nil }

type rustSample struct {
	t  int64
	v  float64
	h  *histogram.Histogram
	fh *histogram.FloatHistogram
}

func (s rustSample) T() int64                      { return s.t }
func (s rustSample) ST() int64                     { return 0 }
func (s rustSample) F() float64                    { return s.v }
func (s rustSample) H() *histogram.Histogram       { return s.h }
func (s rustSample) FH() *histogram.FloatHistogram { return s.fh }
func (s rustSample) Type() chunkenc.ValueType {
	if s.h != nil {
		return chunkenc.ValHistogram
	}
	if s.fh != nil {
		return chunkenc.ValFloatHistogram
	}
	return chunkenc.ValFloat
}
func (s rustSample) Copy() chunks.Sample {
	if s.h != nil {
		s.h = s.h.Copy()
	}
	if s.fh != nil {
		s.fh = s.fh.Copy()
	}
	return s
}
func decodeRustSample(s rustengine.Sample) (rustSample, error) {
	out := rustSample{t: s.Timestamp, v: math.Float64frombits(s.Bits)}
	if s.Kind == 2 || s.Kind == 3 {
		bytes, err := base64.StdEncoding.DecodeString(s.Data)
		if err != nil {
			return out, err
		}
		var proto mimirpb.Histogram
		if err = proto.Unmarshal(bytes); err != nil {
			return out, err
		}
		if s.Kind == 2 {
			out.h = mimirpb.FromHistogramProtoToHistogram(&proto)
		} else {
			out.fh = mimirpb.FromFloatHistogramProtoToFloatHistogram(&proto)
		}
	}
	return out, nil
}
func (e *rustEngine) ExemplarQuerier(ctx context.Context) (storage.ExemplarQuerier, error) {
	return &rustExemplarQuerier{engine: e, ctx: ctx}, ctx.Err()
}

type rustExemplarQuerier struct {
	engine *rustEngine
	ctx    context.Context
}

func (q *rustExemplarQuerier) Select(start, end int64, matchers ...[]*labels.Matcher) ([]exemplar.QueryResult, error) {
	q.engine.mu.Lock()
	defer q.engine.mu.Unlock()
	if err := q.ctx.Err(); err != nil {
		return nil, err
	}
	r, err := q.engine.client.Call(rustengine.Request{Op: "read", Mint: math.MaxInt64, Maxt: math.MinInt64})
	if err != nil {
		return nil, err
	}
	var out []exemplar.QueryResult
	for _, s := range r.Series {
		ls := rustLabels(s.Labels)
		matched := len(matchers) == 0
		for _, group := range matchers {
			ok := true
			for _, m := range group {
				ok = ok && m.Matches(ls.Get(m.Name))
			}
			matched = matched || ok
		}
		if !matched {
			continue
		}
		result := exemplar.QueryResult{SeriesLabels: ls}
		for _, ex := range s.Exemplars {
			if ex.Timestamp >= start && ex.Timestamp <= end {
				result.Exemplars = append(result.Exemplars, exemplar.Exemplar{Labels: rustLabels(ex.Labels), Ts: ex.Timestamp, Value: math.Float64frombits(ex.Bits), HasTs: true})
			}
		}
		if len(result.Exemplars) > 0 {
			out = append(out, result)
		}
	}
	slices.SortFunc(out, func(a, b exemplar.QueryResult) int { return labels.Compare(a.SeriesLabels, b.SeriesLabels) })
	return out, nil
}
