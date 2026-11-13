// SPDX-License-Identifier: AGPL-3.0-only
//go:build rustengine && cgo

package ingester

import (
	"context"
	"encoding/base64"
	"errors"
	"math"
	"slices"
	"strings"
	"sync"

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
func (e *rustEngine) ChunkQuerier(mint, maxt int64, unordered bool) (storage.ChunkQuerier, error) {
	return rustChunkQuerier{&rustQuerier{engine: e, mint: mint, maxt: maxt, unordered: unordered}}, nil
}

type rustQuerier struct {
	engine     *rustEngine
	mint, maxt int64
	mu         sync.Mutex
	closed     bool
	unordered  bool
	cursors    map[uint64]*rustQueryCursor
}

func (q *rustQuerier) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	var err error
	for id, c := range q.cursors {
		c.stop()
		_, failure := q.engine.client.Call(rustengine.Request{Op: "cursor_close", Cursor: id})
		err = errors.Join(err, failure)
		delete(q.cursors, id)
	}
	return err
}
func (q *rustQuerier) selectChunks(ctx context.Context, sorted bool, hints *storage.SelectHints, ms ...*labels.Matcher) storage.ChunkSeriesSet {
	cursor, page, limit, err := q.openCursor(ctx, sorted, hints, "cursor_open", ms...)
	if err != nil {
		return storage.ErrChunkSeriesSet(err)
	}
	return &rustStreamSet{cursor: cursor, count: page.Count, limit: limit, encoding: chunkenc.Encoding(q.engine.floatEncoding.Load()), seriesPage: page.Series, countsPage: page.Refs, firstPage: page.Chunks}
}
func (q *rustQuerier) openCursor(ctx context.Context, sorted bool, hints *storage.SelectHints, op string, ms ...*labels.Matcher) (*rustQueryCursor, rustengine.Response, int, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil, rustengine.Response{}, 0, errors.New("Rust querier is closed")
	}
	if err := ctx.Err(); err != nil {
		return nil, rustengine.Response{}, 0, err
	}
	q.engine.mu.Lock()
	defer q.engine.mu.Unlock()
	var ids []uint64
	var equal []rustengine.Label
	var filters []rustengine.LabelFilter
	unsharded := hints == nil || hints.ShardCount == 0
	nativeEqual := len(ms) > 0 && unsharded
	if nativeEqual {
		for _, matcher := range ms {
			if matcher.Type != labels.MatchEqual || matcher.Value == "" {
				nativeEqual = false
				break
			}
		}
	}
	if len(ms) == 0 && unsharded {
		op += "_all"
	} else if nativeEqual {
		op += "_equal"
		equal = make([]rustengine.Label, len(ms))
		for i, matcher := range ms {
			equal[i] = rustengine.Label{Name: matcher.Name, Value: matcher.Value}
		}
	} else if unsharded {
		var err error
		filters, err = (&rustIndex{engine: q.engine, cold: true}).nativeFilters(ctx, ms)
		if err != nil {
			return nil, rustengine.Response{}, 0, err
		}
		op += "_select"
	} else {
		ix := &rustIndex{engine: q.engine, cold: true}
		p, err := ix.PostingsForMatchers(ctx, false, ms...)
		if err != nil {
			return nil, rustengine.Response{}, 0, err
		}
		if hints != nil && hints.ShardCount > 0 {
			p = ix.shardedPostings(ctx, p, hints.ShardIndex, hints.ShardCount)
		}
		refs, err := index.ExpandPostings(p)
		if err != nil {
			return nil, rustengine.Response{}, 0, err
		}
		ids = make([]uint64, len(refs))
		for i, r := range refs {
			ids[i] = uint64(r)
		}
	}
	mint, maxt := q.mint, q.maxt
	limit := 0
	if hints != nil {
		mint = max(mint, hints.Start)
		maxt = min(maxt, hints.End)
		limit = hints.Limit
	}
	if err := ctx.Err(); err != nil {
		return nil, rustengine.Response{}, 0, err
	}
	pageLimit := 16
	if limit > 0 {
		pageLimit = min(pageLimit, limit)
	}
	response, err := q.engine.client.Call(rustengine.Request{Op: op, Mint: mint, Maxt: maxt, Refs: ids, Labels: equal, Filters: filters, Sorted: sorted, Unordered: q.unordered, Limit: pageLimit})
	if err != nil {
		return nil, rustengine.Response{}, 0, err
	}
	n := int(min(response.Count, uint64(pageLimit)))
	labelsOnly := strings.HasPrefix(op, "cursor_open_labels")
	if len(response.Series) != n || (!labelsOnly && (len(response.Refs) != n || len(response.Chunks) != n)) {
		_, _ = q.engine.client.Call(rustengine.Request{Op: "cursor_close", Cursor: response.Cursor})
		return nil, rustengine.Response{}, 0, errors.New("invalid Rust initial series page")
	}
	c := &rustQueryCursor{q: q, ctx: ctx, id: response.Cursor}
	if q.cursors == nil {
		q.cursors = make(map[uint64]*rustQueryCursor)
	}
	q.cursors[c.id] = c
	c.stop = context.AfterFunc(ctx, func() {
		q.mu.Lock()
		defer q.mu.Unlock()
		if _, ok := q.cursors[c.id]; ok {
			_, _ = q.engine.client.Call(rustengine.Request{Op: "cursor_close", Cursor: c.id})
			delete(q.cursors, c.id)
		}
	})
	return c, response, limit, nil
}
func (q *rustQuerier) Select(ctx context.Context, sorted bool, hints *storage.SelectHints, ms ...*labels.Matcher) storage.SeriesSet {
	if hints != nil && hints.Func == "series" {
		cursor, page, limit, err := q.openCursor(ctx, sorted, hints, "cursor_open_labels", ms...)
		if err != nil {
			return storage.ErrSeriesSet(err)
		}
		return &rustLabelSet{cursor: cursor, count: page.Count, limit: limit, page: page.Series}
	}
	return storage.NewSeriesSetFromChunkSeriesSet(q.selectChunks(ctx, sorted, hints, ms...))
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
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	filters, err := (&rustIndex{engine: q.engine, cold: true}).nativeFilters(ctx, ms)
	if err != nil {
		return nil, nil, err
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
	result, err := q.engine.client.Call(rustengine.Request{Op: op, Name: name, Filters: filters, Mint: q.mint, Maxt: q.maxt, Limit: limit})
	return result.Values, nil, err
}

type rustChunkQuerier struct{ *rustQuerier }

func (q rustChunkQuerier) Select(ctx context.Context, sorted bool, hints *storage.SelectHints, ms ...*labels.Matcher) storage.ChunkSeriesSet {
	return q.selectChunks(ctx, sorted, hints, ms...)
}

type rustQueryCursor struct {
	q    *rustQuerier
	ctx  context.Context
	id   uint64
	stop func() bool
}

func (c *rustQueryCursor) call(req rustengine.Request) (rustengine.Response, error) {
	c.q.mu.Lock()
	defer c.q.mu.Unlock()
	if err := c.ctx.Err(); err != nil {
		return rustengine.Response{}, err
	}
	if _, ok := c.q.cursors[c.id]; !ok {
		return rustengine.Response{}, errors.New("Rust query cursor is closed")
	}
	req.Cursor = c.id
	return c.q.engine.client.Call(req)
}

type rustStreamSet struct {
	cursor         *rustQueryCursor
	count, index   uint64
	limit, yielded int
	encoding       chunkenc.Encoding
	at             storage.ChunkSeries
	err            error
	seriesPage     []rustengine.Series
	countsPage     []uint64
	firstPage      []rustengine.QueryChunk
}

type rustSeriesFrame struct {
	index  uint64
	labels []mimirpb.LabelAdapter
	count  int
	first  rustengine.QueryChunk
}

func (s *rustStreamSet) Next() bool {
	frame, ok := s.nextRaw()
	if !ok {
		return false
	}
	s.at = &rustStreamSeries{adapters: frame.labels, ChunkSeriesEntry: &storage.ChunkSeriesEntry{ChunkCountFn: func() (int, error) { return frame.count, nil }, ChunkIteratorFn: func(chunks.Iterator) chunks.Iterator {
		return &rustStreamChunks{cursor: s.cursor, series: frame.index, encoding: s.encoding, offset: 1, page: []rustengine.QueryChunk{frame.first}, done: frame.count == 1}
	}}}
	return true
}
func (s *rustStreamSet) nextRaw() (rustSeriesFrame, bool) {
	if err := s.cursor.ctx.Err(); err != nil {
		s.err = err
		return rustSeriesFrame{}, false
	}
	if s.err != nil || (s.limit > 0 && s.yielded >= s.limit) {
		return rustSeriesFrame{}, false
	}
	for s.index < s.count {
		i := s.index
		if len(s.seriesPage) == 0 {
			limit := 16
			if s.limit > 0 {
				limit = min(limit, s.limit-s.yielded)
			}
			r, err := s.cursor.call(rustengine.Request{Op: "cursor_series_page", Ref: i, Limit: limit})
			if err != nil {
				s.err = err
				return rustSeriesFrame{}, false
			}
			if len(r.Series) == 0 || len(r.Series) != len(r.Refs) || len(r.Series) != len(r.Chunks) {
				s.err = errors.New("invalid Rust series page")
				return rustSeriesFrame{}, false
			}
			s.seriesPage, s.countsPage, s.firstPage = r.Series, r.Refs, r.Chunks
		}
		s.index++
		series, count, first := s.seriesPage[0], int(s.countsPage[0]), s.firstPage[0]
		s.seriesPage[0], s.firstPage[0] = rustengine.Series{}, rustengine.QueryChunk{}
		s.seriesPage, s.countsPage, s.firstPage = s.seriesPage[1:], s.countsPage[1:], s.firstPage[1:]
		if count == 0 {
			continue
		}
		s.yielded++
		return rustSeriesFrame{index: i, labels: series.Labels, count: count, first: first}, true
	}
	return rustSeriesFrame{}, false
}
func (s *rustStreamSet) At() storage.ChunkSeries           { return s.at }
func (s *rustStreamSet) Err() error                        { return s.err }
func (s *rustStreamSet) Warnings() annotations.Annotations { return nil }

type rustStreamSeries struct {
	*storage.ChunkSeriesEntry
	adapters []mimirpb.LabelAdapter
	once     sync.Once
	lset     labels.Labels
}

func (s *rustStreamSeries) Labels() labels.Labels {
	s.once.Do(func() { s.lset = rustLabels(s.adapters) })
	return s.lset
}

func (s *rustStreamSeries) LabelAdapters() []mimirpb.LabelAdapter { return s.adapters }

type rustStreamChunks struct {
	cursor   *rustQueryCursor
	series   uint64
	offset   int64
	page     []rustengine.QueryChunk
	done     bool
	encoding chunkenc.Encoding
	at       chunks.Meta
	err      error
}

func (it *rustStreamChunks) Next() bool {
	if it.err != nil {
		return false
	}
	if err := it.cursor.ctx.Err(); err != nil {
		it.err = err
		return false
	}
	if len(it.page) == 0 {
		if it.done {
			return false
		}
		r, err := it.cursor.call(rustengine.Request{Op: "cursor_chunks", Ref: it.series, UpTo: it.offset, Limit: 16})
		if err != nil {
			it.err = err
			return false
		}
		it.page = r.Chunks
		it.offset += int64(len(r.Chunks))
		it.done = r.Done
		if len(it.page) == 0 {
			return false
		}
	}
	c := it.page[0]
	it.page[0] = rustengine.QueryChunk{}
	it.page = it.page[1:]
	var chunk chunkenc.Chunk
	if len(c.Data) > 0 {
		chunk, it.err = chunkenc.FromData(chunkenc.EncXOR, c.Data)
		if it.err == nil && it.encoding == chunkenc.EncXOR2 {
			converted := chunkenc.NewXOR2Chunk()
			app, err := converted.Appender()
			if err != nil {
				it.err = err
				return false
			}
			values := chunk.Iterator(nil)
			for values.Next() != chunkenc.ValNone {
				t, v := values.At()
				app.Append(0, t, v)
			}
			it.err = values.Err()
			chunk = converted
		}
	} else {
		samples := make([]chunks.Sample, 0, len(c.Samples))
		for _, sample := range c.Samples {
			decoded, err := decodeRustSample(sample)
			if err != nil {
				it.err = err
				return false
			}
			samples = append(samples, decoded)
		}
		meta, err := chunks.ChunkFromSamples(samples)
		it.err = err
		chunk = meta.Chunk
	}
	if it.err != nil {
		return false
	}
	it.at = chunks.Meta{MinTime: c.Mint, MaxTime: c.Maxt, Chunk: chunk}
	return true
}
func (it *rustStreamChunks) At() chunks.Meta { return it.at }
func (it *rustStreamChunks) Err() error      { return it.err }

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
	var ids []uint64
	if len(matchers) > 0 {
		selected := map[uint64]struct{}{}
		ix := &rustIndex{engine: q.engine, cold: true}
		for _, group := range matchers {
			p, err := ix.PostingsForMatchers(q.ctx, false, group...)
			if err != nil {
				return nil, err
			}
			for p.Next() {
				selected[uint64(p.At())] = struct{}{}
			}
			if err := p.Err(); err != nil {
				return nil, err
			}
		}
		for id := range selected {
			ids = append(ids, id)
		}
	}
	r, err := q.engine.client.Call(rustengine.Request{Op: "exemplars", Mint: start, Maxt: end, Refs: ids, Filter: len(matchers) > 0})
	if err != nil {
		return nil, err
	}
	var out []exemplar.QueryResult
	for _, s := range r.Series {
		ls := rustLabels(s.Labels)
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
