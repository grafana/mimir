// SPDX-License-Identifier: AGPL-3.0-only
//go:build rustengine && cgo

package ingester

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"

	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/chunk"
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
	unordered  bool

	mu      sync.Mutex
	closed  bool
	cursors map[uint64]struct{}
}

func (q *rustQuerier) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	for id := range q.cursors {
		q.engine.native.CursorClose(id)
	}
	q.cursors = nil
	return nil
}

// rustCursor is a snapshot of what a select read, open until it is read through or the querier closes.
type rustCursor struct {
	q     *rustQuerier
	ctx   context.Context
	id    uint64
	count int
	stop  func() bool
	// The first series and chunks, which came with the cursor.
	series []rustengine.CursorSeries
	chunks [][]rustengine.Chunk
}

// Pages of a cursor's series and chunks.
const (
	rustSeriesPage = 256
	rustChunkBytes = 4 << 20
)

func (c *rustCursor) close() {
	if c.stop != nil {
		c.stop()
	}
	c.q.mu.Lock()
	defer c.q.mu.Unlock()
	if _, ok := c.q.cursors[c.id]; ok {
		delete(c.q.cursors, c.id)
		c.q.engine.native.CursorClose(c.id)
	}
}

// selection is what the matchers select, with the hints' sharding.
func (e *rustEngine) selection(ctx context.Context, hints *storage.SelectHints, headOnly bool, ms []*labels.Matcher) (rustengine.Selection, error) {
	sharded := hints != nil && hints.ShardCount > 0
	if len(ms) == 0 && !sharded {
		return rustengine.Selection{All: true}, nil
	}
	filters, err := e.nativeFilters(ctx, ms)
	if err != nil {
		return rustengine.Selection{}, err
	}
	if !sharded {
		return rustengine.Selection{Filters: filters}, nil
	}
	refs, err := e.native.Postings(headOnly, len(filters) == 0, filters)
	if err != nil {
		return rustengine.Selection{}, err
	}
	refs, err = e.shard(refs, hints.ShardIndex, hints.ShardCount)
	return rustengine.Selection{Refs: refs, ByRefs: true}, err
}

// shard keeps the refs of the series in the shard, by the hash of their labels.
func (e *rustEngine) shard(refs []uint64, shard, count uint64) ([]uint64, error) {
	out := refs[:0:0]
	err := e.native.SeriesLabels(refs, func(i int, ls []rustengine.Label) error {
		if labels.StableHash(rustLabels(ls))%count == shard {
			out = append(out, refs[i])
		}
		return nil
	})
	return out, err
}

func (q *rustQuerier) open(ctx context.Context, sorted bool, hints *storage.SelectHints, labelsOnly bool, ms []*labels.Matcher) (*rustCursor, int, error) {
	return q.openPaged(ctx, sorted, hints, labelsOnly, ms, rustSeriesPage, rustSeriesPage, rustChunkBytes)
}

func (q *rustQuerier) openPaged(ctx context.Context, sorted bool, hints *storage.SelectHints, labelsOnly bool, ms []*labels.Matcher, inlineSeries, inlineChunkSeries, inlineBytes int) (*rustCursor, int, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	sel, err := q.engine.selection(ctx, hints, false, ms)
	if err != nil {
		return nil, 0, err
	}
	mint, maxt, limit := q.mint, q.maxt, 0
	if hints != nil {
		mint, maxt, limit = max(mint, hints.Start), min(maxt, hints.End), max(0, hints.Limit)
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil, 0, errors.New("Rust querier is closed")
	}
	res, err := q.engine.native.Select(sel, mint, maxt, sorted, !q.unordered, labelsOnly, inlineSeries, inlineChunkSeries, inlineBytes)
	if err != nil {
		return nil, 0, err
	}
	c := &rustCursor{q: q, ctx: ctx, id: res.Cursor, count: res.Count, series: res.Series, chunks: res.Chunks}
	// A select read whole with its first page left no cursor to close.
	if res.Count > len(res.Series) || !labelsOnly && res.Count > len(res.Chunks) {
		if q.cursors == nil {
			q.cursors = map[uint64]struct{}{}
		}
		q.cursors[res.Cursor] = struct{}{}
		c.stop = context.AfterFunc(ctx, c.close)
	}
	return c, limit, nil
}

func (q *rustQuerier) Select(ctx context.Context, sorted bool, hints *storage.SelectHints, ms ...*labels.Matcher) storage.SeriesSet {
	if hints != nil && hints.Func == "series" {
		c, limit, err := q.open(ctx, sorted, hints, true, ms)
		if err != nil {
			return storage.ErrSeriesSet(err)
		}
		return &rustLabelSet{cursor: c, limit: limit}
	}
	mint, maxt := q.mint, q.maxt
	if hints != nil {
		mint, maxt = max(mint, hints.Start), min(maxt, hints.End)
	}
	return &rustBoundedSet{SeriesSet: storage.NewSeriesSetFromChunkSeriesSet(q.selectChunks(ctx, sorted, hints, ms...)), mint: mint, maxt: maxt}
}

// rustBoundedSet keeps the samples of its series in [mint, maxt]: chunks come whole.
type rustBoundedSet struct {
	storage.SeriesSet
	mint, maxt int64
}

func (s *rustBoundedSet) At() storage.Series {
	return &rustBoundedSeries{Series: s.SeriesSet.At(), mint: s.mint, maxt: s.maxt}
}

type rustBoundedSeries struct {
	storage.Series
	mint, maxt int64
}

func (s *rustBoundedSeries) Iterator(it chunkenc.Iterator) chunkenc.Iterator {
	if b, ok := it.(*rustBoundedIterator); ok {
		b.Iterator = s.Series.Iterator(b.Iterator)
		b.mint, b.maxt, b.started = s.mint, s.maxt, false
		return b
	}
	return &rustBoundedIterator{Iterator: s.Series.Iterator(it), mint: s.mint, maxt: s.maxt}
}

type rustBoundedIterator struct {
	chunkenc.Iterator
	mint, maxt int64
	started    bool
}

func (it *rustBoundedIterator) Next() chunkenc.ValueType {
	if !it.started {
		it.started = true
		return it.Seek(it.mint)
	}
	typ := it.Iterator.Next()
	if typ != chunkenc.ValNone && it.AtT() > it.maxt {
		return chunkenc.ValNone
	}
	return typ
}

func (it *rustBoundedIterator) Seek(t int64) chunkenc.ValueType {
	it.started = true
	typ := it.Iterator.Seek(max(t, it.mint))
	if typ != chunkenc.ValNone && it.AtT() > it.maxt {
		return chunkenc.ValNone
	}
	return typ
}

func (q *rustQuerier) selectChunks(ctx context.Context, sorted bool, hints *storage.SelectHints, ms ...*labels.Matcher) storage.ChunkSeriesSet {
	c, limit, err := q.open(ctx, sorted, hints, false, ms)
	if err != nil {
		return storage.ErrChunkSeriesSet(err)
	}
	return &rustChunkSet{cursor: c, limit: limit, encoding: chunkenc.Encoding(q.engine.floatEncoding.Load())}
}

func (q *rustQuerier) LabelNames(ctx context.Context, hints *storage.LabelHints, ms ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	filters, err := q.engine.nativeFilters(ctx, ms)
	if err != nil {
		return nil, nil, err
	}
	names, err := q.engine.native.LabelNames(q.mint, q.maxt, labelLimit(hints), filters)
	return names, nil, err
}

func (q *rustQuerier) LabelValues(ctx context.Context, name string, hints *storage.LabelHints, ms ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	filters, err := q.engine.nativeFilters(ctx, ms)
	if err != nil {
		return nil, nil, err
	}
	values, err := q.engine.native.LabelValues(name, q.mint, q.maxt, labelLimit(hints), filters)
	return values, nil, err
}

func labelLimit(hints *storage.LabelHints) int {
	if hints == nil {
		return 0
	}
	return max(0, hints.Limit)
}

type rustChunkQuerier struct{ *rustQuerier }

func (q rustChunkQuerier) Select(ctx context.Context, sorted bool, hints *storage.SelectHints, ms ...*labels.Matcher) storage.ChunkSeriesSet {
	return q.selectChunks(ctx, sorted, hints, ms...)
}

// rustChunkSet reads a cursor's series and their chunks a page at a time.
type rustChunkSet struct {
	cursor   *rustCursor
	limit    int
	encoding chunkenc.Encoding
	next     int
	page     []rustengine.CursorSeries
	chunks   [][]rustengine.Chunk
	at       storage.ChunkSeries
	err      error
}

func (s *rustChunkSet) Next() bool {
	if s.err != nil {
		return false
	}
	if err := s.cursor.ctx.Err(); err != nil {
		s.err = err
		return false
	}
	if s.limit > 0 && s.next >= s.limit || s.next >= s.cursor.count {
		s.cursor.close()
		return false
	}
	if s.next == 0 {
		s.page, s.chunks = s.cursor.series, s.cursor.chunks
	}
	native := s.cursor.q.engine.native
	if len(s.page) == 0 {
		page, err := native.CursorSeries(s.cursor.id, s.next, min(rustSeriesPage, s.cursor.count-s.next), nil)
		if err != nil {
			s.err = err
			return false
		}
		if len(page) == 0 {
			s.err = errors.New("Rust cursor returned no series")
			return false
		}
		s.page = page
	}
	if len(s.chunks) == 0 {
		chunks, err := native.CursorChunks(s.cursor.id, s.next, len(s.page), rustChunkBytes, nil)
		if err != nil {
			s.err = err
			return false
		}
		if len(chunks) == 0 {
			s.err = errors.New("Rust cursor returned no chunks")
			return false
		}
		s.chunks = chunks
	}
	series, raw := s.page[0], s.chunks[0]
	s.page, s.chunks = s.page[1:], s.chunks[1:]
	s.next++
	metas := make([]chunks.Meta, 0, len(raw))
	for _, c := range raw {
		meta, err := rustChunkMeta(c, s.encoding)
		if err != nil {
			s.err = err
			return false
		}
		metas = append(metas, meta...)
	}
	s.at = &rustStreamSeries{adapters: series.Labels, ChunkSeriesEntry: &storage.ChunkSeriesEntry{
		ChunkCountFn:    func() (int, error) { return len(metas), nil },
		ChunkIteratorFn: func(chunks.Iterator) chunks.Iterator { return storage.NewListChunkSeriesIterator(metas...) },
	}}
	return true
}

func (s *rustChunkSet) At() storage.ChunkSeries           { return s.at }
func (s *rustChunkSet) Err() error                        { return s.err }
func (s *rustChunkSet) Warnings() annotations.Annotations { return nil }

// rustChunkMeta makes the chunk Prometheus' chunk: XOR as it is, or transcoded to XOR2; histograms decoded and
// encoded.
func rustChunkMeta(c rustengine.Chunk, encoding chunkenc.Encoding) ([]chunks.Meta, error) {
	if c.Encoding == rustengine.HistogramEncoding {
		samples, err := rustHistogramSamples(c.Data)
		if err != nil {
			return nil, err
		}
		return rustHistogramChunks(samples)
	}
	xor, err := chunkenc.FromData(chunkenc.EncXOR, c.Data)
	if err != nil {
		return nil, err
	}
	if encoding == chunkenc.EncXOR2 {
		converted := chunkenc.NewXOR2Chunk()
		app, err := converted.Appender()
		if err != nil {
			return nil, err
		}
		it := xor.Iterator(nil)
		for it.Next() != chunkenc.ValNone {
			t, v := it.At()
			app.Append(0, t, v)
		}
		if err := it.Err(); err != nil {
			return nil, err
		}
		xor = converted
	}
	return []chunks.Meta{{MinTime: c.Mint, MaxTime: c.Maxt, Chunk: xor}}, nil
}

// rustHistogramChunks encodes the samples in as many chunks as their kinds, counter resets and layouts need.
func rustHistogramChunks(samples []chunks.Sample) ([]chunks.Meta, error) {
	var (
		out        []chunks.Meta
		chunk      chunkenc.Chunk
		app        chunkenc.Appender
		mint, maxt int64
		typ        chunkenc.ValueType
	)
	flush := func() {
		if chunk != nil {
			out = append(out, chunks.Meta{MinTime: mint, MaxTime: maxt, Chunk: chunk})
		}
	}
	for _, s := range samples {
		if chunk == nil || s.Type() != typ {
			flush()
			typ = s.Type()
			switch typ {
			case chunkenc.ValHistogram:
				chunk = chunkenc.NewHistogramChunk()
			default:
				chunk = chunkenc.NewFloatHistogramChunk()
			}
			var err error
			if app, err = chunk.Appender(); err != nil {
				return nil, err
			}
			mint = s.T()
		}
		var (
			next    chunkenc.Chunk
			recoded bool
			err     error
		)
		if typ == chunkenc.ValHistogram {
			next, recoded, app, err = app.AppendHistogram(nil, 0, s.T(), s.H(), false)
		} else {
			next, recoded, app, err = app.AppendFloatHistogram(nil, 0, s.T(), s.FH(), false)
		}
		if err != nil {
			return nil, err
		}
		if next != nil {
			// A recoded chunk has the samples so far too; another one starts after them.
			if !recoded {
				flush()
				mint = s.T()
			}
			chunk = next
		}
		maxt = s.T()
	}
	flush()
	return out, nil
}

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

// rustHistogramSamples decodes a chunk of histogram samples: a count, then the time, kind and protobuf of each.
func rustHistogramSamples(data []byte) ([]chunks.Sample, error) {
	if len(data) < 4 {
		return nil, errors.New("Rust histogram chunk too short")
	}
	n := int(binary.LittleEndian.Uint32(data))
	data = data[4:]
	out := make([]chunks.Sample, 0, n)
	for range n {
		if len(data) < 13 {
			return nil, errors.New("Rust histogram chunk truncated")
		}
		t := int64(binary.LittleEndian.Uint64(data))
		kind := data[8]
		size := int(binary.LittleEndian.Uint32(data[9:]))
		data = data[13:]
		if size > len(data) {
			return nil, errors.New("Rust histogram chunk truncated")
		}
		var proto mimirpb.Histogram
		if err := proto.Unmarshal(data[:size]); err != nil {
			return nil, err
		}
		data = data[size:]
		s := rustSample{t: t}
		if kind == rustengine.KindHistogram {
			s.h = mimirpb.FromHistogramProtoToHistogram(&proto)
		} else {
			s.fh = mimirpb.FromFloatHistogramProtoToFloatHistogram(&proto)
		}
		out = append(out, s)
	}
	return out, nil
}

// rustStreamSeries is a series that gives its labels as they are sent, building Prometheus labels only if asked.
type rustStreamSeries struct {
	*storage.ChunkSeriesEntry
	adapters []mimirpb.LabelAdapter
	once     sync.Once
	lset     labels.Labels
}

func (s *rustStreamSeries) Labels() labels.Labels {
	s.once.Do(func() { s.lset = mimirpb.FromLabelAdaptersToLabels(s.adapters) })
	return s.lset
}

func (s *rustStreamSeries) LabelAdapters() []mimirpb.LabelAdapter { return s.adapters }

// rustLabelSet is the series of a cursor without their samples, for a "series" select.
type rustLabelSet struct {
	cursor *rustCursor
	limit  int
	next   int
	page   []rustengine.CursorSeries
	at     storage.Series
	err    error
}

func (s *rustLabelSet) Next() bool {
	if s.err != nil {
		return false
	}
	if err := s.cursor.ctx.Err(); err != nil {
		s.err = err
		return false
	}
	if s.next >= s.cursor.count || s.limit > 0 && s.next >= s.limit {
		s.cursor.close()
		return false
	}
	if s.next == 0 {
		s.page = s.cursor.series
	}
	if len(s.page) == 0 {
		page, err := s.cursor.q.engine.native.CursorSeries(s.cursor.id, s.next, min(rustSeriesPage, s.cursor.count-s.next), nil)
		if err != nil {
			s.err = err
			return false
		}
		if len(page) == 0 {
			s.err = errors.New("Rust cursor returned no series")
			return false
		}
		s.page = page
	}
	s.at = &rustLabelSeries{adapters: s.page[0].Labels}
	s.page = s.page[1:]
	s.next++
	return true
}

func (s *rustLabelSet) At() storage.Series                { return s.at }
func (s *rustLabelSet) Err() error                        { return s.err }
func (s *rustLabelSet) Warnings() annotations.Annotations { return nil }

type rustLabelSeries struct {
	adapters []mimirpb.LabelAdapter
	once     sync.Once
	labels   labels.Labels
}

func (s *rustLabelSeries) Labels() labels.Labels {
	s.once.Do(func() { s.labels = mimirpb.FromLabelAdaptersToLabels(s.adapters) })
	return s.labels
}
func (s *rustLabelSeries) LabelAdapters() []mimirpb.LabelAdapter { return s.adapters }
func (*rustLabelSeries) Iterator(chunkenc.Iterator) chunkenc.Iterator {
	return chunkenc.NewNopIterator()
}

var _ streamingQuerier = rustChunkQuerier{}

// QueryStream sends the series' labels, then their chunks, straight from the cursor's pages.
func (q rustChunkQuerier) QueryStream(ctx context.Context, hints *storage.SelectHints, matchers []*labels.Matcher, batching streamBatching, send func(*client.QueryStreamResponse) error) (streamStats, error) {
	var stats streamStats
	if batching.SeriesBatchSize <= 0 || batching.ChunksBatchSize == 0 || batching.ChunksBatchMessageBytes <= 0 {
		return stats, errors.New("invalid Rust stream batching")
	}
	chunkSeries := int(min(batching.ChunksBatchSize, math.MaxInt32))
	c, limit, err := q.openPaged(ctx, true, hints, false, matchers, batching.SeriesBatchSize, chunkSeries, batching.ChunksBatchMessageBytes)
	if err != nil {
		return stats, err
	}
	defer c.close()
	total := c.count
	if limit > 0 {
		total = min(total, limit)
	}
	native := q.engine.native
	encoding := chunkenc.Encoding(q.engine.floatEncoding.Load())
	page := c.series
	var series []client.QueryStreamSeries
	// The chunks of the series with histograms, encoded while sending the series.
	var histograms map[int][]client.Chunk
	for from := 0; ; {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		// Pages after the first are read in Rust's memory, given back once sent: sending encodes them.
		free := func() {}
		if from > 0 {
			if page, free, err = native.CursorSeriesBorrowed(c.id, from, min(total-from, batching.SeriesBatchSize), page[:0]); err != nil {
				return stats, err
			}
			if len(page) == 0 {
				free()
				return stats, errors.New("Rust cursor returned no series")
			}
		}
		page = page[:min(len(page), total-from)]
		if cap(series) < len(page) {
			series = make([]client.QueryStreamSeries, len(page))
		}
		series = series[:len(page)]
		for i, s := range page {
			series[i] = client.QueryStreamSeries{Labels: s.Labels, ChunkCount: int64(s.Chunks)}
			if s.Histograms {
				// Its chunks are encoded now, so the count sent is theirs.
				var raw []rustengine.Chunk
				if from+i < len(c.chunks) {
					raw = c.chunks[from+i]
				} else {
					chunks, err := native.CursorChunks(c.id, from+i, 1, math.MaxInt32, nil)
					if err != nil {
						free()
						return stats, err
					}
					raw = chunks[0]
				}
				var encoded []client.Chunk
				for _, raw := range raw {
					var samples int
					if encoded, samples, err = rustStreamChunks(raw, encoding, encoded); err != nil {
						free()
						return stats, err
					}
					stats.Samples += samples
				}
				if histograms == nil {
					histograms = map[int][]client.Chunk{}
				}
				histograms[from+i] = encoded
				series[i].ChunkCount = int64(len(encoded))
			}
		}
		from += len(page)
		last := from >= total
		err := send(&client.QueryStreamResponse{StreamingSeries: series, IsEndOfSeriesStream: last})
		clear(series)
		free()
		if err != nil {
			return stats, err
		}
		if last {
			break
		}
	}
	stats.Series = total
	pending := c.chunks
	var (
		arena []client.Chunk
		batch []client.QueryStreamSeriesChunks
	)
	for from := 0; from < total; {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		free := func() {}
		if len(pending) == 0 {
			if pending, free, err = native.CursorChunksBorrowed(c.id, from, min(total-from, chunkSeries), batching.ChunksBatchMessageBytes, pending[:0]); err != nil {
				return stats, err
			}
			if len(pending) == 0 {
				free()
				return stats, errors.New("Rust cursor returned no chunks")
			}
		}
		pending = pending[:min(len(pending), total-from)]
		count := 0
		for _, raw := range pending {
			count += len(raw)
		}
		if cap(arena) < count {
			arena = make([]client.Chunk, 0, count)
		}
		arena = arena[:0]
		batch = batch[:0]
		for i, raw := range pending {
			start := len(arena)
			if encoded, ok := histograms[from+i]; ok {
				arena = append(arena, encoded...)
				raw = nil
			}
			for _, r := range raw {
				chunks, samples, err := rustStreamChunks(r, encoding, arena)
				if err != nil {
					free()
					return stats, err
				}
				arena = chunks
				stats.Samples += samples
			}
			stats.Chunks += len(arena) - start
			batch = append(batch, client.QueryStreamSeriesChunks{SeriesIndex: uint64(from + i), Chunks: arena[start:len(arena):len(arena)]})
		}
		from += len(pending)
		pending = pending[:0]
		err := send(&client.QueryStreamResponse{StreamingSeriesChunks: batch})
		clear(arena)
		clear(batch)
		free()
		if err != nil {
			return stats, err
		}
		stats.Batches++
	}
	return stats, nil
}

// rustStreamChunks appends to into the chunks to send for a chunk the engine read: XOR chunks as they are.
func rustStreamChunks(c rustengine.Chunk, encoding chunkenc.Encoding, into []client.Chunk) ([]client.Chunk, int, error) {
	if c.Encoding != rustengine.HistogramEncoding && encoding != chunkenc.EncXOR2 {
		if len(c.Data) < 2 {
			return into, 0, fmt.Errorf("Rust XOR chunk too short: %d", len(c.Data))
		}
		return append(into, client.Chunk{StartTimestampMs: c.Mint, EndTimestampMs: c.Maxt, Encoding: int32(chunk.PrometheusXorChunk), Data: c.Data}), int(binary.BigEndian.Uint16(c.Data)), nil
	}
	metas, err := rustChunkMeta(c, encoding)
	if err != nil {
		return into, 0, err
	}
	samples := 0
	for _, m := range metas {
		ch, err := client.ChunkFromMeta(m)
		if err != nil {
			return into, 0, err
		}
		into = append(into, ch)
		samples += m.Chunk.NumSamples()
	}
	return into, samples, nil
}

func (e *rustEngine) ExemplarQuerier(ctx context.Context) (storage.ExemplarQuerier, error) {
	return &rustExemplarQuerier{engine: e, ctx: ctx}, ctx.Err()
}

type rustExemplarQuerier struct {
	engine *rustEngine
	ctx    context.Context
}

func (q *rustExemplarQuerier) Select(start, end int64, matchers ...[]*labels.Matcher) ([]exemplar.QueryResult, error) {
	if err := q.ctx.Err(); err != nil {
		return nil, err
	}
	var refs []uint64
	if len(matchers) > 0 {
		selected := map[uint64]struct{}{}
		for _, group := range matchers {
			filters, err := q.engine.nativeFilters(q.ctx, group)
			if err != nil {
				return nil, err
			}
			ids, err := q.engine.native.Postings(false, len(filters) == 0, filters)
			if err != nil {
				return nil, err
			}
			for _, id := range ids {
				selected[id] = struct{}{}
			}
		}
		for id := range selected {
			refs = append(refs, id)
		}
		if len(refs) == 0 {
			return nil, nil
		}
	}
	series, err := q.engine.native.Exemplars(start, end, len(matchers) == 0, refs)
	if err != nil {
		return nil, err
	}
	out := make([]exemplar.QueryResult, 0, len(series))
	for _, s := range series {
		result := exemplar.QueryResult{SeriesLabels: rustLabels(s.Labels)}
		for _, ex := range s.Exemplars {
			result.Exemplars = append(result.Exemplars, exemplar.Exemplar{Labels: rustLabels(ex.Labels), Ts: ex.Timestamp, Value: ex.Value, HasTs: true})
		}
		out = append(out, result)
	}
	slices.SortFunc(out, func(a, b exemplar.QueryResult) int { return labels.Compare(a.SeriesLabels, b.SeriesLabels) })
	return out, nil
}
