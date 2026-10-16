// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"context"
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"
	"sync"
	"unsafe"

	"github.com/prometheus/prometheus/model/exemplar"
	promlabels "github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	tsdbchunks "github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/seriesstore/chunks"
	"github.com/grafana/mimir/pkg/storage/seriesstore/exemplars"
	"github.com/grafana/mimir/pkg/storage/seriesstore/labels"
)

// compilePromMatchers compiles Prometheus matchers, adding a query shard from hints.
func compilePromMatchers(hints *storage.SelectHints, matchers []*promlabels.Matcher) ([]compiledMatcher, error) {
	compiled := make([]compiledMatcher, 0, len(matchers)+1)
	for _, matcher := range matchers {
		switch matcher.Type {
		case promlabels.MatchEqual:
			compiled = append(compiled, compiledMatcher{kind: kindEqual, name: matcher.Name, value: matcher.Value})
		case promlabels.MatchNotEqual:
			compiled = append(compiled, compiledMatcher{kind: kindNotEqual, name: matcher.Name, value: matcher.Value})
		case promlabels.MatchRegexp, promlabels.MatchNotRegexp:
			re, err := anchoredRegex(matcher.Value)
			if err != nil {
				return nil, err
			}
			kind := kindRegex
			if matcher.Type == promlabels.MatchNotRegexp {
				kind = kindNotRegex
			}
			compiled = append(compiled, compiledMatcher{kind: kind, name: matcher.Name, re: re})
		default:
			return nil, fmt.Errorf("invalid matcher type %d", matcher.Type)
		}
	}
	if hints != nil && hints.ShardCount > 0 {
		compiled = append(compiled, compiledMatcher{kind: kindShard, shardIndex: hints.ShardIndex, shardCount: hints.ShardCount})
	}
	return compiled, nil
}

func storeMatchers(matchers []*promlabels.Matcher) []LabelMatcher {
	out := make([]LabelMatcher, len(matchers))
	for index, matcher := range matchers {
		out[index] = LabelMatcher{Type: int32(matcher.Type), Name: matcher.Name, Value: matcher.Value}
	}
	return out
}

// ChunkQuerier returns a querier of the head and cold blocks' chunks in [mint, maxt], like the
// TSDB's. Out-of-order samples are always merged into the chunks they overlap, so the ordered and
// unordered queriers are the same.
func (e *Engine) ChunkQuerier(mint, maxt int64) (storage.ChunkQuerier, error) {
	return &engineChunkQuerier{e: e, mint: mint, maxt: maxt, epoch: e.store.reads.enter()}, nil
}

func (e *Engine) UnorderedChunkQuerier(mint, maxt int64) (storage.ChunkQuerier, error) {
	return e.ChunkQuerier(mint, maxt)
}

type engineChunkQuerier struct {
	e          *Engine
	mint, maxt int64
	// The data buffers of the chunks it selected, given back when it is closed.
	mu      sync.Mutex
	buffers [][]byte
	// The epoch of the store's read guard it reads in: the chunk files it returns chunks of stay mapped until it is
	// closed.
	epoch  uint64
	closed bool
}

// Close gives back the buffers of the chunks the querier selected, which callers are done with, and lets the chunk
// files it read from go.
func (q *engineChunkQuerier) Close() error {
	q.mu.Lock()
	buffers, closed := q.buffers, q.closed
	q.buffers, q.closed = nil, true
	q.mu.Unlock()
	giveQueryBuffers(buffers)
	if !closed {
		q.e.store.reads.leave(q.epoch)
	}
	return nil
}

func (q *engineChunkQuerier) LabelValues(ctx context.Context, name string, hints *storage.LabelHints, matchers ...*promlabels.Matcher) ([]string, annotations.Annotations, error) {
	return (&engineQuerier{e: q.e, mint: q.mint, maxt: q.maxt}).LabelValues(ctx, name, hints, matchers...)
}

func (q *engineChunkQuerier) LabelNames(ctx context.Context, hints *storage.LabelHints, matchers ...*promlabels.Matcher) ([]string, annotations.Annotations, error) {
	return (&engineQuerier{e: q.e, mint: q.mint, maxt: q.maxt}).LabelNames(ctx, hints, matchers...)
}

// Select returns the series with chunks in the range, sorted by labels whatever sortSeries asks:
// the ingester streams them in order.
func (q *engineChunkQuerier) Select(_ context.Context, _ bool, hints *storage.SelectHints, matchers ...*promlabels.Matcher) storage.ChunkSeriesSet {
	start, end := q.mint, q.maxt
	if hints != nil {
		start, end = max(start, hints.Start), min(end, hints.End)
	}
	compiled, err := compilePromMatchers(hints, matchers)
	if err != nil {
		return storage.ErrChunkSeriesSet(err)
	}
	selected, buffers, err := q.e.selectRaw(start, end, compiled)
	if err != nil {
		return storage.ErrChunkSeriesSet(err)
	}
	q.mu.Lock()
	q.buffers = append(q.buffers, buffers...)
	q.mu.Unlock()
	return &engineChunkSeriesSet{series: selected, index: -1}
}

// StreamedSeries is a selected series as a query response sends it.
type StreamedSeries struct {
	labels labels.Labels
	// The chunks, in the encoding the store keeps them in. Their data stays valid until the querier is closed.
	Chunks []chunks.Chunk
}

// AppendLabels appends the series' labels to dst as the ingester sends them, pointing into the store's own: the
// ingester builds them a batch of series at a time, where adapters for all the series of a large response, many
// responses at once, were gigabytes.
func (s *StreamedSeries) AppendLabels(dst []mimirpb.LabelAdapter) []mimirpb.LabelAdapter {
	it := s.labels.Iter()
	for name, value, ok := it.Next(); ok; name, value, ok = it.Next() {
		dst = append(dst, mimirpb.LabelAdapter{Name: name, Value: value})
	}
	return dst
}

// SelectStreamed is Select for a response that streams the series: it returns them in label order with their labels as
// adapters and their chunks as the store has them, without a series set and chunk iterators in between.
func (q *engineChunkQuerier) SelectStreamed(hints *storage.SelectHints, matchers ...*promlabels.Matcher) ([]StreamedSeries, error) {
	start, end := q.mint, q.maxt
	if hints != nil {
		start, end = max(start, hints.Start), min(end, hints.End)
	}
	compiled, err := compilePromMatchers(hints, matchers)
	if err != nil {
		return nil, err
	}
	selected, buffers, err := q.e.selectRaw(start, end, compiled)
	if err != nil {
		return nil, err
	}
	q.mu.Lock()
	q.buffers = append(q.buffers, buffers...)
	q.mu.Unlock()
	out := make([]StreamedSeries, len(selected))
	for index := range selected {
		out[index] = StreamedSeries{labels: selected[index].stored, Chunks: selected[index].chunks}
	}
	return out, nil
}

// rawSeries is a selected series: its labels, and its chunks with their own copy of the data.
type rawSeries struct {
	stored labels.Labels
	chunks []chunks.Chunk
}

// selectRaw returns the series matching compiled with chunks in [start, end], sorted by labels,
// with the same chunks the store's QueryStream returns.
// perShard is the store's perShardIndexed for the engine's tenant, which it finds by what it remembers of where it is
// rather than by hashing its id in every shard of every query.
func (e *Engine) perShard(query func(index int, t *tenant, disk *chunks.DiskMapper, cold *coldState)) {
	for index, shard := range e.store.shards {
		shard.RLock()
		if t, ok := e.tenantLocked(index); ok {
			query(index, t, shard.disk, shard.cold)
		}
		shard.RUnlock()
	}
}

func (e *Engine) selectRaw(start, end int64, compiled []compiledMatcher) ([]rawSeries, [][]byte, error) {
	var coldLabels, coldShard []compiledMatcher
	for _, matcher := range compiled {
		if matcher.kind == kindShard {
			coldShard = append(coldShard, matcher)
		} else {
			coldLabels = append(coldLabels, matcher)
		}
	}
	prunedBefore := e.store.prunedBefore.Load()
	// What a cold block remembers its candidates by.
	coldKey := selectorKey(coldLabels)
	var (
		selected []rawSeries
		metas    []ChunkMeta
		scratch  []chunks.Chunk
		data     []byte
		// The chunks' data buffers, which the caller gives back once it is done with the chunks.
		buffers          [][]byte
		candidatesBuffer []chunks.Candidate
	)
	// The chunks' data in memory is copied out while the shard is locked, into one buffer per shard: the chunk files'
	// stay mapped until the querier is closed, so only the open chunks, which appends change, and the out-of-order
	// ones merged for the query are.
	copyOut := func(raw []chunks.Chunk, disk *chunks.DiskMapper) []chunks.Chunk {
		size := 0
		for index := range raw {
			if !disk.Holds(raw[index].Data) {
				size += len(raw[index].Data)
			}
		}
		if size == 0 {
			return slices.Clone(raw)
		}
		if cap(data)-len(data) < size {
			// Doubling from small, so a query of a few series doesn't pay for a large buffer.
			data = takeQueryBuffer(max(size, min(2*cap(data), 1<<20), 4<<10))
			buffers = append(buffers, data)
		}
		out := make([]chunks.Chunk, len(raw))
		for index := range raw {
			out[index] = raw[index]
			if disk.Holds(raw[index].Data) {
				continue
			}
			offset := len(data)
			data = append(data, raw[index].Data...)
			out[index].Data = data[offset:len(data):len(data)]
		}
		return out
	}
	var headMetas, compacted []ChunkMeta
	// A selector with a metric name that comes in again finds its series in the shard's candidates.
	var (
		cached     *cachedSelector
		cachedName string
		cachedRest []compiledMatcher
		general    *cachedSelector
	)
	if name, rest, ok := cacheableName(coldLabels); ok {
		cached, cachedName, cachedRest = e.selectors.get(selectorKey(coldLabels), len(e.store.shards)), name, rest
	} else if len(coldLabels) > 0 {
		// Selectors with no metric name equality are kept apart from the others: what is kept is by series id.
		general = e.selectors.get("\x02"+selectorKey(coldLabels), len(e.store.shards))
	}
	e.perShard(func(shard int, t *tenant, disk *chunks.DiskMapper, cold *coldState) {
		watermark := e.oooWatermarks[shard]
		coldSeries := e.store.selectCold(e.tenantID, cold, compiled, coldLabels, coldShard, coldKey, start, end, prunedBefore)
		matchHead := func(visit func(entry *seriesEntry) bool) {
			if cached != nil {
				t.series.matchingCached(cached, shard, cachedName, coldShard, coldLabels, cachedRest, visit)
				return
			}
			if general != nil {
				t.series.matchingGeneral(general, shard, coldShard, coldLabels, visit)
				return
			}
			t.series.matching(compiled, visit)
		}
		matchHead(func(entry *seriesEntry) bool {
			series := &entry.series
			metas = series.chunks.appendTo(metas[:0])
			overlaps := false
			for index := range metas {
				if metas[index].MinTime <= end && metas[index].MaxTime >= start {
					overlaps = true
					break
				}
			}
			if !overlaps {
				overlaps = series.headOverlaps(start, end)
			}
			var coldChunks []ChunkMeta
			hasCold := false
			if len(coldSeries) > 0 {
				if candidate, ok := coldSeries[entry.labels]; ok {
					coldChunks, hasCold = candidate.chunks, true
					delete(coldSeries, entry.labels)
				}
			}
			if !overlaps && !hasCold {
				return true
			}
			// Compacted out-of-order chunks are in blocks: Prometheus doesn't merge them with the
			// head's chunks, and returns them after, so the head's copy of a conflicting duplicate
			// comes first.
			headMetas, compacted = headMetas[:0], compacted[:0]
			for _, meta := range metas {
				if meta.OutOfOrder && uint64(meta.Ref) < watermark {
					compacted = append(compacted, meta)
				} else {
					headMetas = append(headMetas, meta)
				}
			}
			scratch = queryRawChunks(series, headMetas, coldChunks, disk, start, end, scratch[:0], &candidatesBuffer)
			for _, meta := range compacted {
				if meta.MinTime <= end && meta.MaxTime >= start {
					scratch = append(scratch, chunks.Chunk{MinTime: meta.MinTime, MaxTime: meta.MaxTime, Encoding: int32(meta.Encoding), Data: disk.Read(meta.Ref, meta.Len)})
				}
			}
			if len(scratch) > 0 {
				selected = append(selected, rawSeries{entry.labels, copyOut(scratch, disk)})
			}
			return true
		})
		for stored, candidate := range coldSeries {
			scratch = queryRawChunks(nil, nil, candidate.chunks, disk, start, end, scratch[:0], &candidatesBuffer)
			if len(scratch) > 0 {
				selected = append(selected, rawSeries{stored, copyOut(scratch, disk)})
			}
		}
	})
	sortByLabels(selected)
	return selected, buffers, nil
}

// sortByLabels sorts series in Prometheus's label order. A query's series are mostly one metric's, sharing most of
// their labels, which comparing them decoded on every comparison: each is decoded once instead, into a key of its
// names and values that compares as bytes in the same order, "name\x00value\x00" for each label. A label with a zero
// byte would compare otherwise, so then the series are compared as they are. The keys' memory is kept between sorts.
func sortByLabels(selected []rawSeries) {
	if len(selected) < 2 {
		return
	}
	names := labels.TakeSnapshot()
	compare := func(x, y rawSeries) int { return compareLabels(names, x.stored, y.stored) }
	if len(selected) < 32 {
		slices.SortFunc(selected, compare)
		return
	}
	size := 0
	for index := range selected {
		it := selected[index].stored.Iter()
		for name, value, ok := it.Next(); ok; name, value, ok = it.Next() {
			if strings.IndexByte(name, 0) >= 0 || strings.IndexByte(value, 0) >= 0 {
				slices.SortFunc(selected, compare)
				return
			}
			size += len(name) + len(value) + 2
		}
	}
	scratch, _ := sortScratch.Get().(*labelSortScratch)
	if scratch == nil {
		scratch = &labelSortScratch{}
	}
	defer func() {
		// Large sorts' memory isn't kept for the small ones.
		if cap(scratch.buffer) <= 64<<20 {
			clear(scratch.keys)
			scratch.buffer, scratch.keys = scratch.buffer[:0], scratch.keys[:0]
			sortScratch.Put(scratch)
		}
	}()
	if cap(scratch.buffer) < size {
		scratch.buffer = make([]byte, 0, size)
	}
	buffer := scratch.buffer
	keys := slices.Grow(scratch.keys, len(selected))[:len(selected)]
	scratch.keys = keys
	for index := range selected {
		start := len(buffer)
		it := selected[index].stored.Iter()
		for name, value, ok := it.Next(); ok; name, value, ok = it.Next() {
			buffer = append(buffer, name...)
			buffer = append(buffer, 0)
			buffer = append(buffer, value...)
			buffer = append(buffer, 0)
		}
		// The buffer holds every key, so it is never reallocated under the keys taken from it.
		keys[index] = keyedSeries{key: unsafe.String(unsafe.SliceData(buffer[start:]), len(buffer)-start), series: selected[index]}
	}
	slices.SortFunc(keys, func(x, y keyedSeries) int { return strings.Compare(x.key, y.key) })
	for index := range keys {
		selected[index] = keys[index].series
	}
}

type keyedSeries struct {
	key    string
	series rawSeries
}

type labelSortScratch struct {
	buffer []byte
	keys   []keyedSeries
}

var sortScratch sync.Pool

// The buffers a query copies its chunks' data into are kept between queries, in two sizes: allocating them was a tenth
// of what an ingester allocated, and the memory clearing and garbage collection that goes with it.
const (
	smallQueryBuffer = 64 << 10
	largeQueryBuffer = 1 << 20
)

var queryBuffers [2]sync.Pool

// takeQueryBuffer returns an empty buffer that holds size bytes, which giveQueryBuffers may get back.
func takeQueryBuffer(size int) []byte {
	switch {
	case size <= smallQueryBuffer:
		if buffer, ok := queryBuffers[0].Get().(*[]byte); ok {
			return (*buffer)[:0]
		}
		return make([]byte, 0, smallQueryBuffer)
	case size <= largeQueryBuffer:
		if buffer, ok := queryBuffers[1].Get().(*[]byte); ok {
			return (*buffer)[:0]
		}
		return make([]byte, 0, largeQueryBuffer)
	}
	return make([]byte, 0, size)
}

// giveQueryBuffers keeps the buffers of a query that is done with its chunks.
func giveQueryBuffers(buffers [][]byte) {
	for _, buffer := range buffers {
		switch cap(buffer) {
		case smallQueryBuffer:
			buffer = buffer[:0]
			queryBuffers[0].Put(&buffer)
		case largeQueryBuffer:
			buffer = buffer[:0]
			queryBuffers[1].Put(&buffer)
		}
	}
}

type engineChunkSeriesSet struct {
	series  []rawSeries
	index   int
	builder promlabels.ScratchBuilder
	slab    adapterSlab
	current engineChunkSeries
}

func (s *engineChunkSeriesSet) Next() bool {
	s.index++
	if s.index >= len(s.series) {
		return false
	}
	raw := &s.series[s.index]
	s.slab.remaining = len(s.series) - s.index
	s.current = engineChunkSeries{stored: raw.stored, chunks: raw.chunks, builder: &s.builder, slab: &s.slab}
	return true
}

func (s *engineChunkSeriesSet) At() storage.ChunkSeries           { return &s.current }
func (s *engineChunkSeriesSet) Err() error                        { return nil }
func (s *engineChunkSeriesSet) Warnings() annotations.Annotations { return nil }

// engineChunkSeries is a selected series, whose chunks decode from the store's data.
type engineChunkSeries struct {
	stored labels.Labels
	chunks []chunks.Chunk
	// What labels are built with, which the series' set keeps from series to series.
	builder *promlabels.ScratchBuilder
	slab    *adapterSlab
}

// Labels builds the Prometheus labels of the series, which the ingester doesn't need for the series it streams.
func (s *engineChunkSeries) Labels() promlabels.Labels { return toPromLabels(s.stored, s.builder) }

// LabelAdapters returns the labels as the ingester sends them, which point into the stored labels and share arrays
// between series: the Prometheus labels it would convert were two allocations a series.
func (s *engineChunkSeries) LabelAdapters() []mimirpb.LabelAdapter { return s.slab.adapters(s.stored) }

// adapterSlab hands out label adapters from arrays that hold many series' labels.
type adapterSlab struct {
	free []mimirpb.LabelAdapter
	// How many series are left, to size an array that a short result doesn't waste.
	remaining int
}

func (a *adapterSlab) adapters(stored labels.Labels) []mimirpb.LabelAdapter {
	n := stored.Len()
	if len(a.free) < n {
		a.free = make([]mimirpb.LabelAdapter, max(n, min(4096, a.remaining*n)))
	}
	out := a.free[:0:n]
	it := stored.Iter()
	for name, value, ok := it.Next(); ok; name, value, ok = it.Next() {
		out = append(out, mimirpb.LabelAdapter{Name: name, Value: value})
	}
	a.free = a.free[n:]
	return out
}

func (s *engineChunkSeries) ChunkCount() (int, error) { return len(s.chunks), nil }

func (s *engineChunkSeries) IteratorFactory() storage.ChunkIterable {
	// What it iterates are the chunks.
	return &engineChunkSeries{stored: s.stored, chunks: s.chunks}
}

func (s *engineChunkSeries) Iterator(it tsdbchunks.Iterator) tsdbchunks.Iterator {
	if existing, ok := it.(*engineChunkIterator); ok {
		existing.reset(s.chunks)
		return existing
	}
	iterator := &engineChunkIterator{}
	iterator.reset(s.chunks)
	return iterator
}

type engineChunkIterator struct {
	chunks []chunks.Chunk
	index  int
	meta   tsdbchunks.Meta
	err    error
}

func (it *engineChunkIterator) reset(list []chunks.Chunk) {
	it.chunks, it.index, it.err = list, -1, nil
}

func (it *engineChunkIterator) Next() bool {
	it.index++
	if it.index >= len(it.chunks) || it.err != nil {
		return false
	}
	raw := &it.chunks[it.index]
	chunk, err := chunkenc.FromData(chunkEncoding(raw.Encoding), raw.Data)
	if err != nil {
		it.err = err
		return false
	}
	it.meta = tsdbchunks.Meta{MinTime: raw.MinTime, MaxTime: raw.MaxTime, Chunk: chunk}
	return true
}

func (it *engineChunkIterator) At() tsdbchunks.Meta { return it.meta }
func (it *engineChunkIterator) Err() error          { return it.err }

// chunkEncoding maps the store's wire encodings to Prometheus's chunk encodings.
func chunkEncoding(wire int32) chunkenc.Encoding {
	switch wire {
	case chunks.EncodingHistogram:
		return chunkenc.EncHistogram
	case chunks.EncodingFloatHistogram:
		return chunkenc.EncFloatHistogram
	default:
		return chunkenc.EncXOR
	}
}

// Querier returns a querier of the head and cold blocks' series in [mint, maxt], like the TSDB's.
func (e *Engine) Querier(mint, maxt int64) (storage.Querier, error) {
	return &engineQuerier{e: e, mint: mint, maxt: maxt}, nil
}

type engineQuerier struct {
	e          *Engine
	mint, maxt int64
}

func (q *engineQuerier) Close() error { return nil }

// limited applies a lookup's limit; like Prometheus, nothing found is nil.
func limited(values []string, limit int) []string {
	if len(values) == 0 {
		return nil
	}
	if limit > 0 && len(values) > limit {
		return values[:limit]
	}
	return values
}

func (q *engineQuerier) LabelValues(_ context.Context, name string, hints *storage.LabelHints, matchers ...*promlabels.Matcher) ([]string, annotations.Annotations, error) {
	values, err := q.e.store.LabelValues(q.e.tenantID, name, q.mint, q.maxt, storeMatchers(matchers))
	if err != nil {
		return nil, nil, err
	}
	limit := 0
	if hints != nil {
		limit = hints.Limit
	}
	return limited(values, limit), nil, nil
}

func (q *engineQuerier) LabelNames(_ context.Context, hints *storage.LabelHints, matchers ...*promlabels.Matcher) ([]string, annotations.Annotations, error) {
	names, err := q.e.store.LabelNames(q.e.tenantID, q.mint, q.maxt, storeMatchers(matchers))
	if err != nil {
		return nil, nil, err
	}
	limit := 0
	if hints != nil {
		limit = hints.Limit
	}
	return limited(names, limit), nil, nil
}

// Select returns the labels of the series with samples in the range, sorted: the ingester only
// asks for series, not their samples.
func (q *engineQuerier) Select(_ context.Context, _ bool, hints *storage.SelectHints, matchers ...*promlabels.Matcher) storage.SeriesSet {
	start, end := q.mint, q.maxt
	if hints != nil {
		start, end = max(start, hints.Start), min(end, hints.End)
	}
	selected, err := q.e.store.SelectLabels(q.e.tenantID, start, end, storeMatchers(matchers))
	if err != nil {
		return storage.ErrSeriesSet(err)
	}
	series := make([]storage.Series, 0, len(selected))
	var builder promlabels.ScratchBuilder
	for _, pairs := range selected {
		builder.Reset()
		for _, pair := range pairs {
			builder.Add(pair[0], pair[1])
		}
		series = append(series, labelsOnlySeries{builder.Labels()})
	}
	sort.Slice(series, func(x, y int) bool { return promlabels.Compare(series[x].Labels(), series[y].Labels()) < 0 })
	if hints != nil && hints.Limit > 0 && len(series) > hints.Limit {
		series = series[:hints.Limit]
	}
	return &sliceSeriesSet{series: series, index: -1}
}

func (q *engineQuerier) SearchLabelNames(ctx context.Context, hints *storage.SearchHints, matchers ...*promlabels.Matcher) storage.SearchResultSet {
	names, _, err := q.LabelNames(ctx, nil, matchers...)
	if err != nil {
		return storage.ErrSearchResultSet(err)
	}
	return storage.NewSearchResultSetFromSlice(storage.ApplySearchHints(names, hints), nil)
}

func (q *engineQuerier) SearchLabelValues(ctx context.Context, name string, hints *storage.SearchHints, matchers ...*promlabels.Matcher) storage.SearchResultSet {
	values, _, err := q.LabelValues(ctx, name, nil, matchers...)
	if err != nil {
		return storage.ErrSearchResultSet(err)
	}
	return storage.NewSearchResultSetFromSlice(storage.ApplySearchHints(values, hints), nil)
}

type labelsOnlySeries struct{ labels promlabels.Labels }

func (s labelsOnlySeries) Labels() promlabels.Labels { return s.labels }
func (s labelsOnlySeries) Iterator(chunkenc.Iterator) chunkenc.Iterator {
	return chunkenc.NewNopIterator()
}

type sliceSeriesSet struct {
	series []storage.Series
	index  int
}

func (s *sliceSeriesSet) Next() bool {
	s.index++
	return s.index < len(s.series)
}
func (s *sliceSeriesSet) At() storage.Series                { return s.series[s.index] }
func (s *sliceSeriesSet) Err() error                        { return nil }
func (s *sliceSeriesSet) Warnings() annotations.Annotations { return nil }

// ExemplarQuerier returns a querier of the tenant's exemplars.
func (e *Engine) ExemplarQuerier(context.Context) (storage.ExemplarQuerier, error) {
	return engineExemplarQuerier{e}, nil
}

type engineExemplarQuerier struct{ e *Engine }

// Select returns the exemplars in [start, end] of the series matching any of the matcher sets.
func (q engineExemplarQuerier) Select(start, end int64, matcherSets ...[]*promlabels.Matcher) ([]exemplar.QueryResult, error) {
	compiled := make([][]compiledMatcher, 0, len(matcherSets))
	for _, set := range matcherSets {
		c, err := compilePromMatchers(nil, set)
		if err != nil {
			return nil, err
		}
		compiled = append(compiled, c)
	}
	q.e.store.exemplarsLock.Lock()
	tenantExemplars, ok := q.e.store.exemplars[q.e.tenantID]
	var selected []exemplars.Selected[labels.Labels]
	if ok {
		selected = tenantExemplars.Select(start, end, func(stored *labels.Labels) bool {
			for _, set := range compiled {
				if matches(*stored, set) {
					return true
				}
			}
			return false
		})
	}
	q.e.store.exemplarsLock.Unlock()
	slices.SortFunc(selected, func(a, b exemplars.Selected[labels.Labels]) int { return labels.Compare(a.Labels, b.Labels) })
	results := make([]exemplar.QueryResult, 0, len(selected))
	var builder promlabels.ScratchBuilder
	for _, series := range selected {
		result := exemplar.QueryResult{SeriesLabels: toPromLabels(series.Labels, &builder)}
		for _, stored := range series.Exemplars {
			builder.Reset()
			for _, label := range stored.Labels {
				builder.Add(label.Name, label.Value)
			}
			result.Exemplars = append(result.Exemplars, exemplar.Exemplar{Labels: builder.Labels(), Value: stored.Value, Ts: stored.TimestampMs, HasTs: true})
		}
		results = append(results, result)
	}
	return results, nil
}

// ForEachSecondaryHash calls fn with the head's series and their owned series tokens, a store
// shard at a time, like the head's.
func (e *Engine) ForEachSecondaryHash(fn func(refs []tsdbchunks.HeadSeriesRef, secondaryHashes []uint32)) {
	var (
		refs   []tsdbchunks.HeadSeriesRef
		hashes []uint32
	)
	for _, shard := range e.store.shards {
		refs, hashes = refs[:0], hashes[:0]
		shard.RLock()
		if t, ok := shard.tenants[e.tenantID]; ok {
			t.series.forEach(func(entry *seriesEntry) {
				refs = append(refs, tsdbchunks.HeadSeriesRef(entry.series.ref))
				hashes = append(hashes, entry.series.ownedHash)
			})
		}
		shard.RUnlock()
		if len(refs) > 0 {
			fn(refs, hashes)
		}
	}
}

// ForEachShardHash calls fn with the head's series and their query shard hashes.
func (e *Engine) ForEachShardHash(fn func(refs []storage.SeriesRef, shardHashes []uint64)) {
	var (
		refs   []storage.SeriesRef
		hashes []uint64
	)
	for _, shard := range e.store.shards {
		refs, hashes = refs[:0], hashes[:0]
		shard.RLock()
		if t, ok := shard.tenants[e.tenantID]; ok {
			t.series.forEach(func(entry *seriesEntry) {
				refs = append(refs, storage.SeriesRef(entry.series.ref))
				hashes = append(hashes, entry.series.shardHash)
			})
		}
		shard.RUnlock()
		if len(refs) > 0 {
			fn(refs, hashes)
		}
	}
}

// Index returns a reader of the head's index, like the head's.
func (e *Engine) Index() (tsdb.IndexReader, error) {
	return &engineIndex{e: e}, nil
}

func (e *Engine) MustIndex() tsdb.IndexReader {
	return &engineIndex{e: e}
}

// engineIndex reads the head's series: postings list series references in increasing order.
type engineIndex struct {
	e *Engine
	// The postings of every value of a label, built once a reader asks for a second of its values, as the ingester's
	// label APIs ask for each value of each label in turn: each was a pass over the label's series, with every shard
	// locked. A reader lasts a request, so they are as of its first lookup of the label.
	mu      sync.Mutex
	asked   map[string]int
	byValue map[string]map[string][]storage.SeriesRef
}

func (ix *engineIndex) Close() error { return nil }

// forEach calls visit with every head series, a shard at a time with the shard read-locked.
func (ix *engineIndex) forEach(visit func(entry *seriesEntry)) {
	for _, shard := range ix.e.store.shards {
		shard.RLock()
		if t, ok := shard.tenants[ix.e.tenantID]; ok {
			t.series.forEach(visit)
		}
		shard.RUnlock()
	}
}

// forEachMatching calls visit with every head series matching compiled.
func (ix *engineIndex) forEachMatching(compiled []compiledMatcher, visit func(entry *seriesEntry)) {
	for _, shard := range ix.e.store.shards {
		shard.RLock()
		if t, ok := shard.tenants[ix.e.tenantID]; ok {
			t.series.matching(compiled, func(entry *seriesEntry) bool {
				visit(entry)
				return true
			})
		}
		shard.RUnlock()
	}
}

// forEachWith calls visit with every head series that has the label name.
func (ix *engineIndex) forEachWith(name string, visit func(entry *seriesEntry, value string)) {
	if name == metricNameLabel {
		ix.forEach(func(entry *seriesEntry) { visit(entry, entry.labels.ValueOf(metricNameID)) })
		return
	}
	id, known := labels.Lookup(name)
	if !known {
		return
	}
	for _, shard := range ix.e.store.shards {
		shard.RLock()
		if t, ok := shard.tenants[ix.e.tenantID]; ok {
			t.series.visitRefs(t.series.refsWith(id, known), func(entry *seriesEntry) bool {
				if value, ok := labelValue(entry.labels, id); ok {
					visit(entry, value)
				}
				return true
			})
		}
		shard.RUnlock()
	}
}

// forEachIdleEvicted calls visit with the cold series matching compiled that the engine evicted for being idle and that
// Prometheus's head still holds, as it only collects a series once a head compaction leaves it without samples since the
// head's min valid time: a shard at a time, with the shard read-locked, as what visit gets is in the shard's blocks.
func (ix *engineIndex) forEachIdleEvicted(compiled []compiledMatcher, visit func(series *coldSeries)) {
	home, t := ix.e.home()
	if t == nil {
		return
	}
	home.RLock()
	blocks, headMin := t.blocks, t.headMin
	home.RUnlock()
	var idle []emulatedBlock
	for _, block := range blocks {
		if block.idleEvicted && block.maxTime > headMin {
			idle = append(idle, block)
		}
	}
	if len(idle) == 0 {
		return
	}
	for _, shard := range ix.e.store.shards {
		shard.RLock()
		if _, ok := shard.tenants[ix.e.tenantID]; ok {
			shard.cold.matchingIndexed(ix.e.tenantID, compiled, headMin, math.MaxInt64, func(table *coldTenantIndex, index int, series *coldSeries) {
				hash := series.block.labelsHash(table, index)
				for i := range idle {
					if idle[i].holds(hash) {
						visit(series)
						return
					}
				}
			})
		}
		shard.RUnlock()
	}
}

func postingsOf(refs []storage.SeriesRef) index.Postings {
	slices.Sort(refs)
	return index.NewListPostings(slices.Compact(refs))
}

func (ix *engineIndex) Symbols() index.StringIter {
	set := map[string]struct{}{}
	ix.forEach(func(entry *seriesEntry) {
		entry.labels.Range(func(name, value string) {
			set[name] = struct{}{}
			set[value] = struct{}{}
		})
	})
	return index.NewStringListIter(sortedKeys(set))
}

func (ix *engineIndex) SortedLabelValues(ctx context.Context, name string, hints *storage.LabelHints, matchers ...*promlabels.Matcher) ([]string, error) {
	return ix.LabelValues(ctx, name, hints, matchers...)
}

// LabelValues returns the values of name of the head's series matching matchers, sorted.
func (ix *engineIndex) LabelValues(_ context.Context, name string, hints *storage.LabelHints, matchers ...*promlabels.Matcher) ([]string, error) {
	set := map[string]struct{}{}
	compiled, err := compilePromMatchers(nil, matchers)
	if err != nil {
		return nil, err
	}
	if len(matchers) == 0 {
		ix.forEachWith(name, func(_ *seriesEntry, value string) { set[value] = struct{}{} })
	} else {
		id, known := labels.Lookup(name)
		ix.forEachMatching(compiled, func(entry *seriesEntry) {
			if !known {
				return
			}
			if value, ok := labelValue(entry.labels, id); ok {
				set[value] = struct{}{}
			}
		})
	}
	ix.forEachIdleEvicted(compiled, func(series *coldSeries) {
		if value, ok := series.lookup(name); ok {
			if _, seen := set[value]; !seen {
				// The value is in the block's mapping, which a compaction may unmap once the shard is unlocked.
				set[strings.Clone(value)] = struct{}{}
			}
		}
	})
	values := sortedKeys(set)
	if hints != nil {
		values = limited(values, hints.Limit)
	}
	return values, nil
}

// LabelNames returns the label names of the head's series matching matchers, sorted.
func (ix *engineIndex) LabelNames(_ context.Context, matchers ...*promlabels.Matcher) ([]string, error) {
	set := map[string]struct{}{}
	add := func(entry *seriesEntry) {
		entry.labels.Range(func(name, _ string) { set[name] = struct{}{} })
	}
	compiled, err := compilePromMatchers(nil, matchers)
	if err != nil {
		return nil, err
	}
	if len(matchers) == 0 {
		ix.forEach(add)
	} else {
		ix.forEachMatching(compiled, add)
	}
	// The blocks' names are interned, so they don't reference the mapping.
	ix.forEachIdleEvicted(compiled, func(series *coldSeries) {
		series.rangeNameIDs(func(id uint64) { set[series.block.names[id]] = struct{}{} })
	})
	return sortedKeys(set), nil
}

func (ix *engineIndex) LabelNamesFor(_ context.Context, postings index.Postings) ([]string, error) {
	wanted := map[uint64]struct{}{}
	for postings.Next() {
		wanted[uint64(postings.At())] = struct{}{}
	}
	if err := postings.Err(); err != nil {
		return nil, err
	}
	set := map[string]struct{}{}
	ix.forEach(func(entry *seriesEntry) {
		if _, ok := wanted[entry.series.ref]; ok {
			entry.labels.Range(func(name, _ string) { set[name] = struct{}{} })
		}
	})
	return sortedKeys(set), nil
}

// Postings returns the head's series with the label name set to one of values; the empty name and
// value, index.AllPostingsKey, is every series.
func (ix *engineIndex) Postings(_ context.Context, name string, values ...string) (index.Postings, error) {
	allName, allValue := index.AllPostingsKey()
	if name == allName && len(values) == 1 && values[0] == allValue {
		var refs []storage.SeriesRef
		ix.forEach(func(entry *seriesEntry) { refs = append(refs, storage.SeriesRef(entry.series.ref)) })
		return postingsOf(refs), nil
	}
	if len(values) == 1 {
		if refs, ok := ix.cachedPostings(name, values[0]); ok {
			// Sorted and unique already, and shared with the reader's other lookups of the value.
			return index.NewListPostings(refs), nil
		}
	}
	wanted := make(map[string]struct{}, len(values))
	for _, value := range values {
		wanted[value] = struct{}{}
	}
	var refs []storage.SeriesRef
	if name == metricNameLabel {
		for _, shard := range ix.e.store.shards {
			shard.RLock()
			if t, ok := shard.tenants[ix.e.tenantID]; ok {
				for value := range wanted {
					if groupID, ok := t.series.names[value]; ok {
						for _, entry := range t.series.groups[groupID].entries {
							refs = append(refs, storage.SeriesRef(entry.series.ref))
						}
					}
				}
			}
			shard.RUnlock()
		}
		return postingsOf(refs), nil
	}
	id, known := labels.Lookup(name)
	if !known {
		return index.EmptyPostings(), nil
	}
	for _, shard := range ix.e.store.shards {
		shard.RLock()
		if t, ok := shard.tenants[ix.e.tenantID]; ok && int(id) < len(t.series.postings) && t.series.postings[id] != nil {
			var lists []uint32
			total := 0
			for value := range wanted {
				if posting, ok := t.series.postings[id][postingKey(value)]; ok {
					lists = append(lists, posting)
					total += t.series.postingLen(posting)
				}
			}
			// A posting holds a value's hash: collisions are checked against the labels.
			t.series.visitRefs(t.series.refsOf(lists, total), func(entry *seriesEntry) bool {
				if value, ok := labelValue(entry.labels, id); ok {
					if _, match := wanted[value]; match {
						refs = append(refs, storage.SeriesRef(entry.series.ref))
					}
				}
				return true
			})
		}
		shard.RUnlock()
	}
	return postingsOf(refs), nil
}

// cachedPostings returns the series with the label's value from the postings of all its values, once the reader asked
// for another of its values.
func (ix *engineIndex) cachedPostings(name, value string) ([]storage.SeriesRef, bool) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	values, ok := ix.byValue[name]
	if !ok {
		if ix.asked == nil {
			ix.asked = map[string]int{}
		}
		if ix.asked[name]++; ix.asked[name] < 2 {
			return nil, false
		}
		values = map[string][]storage.SeriesRef{}
		// The values point into the series' labels, which are their own allocations: they stay valid for the reader.
		ix.forEachWith(name, func(entry *seriesEntry, v string) {
			values[v] = append(values[v], storage.SeriesRef(entry.series.ref))
		})
		for _, list := range values {
			slices.Sort(list)
		}
		if ix.byValue == nil {
			ix.byValue = map[string]map[string][]storage.SeriesRef{}
		}
		ix.byValue[name] = values
	}
	return values[value], true
}

func (ix *engineIndex) PostingsForLabelMatching(_ context.Context, name string, match func(value string) bool) index.Postings {
	var refs []storage.SeriesRef
	ix.forEachWith(name, func(entry *seriesEntry, value string) {
		if match(value) {
			refs = append(refs, storage.SeriesRef(entry.series.ref))
		}
	})
	return postingsOf(refs)
}

func (ix *engineIndex) PostingsForAllLabelValues(_ context.Context, name string) index.Postings {
	var refs []storage.SeriesRef
	ix.forEachWith(name, func(entry *seriesEntry, _ string) {
		refs = append(refs, storage.SeriesRef(entry.series.ref))
	})
	return postingsOf(refs)
}

// PostingsForMatchers selects the head's series with the store's matching: name groups, postings
// and presence lists.
func (ix *engineIndex) PostingsForMatchers(_ context.Context, _ bool, matchers ...*promlabels.Matcher) (index.Postings, error) {
	compiled, err := compilePromMatchers(nil, matchers)
	if err != nil {
		return nil, err
	}
	var refs []storage.SeriesRef
	ix.forEachMatching(compiled, func(entry *seriesEntry) { refs = append(refs, storage.SeriesRef(entry.series.ref)) })
	return postingsOf(refs), nil
}

// PostingsForMatchersNative is PostingsForMatchers by the store's matching, for callers that would otherwise assemble
// the postings from one list for each matcher with the index's per-label methods: those read every series that has the
// label, to find the values that a regex or a negation accepts.
func (ix *engineIndex) PostingsForMatchersNative(ctx context.Context, matchers ...*promlabels.Matcher) (index.Postings, error) {
	return ix.PostingsForMatchers(ctx, false, matchers...)
}

// SortedPostings orders postings by their series' labels, like the head's.
func (ix *engineIndex) SortedPostings(postings index.Postings) index.Postings {
	type item struct {
		ref    storage.SeriesRef
		labels labels.Labels
	}
	wanted := map[uint64]struct{}{}
	for postings.Next() {
		wanted[uint64(postings.At())] = struct{}{}
	}
	if err := postings.Err(); err != nil {
		return index.ErrPostings(err)
	}
	var items []item
	ix.forEach(func(entry *seriesEntry) {
		if _, ok := wanted[entry.series.ref]; ok {
			items = append(items, item{storage.SeriesRef(entry.series.ref), entry.labels})
		}
	})
	names := labels.TakeSnapshot()
	slices.SortFunc(items, func(x, y item) int { return compareLabels(names, x.labels, y.labels) })
	refs := make([]storage.SeriesRef, len(items))
	for index, it := range items {
		refs[index] = it.ref
	}
	return index.NewListPostings(refs)
}

// ShardedPostings keeps the postings in query shard shardIndex of shardCount.
func (ix *engineIndex) ShardedPostings(postings index.Postings, shardIndex, shardCount uint64) index.Postings {
	var refs []storage.SeriesRef
	for postings.Next() {
		ref := uint64(postings.At())
		ix.e.withSeries(ref, func(series *Series) {
			if series.shardHash%shardCount == shardIndex {
				refs = append(refs, storage.SeriesRef(ref))
			}
		})
	}
	if err := postings.Err(); err != nil {
		return index.ErrPostings(err)
	}
	return index.NewListPostings(refs)
}

// Series reads the labels of the series ref; the head's chunks aren't listed.
func (ix *engineIndex) Series(ref storage.SeriesRef, builder *promlabels.ScratchBuilder, chks *[]tsdbchunks.Meta) error {
	found := ix.e.withSeriesEntry(uint64(ref), func(entry *seriesEntry) {
		builder.Reset()
		entry.labels.Range(func(name, value string) { builder.Add(name, value) })
	})
	if !found {
		return storage.ErrNotFound
	}
	if chks != nil {
		*chks = (*chks)[:0]
	}
	return nil
}

func (ix *engineIndex) IndexLookupPlanner() index.LookupPlanner {
	return nil
}

// withSeriesEntry calls read with the series ref's entry, its shard read-locked.
func (e *Engine) withSeriesEntry(ref uint64, read func(entry *seriesEntry)) bool {
	shardIndex := refShard(ref)
	if shardIndex >= len(e.store.shards) {
		return false
	}
	shard := e.store.shards[shardIndex]
	shard.RLock()
	defer shard.RUnlock()
	t, ok := shard.tenants[e.tenantID]
	if !ok {
		return false
	}
	entry, ok := e.lookupLocked(t, ref)
	if ok {
		read(entry)
	}
	return ok
}

var (
	_ tsdb.IndexReader        = (*engineIndex)(nil)
	_ storage.ChunkQuerier    = (*engineChunkQuerier)(nil)
	_ storage.Querier         = (*engineQuerier)(nil)
	_ storage.Searcher        = (*engineQuerier)(nil)
	_ storage.ExemplarQuerier = engineExemplarQuerier{}
	_ EngineAppender          = (*engineAppender)(nil)
)
