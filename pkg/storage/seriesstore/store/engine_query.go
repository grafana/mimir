// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"sync"

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
	return &engineChunkQuerier{e: e, mint: mint, maxt: maxt}, nil
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
}

// Close gives back the buffers of the chunks the querier selected, which callers are done with.
func (q *engineChunkQuerier) Close() error {
	q.mu.Lock()
	buffers := q.buffers
	q.buffers = nil
	q.mu.Unlock()
	giveQueryBuffers(buffers)
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

// rawSeries is a selected series: its labels, and its chunks with their own copy of the data.
type rawSeries struct {
	stored labels.Labels
	chunks []chunks.Chunk
}

// selectRaw returns the series matching compiled with chunks in [start, end], sorted by labels,
// with the same chunks the store's QueryStream returns.
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
	var (
		selected []rawSeries
		metas    []ChunkMeta
		scratch  []chunks.Chunk
		data     []byte
		// The chunks' data buffers, which the caller gives back once it is done with the chunks.
		buffers          [][]byte
		candidatesBuffer []chunks.Candidate
	)
	// The chunks' data is copied out while the shard is locked, into one buffer per shard.
	copyOut := func(raw []chunks.Chunk) []chunks.Chunk {
		size := 0
		for index := range raw {
			size += len(raw[index].Data)
		}
		if cap(data)-len(data) < size {
			// Doubling from small, so a query of a few series doesn't pay for a large buffer.
			data = takeQueryBuffer(max(size, min(2*cap(data), 1<<20), 4<<10))
			buffers = append(buffers, data)
		}
		out := make([]chunks.Chunk, len(raw))
		for index := range raw {
			offset := len(data)
			data = append(data, raw[index].Data...)
			out[index] = raw[index]
			out[index].Data = data[offset:len(data):len(data)]
		}
		return out
	}
	shardOf := make(map[*chunks.DiskMapper]int, len(e.store.shards))
	for index, shard := range e.store.shards {
		shardOf[shard.disk] = index
	}
	var headMetas, compacted []ChunkMeta
	e.store.perShardWithCold(e.tenantID, func(t *tenant, disk *chunks.DiskMapper, cold *coldState) {
		watermark := e.oooWatermarks[shardOf[disk]]
		coldSeries := e.store.selectCold(e.tenantID, cold, compiled, coldLabels, coldShard, start, end, prunedBefore)
		t.series.matching(compiled, func(entry *seriesEntry) bool {
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
				selected = append(selected, rawSeries{entry.labels, copyOut(scratch)})
			}
			return true
		})
		for stored, candidate := range coldSeries {
			scratch = queryRawChunks(nil, nil, candidate.chunks, disk, start, end, scratch[:0], &candidatesBuffer)
			if len(scratch) > 0 {
				selected = append(selected, rawSeries{stored, copyOut(scratch)})
			}
		}
	})
	names := labels.TakeSnapshot()
	slices.SortFunc(selected, func(x, y rawSeries) int { return compareLabels(names, x.stored, y.stored) })
	return selected, buffers, nil
}

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
	if len(matchers) == 0 {
		ix.forEachWith(name, func(_ *seriesEntry, value string) { set[value] = struct{}{} })
	} else {
		compiled, err := compilePromMatchers(nil, matchers)
		if err != nil {
			return nil, err
		}
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
	if len(matchers) == 0 {
		ix.forEach(add)
	} else {
		compiled, err := compilePromMatchers(nil, matchers)
		if err != nil {
			return nil, err
		}
		ix.forEachMatching(compiled, add)
	}
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
