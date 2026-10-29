// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"context"
	"fmt"
	"math"
	"slices"
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
	q := &engineChunkQuerier{e: e, mint: mint, maxt: maxt, epoch: e.store.reads.enter()}
	q.buffers, q.selections, q.slabs = q.firstBuffers[:0], q.firstSelection[:0], q.firstSlab[:0]
	return q, nil
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
	// The series it selected for streamed responses and their chunk lists' slabs, given back when it is closed.
	selections [][]rawSeries
	slabs      [][]chunks.Chunk
	// What a querier of one selection holds, without a list of each.
	firstBuffers   [2][]byte
	firstSelection [1][]rawSeries
	firstSlab      [1][]chunks.Chunk
	// Room for its first selection, if small.
	room      selectRoom
	roomTaken bool
	// The epoch of the store's read guard it reads in: the chunk files it returns chunks of stay mapped until it is
	// closed.
	epoch  uint64
	closed bool
}

// selectRoom is a querier's room for what a small query selects, its series and their chunk lists: a query of a few
// series takes nothing from the pools, which cost it more than they saved.
type selectRoom struct {
	series [2]rawSeries
	chunks [8]chunks.Chunk
}

// takeRoom returns the querier's room the first time only: what it holds stays in use until the querier is closed.
// With mu held.
func (q *engineChunkQuerier) takeRoom() *selectRoom {
	if q.roomTaken {
		return nil
	}
	q.roomTaken = true
	return &q.room
}

// Close gives back the buffers of the chunks the querier selected, which callers are done with, and lets the chunk
// files it read from go.
func (q *engineChunkQuerier) Close() error {
	q.mu.Lock()
	buffers, selections, slabs, closed := q.buffers, q.selections, q.slabs, q.closed
	q.buffers, q.selections, q.slabs, q.closed = nil, nil, nil, true
	q.mu.Unlock()
	giveQueryBuffers(buffers)
	giveChunkSlabs(slabs)
	for _, selected := range selections {
		giveSelection(selected)
	}
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
	q.mu.Lock()
	room := q.takeRoom()
	q.mu.Unlock()
	selected, buffers, slabs, pooled, err := q.e.selectRaw(start, end, compiled, room)
	if err != nil {
		return storage.ErrChunkSeriesSet(err)
	}
	q.mu.Lock()
	q.buffers = append(q.buffers, buffers...)
	q.slabs = append(q.slabs, slabs...)
	if pooled {
		q.selections = append(q.selections, selected)
	}
	q.mu.Unlock()
	return &engineChunkSeriesSet{series: selected, index: -1}
}

// StreamedSeries is a selected series as a query response sends it.
type StreamedSeries struct {
	stored labels.Labels
	// The chunks, in the encoding the store keeps them in. Their data stays valid until the querier is closed.
	Chunks []chunks.Chunk
}

// AppendLabels appends the series' labels to dst as the ingester sends them, pointing into the store's own: the
// ingester builds them a batch of series at a time, where adapters for all the series of a large response, many
// responses at once, were gigabytes.
func (s *StreamedSeries) AppendLabels(dst []mimirpb.LabelAdapter) []mimirpb.LabelAdapter {
	it := s.stored.Iter()
	for name, value, ok := it.Next(); ok; name, value, ok = it.Next() {
		dst = append(dst, mimirpb.LabelAdapter{Name: name, Value: value})
	}
	return dst
}

// SelectStreamed is Select for a response that streams the series: it returns them in label order with their labels as
// adapters and their chunks as the store has them, without a series set and chunk iterators in between. The slice is
// reused once the querier is closed.
func (q *engineChunkQuerier) SelectStreamed(hints *storage.SelectHints, matchers ...*promlabels.Matcher) ([]StreamedSeries, error) {
	start, end := q.mint, q.maxt
	if hints != nil {
		start, end = max(start, hints.Start), min(end, hints.End)
	}
	compiled, err := compilePromMatchers(hints, matchers)
	if err != nil {
		return nil, err
	}
	q.mu.Lock()
	room := q.takeRoom()
	q.mu.Unlock()
	selected, buffers, slabs, pooled, err := q.e.selectRaw(start, end, compiled, room)
	if err != nil {
		return nil, err
	}
	q.mu.Lock()
	q.buffers = append(q.buffers, buffers...)
	q.slabs = append(q.slabs, slabs...)
	if pooled {
		q.selections = append(q.selections, selected)
	}
	q.mu.Unlock()
	return selected, nil
}

// rawSeries is a selected series: its labels, and its chunks with their own copy of the data. It is the streamed
// series, which a streamed response is then without a copy of them all.
type rawSeries = StreamedSeries

// perShard is the store's perShardIndexed for the engine's tenant, which it finds by what it remembers of where it is
// rather than by hashing its id in every shard of every query.
func (e *Engine) perShard(query func(index int, t *tenant, disk *chunks.DiskMapper, cold *coldState)) {
	timer := startReadTimer(phaseSelect)
	for index, shard := range e.store.shards {
		shard.RLock()
		at := timer.held()
		if t, ok := e.tenantLocked(index); ok {
			query(index, t, shard.disk, shard.cold)
		}
		timer.release(at)
		shard.RUnlock()
	}
}

// selectRaw returns the series matching compiled with chunks in [start, end], sorted by labels,
// with the same chunks the store's QueryStream returns.
//
// The series and their chunk lists go in room first, when there is one, then in what is taken from the pools: pooled
// says the series are in a pooled slice, which the caller gives back.
func (e *Engine) selectRaw(start, end int64, compiled []compiledMatcher, room *selectRoom) (selected []rawSeries, buffers [][]byte, slabs [][]chunks.Chunk, pooled bool, err error) {
	var coldLabels, coldShard []compiledMatcher
	for _, matcher := range compiled {
		if matcher.kind == kindShard {
			coldShard = append(coldShard, matcher)
		} else {
			coldLabels = append(coldLabels, matcher)
		}
	}
	kept := e.store.retained.Load()
	// What a cold block, and the head's selector cache, remember the selector's candidates by.
	coldKey := selectorID(coldLabels)
	var (
		metas   []ChunkMeta
		scratch []chunks.Chunk
		data    []byte
		// The chunks' data buffers, which the caller gives back once it is done with the chunks.
		candidatesBuffer []chunks.Candidate
		// The series' chunk lists, cut from slabs that are given back with the buffers.
		slab       []chunks.Chunk
		slabPooled bool
	)
	if room != nil {
		selected, slab = room.series[:0], room.chunks[:0]
	} else {
		selected, pooled = takeSelection(), true
	}
	add := func(series rawSeries) {
		if !pooled && len(selected) == cap(selected) {
			selected, pooled = append(takeSelection(), selected...), true
		}
		selected = append(selected, series)
	}
	cut := func(raw []chunks.Chunk) []chunks.Chunk {
		if cap(slab)-len(slab) < len(raw) {
			slab, slabPooled = takeChunkSlab(len(raw)), true
			slabs = append(slabs, slab)
		}
		out := append(slab[len(slab):len(slab):len(slab)+len(raw)], raw...)
		slab = slab[:len(slab)+len(raw)]
		if slabPooled {
			// What the slab holds is what is cleared when it is given back.
			slabs[len(slabs)-1] = slab
		}
		return out
	}
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
			return cut(raw)
		}
		if cap(data)-len(data) < size {
			// Doubling from small, so a query of a few series doesn't pay for a large buffer.
			data = takeQueryBuffer(max(size, min(2*cap(data), 1<<20), 4<<10))
			buffers = append(buffers, data)
		}
		out := cut(raw)
		for index := range raw {
			if disk.Holds(raw[index].Data) {
				continue
			}
			offset := len(data)
			data = append(data, raw[index].Data...)
			out[index].Data = data[offset:len(data):len(data)]
		}
		return out
	}
	var headMetas, compacted, blockMetas []ChunkMeta
	// In-order chunks ending before the head's min time are a block's in Prometheus, which doesn't merge them with the
	// out-of-order head: a query's head spans the hours its samples were backdated over, and had every chunk of a series
	// decoded and re-encoded.
	headMin, _, _ := e.headTimes()
	// A selector with a metric name that comes in again finds its series in the shard's candidates.
	var (
		cached     *cachedSelector
		cachedName string
		cachedRest []compiledMatcher
		general    *cachedSelector
	)
	if name, rest, ok := cacheableName(coldLabels); ok {
		// By the query shard too, when there is one: what is kept is the shard's series.
		key, selector := coldKey, coldLabels
		if len(coldShard) > 0 {
			key, selector = selectorID(compiled), compiled
		}
		cached, cachedName, cachedRest = e.selectors.get(key, selector, len(e.store.shards)), name, rest
	} else if len(coldLabels) > 0 {
		// Selectors with no metric name equality are kept apart from the others: what is kept is by series id.
		general = e.selectors.get(coldKey^generalSelector, coldLabels, len(e.store.shards))
	}
	// The selector's parts, which the cold blocks keep their candidates by, as the selectors' cache has them: shared by every
	// query of the selector, which then tell the blocks' kept selector apart from another without comparing them.
	var coldParts []selectorPart
	switch {
	case cached != nil && len(coldShard) == 0:
		coldParts = cached.parts
	case cached != nil:
		coldParts = cached.unshardedParts(coldLabels)
	case general != nil:
		coldParts = general.parts
	}
	coldSelected := takeColdSelection()
	defer coldSelected.give()
	e.perShardOfName(cachedName, cached != nil, func(shard int, t *tenant, disk *chunks.DiskMapper, cold *coldState) {
		watermark := e.oooWatermarks[shard]
		coldSelected.reset()
		e.store.selectCold(e.tenantID, cold, compiled, coldLabels, coldShard, coldKey, coldParts, start, end, kept, shard, coldSelected)
		// A series' cold chunks are told apart like its chunks in memory: Prometheus's head keeps an idle series that the
		// engine evicts, with its in-order chunks until the head is truncated past them and its out-of-order ones until
		// the out-of-order head is compacted. Merged whatever they were, every query of a series that left decoded and
		// re-encoded its chunks.
		seriesChunks := func(series *Series, cold, metas []ChunkMeta) []chunks.Chunk {
			headMetas, compacted, blockMetas = headMetas[:0], compacted[:0], blockMetas[:0]
			for _, list := range [2][]ChunkMeta{cold, metas} {
				for _, meta := range list {
					switch {
					// Compacted out-of-order chunks are in blocks: Prometheus doesn't merge them with the head's chunks,
					// and returns them after, so the head's copy of a conflicting duplicate comes first.
					case meta.OutOfOrder && uint64(meta.Ref) < watermark:
						compacted = append(compacted, meta)
					case !meta.OutOfOrder && meta.MaxTime < headMin:
						blockMetas = append(blockMetas, meta)
					default:
						headMetas = append(headMetas, meta)
					}
				}
			}
			scratch = scratch[:0]
			if len(blockMetas) > 0 {
				// Blocks' chunks come before the head's.
				scratch = queryRawChunks(nil, blockMetas, nil, disk, start, end, scratch, &candidatesBuffer)
			}
			scratch = queryRawChunks(series, headMetas, nil, disk, start, end, scratch, &candidatesBuffer)
			for _, meta := range compacted {
				if meta.MinTime <= end && meta.MaxTime >= start {
					scratch = append(scratch, chunks.Chunk{MinTime: meta.MinTime, MaxTime: meta.MaxTime, Encoding: int32(meta.Encoding), Data: disk.Read(meta.Ref, meta.Len)})
				}
			}
			return scratch
		}
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
			coldChunks, hasCold := coldSelected.take(entry.labels)
			if !overlaps && !hasCold {
				return true
			}
			scratch = seriesChunks(series, coldChunks, metas)
			if len(scratch) > 0 {
				add(rawSeries{entry.labels, copyOut(scratch, disk)})
			}
			return true
		})
		for index := range coldSelected.candidates {
			candidate := &coldSelected.candidates[index]
			if candidate.taken {
				continue
			}
			scratch = seriesChunks(nil, candidate.chunks, nil)
			if len(scratch) > 0 {
				add(rawSeries{candidate.labels, copyOut(scratch, disk)})
			}
		}
	})
	sortByLabels(selected)
	return selected, buffers, slabs, pooled, nil
}

// sortByLabels sorts series in Prometheus's label order. A query's series are mostly one metric's, sharing most of
// their labels, which comparing them decoded on every comparison: each is decoded once instead, into a key that
// compares as bytes in the same order (labels.AppendSortKey). A label with a zero byte would compare otherwise, so then
// the series are compared as they are. The keys' memory is kept between sorts.
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
		size += len(selected[index].stored)
	}
	// At most 3 more bytes a label than its encoding's 2, which every label has at least.
	size += size * 3 / 2
	scratch, _ := sortScratch.Get().(*labelSortScratch)
	if scratch == nil {
		scratch = &labelSortScratch{}
	}
	defer func() {
		// Large sorts' memory isn't kept for the small ones.
		if cap(scratch.buffer) <= 64<<20 {
			clear(scratch.series)
			scratch.buffer, scratch.keys, scratch.series = scratch.buffer[:0], scratch.keys[:0], scratch.series[:0]
			sortScratch.Put(scratch)
		}
	}()
	if cap(scratch.buffer) < size {
		// Doubling: the sorts of a scratch are of all sizes, and one made just large enough was made again for most.
		scratch.buffer = make([]byte, 0, max(size, 2*cap(scratch.buffer)))
	}
	buffer := scratch.buffer
	keys := slices.Grow(scratch.keys, len(selected))[:len(selected)]
	scratch.keys = keys
	ranks := names.Ranks()
	for index := range selected {
		start := len(buffer)
		var ok bool
		if buffer, ok = selected[index].stored.AppendSortKey(buffer, ranks); !ok {
			slices.SortFunc(selected, compare)
			return
		}
		keys[index] = sortKey{start: uint32(start), end: uint32(len(buffer)), index: uint32(index)}
	}
	// The buffer holds every key, so it is never reallocated under the keys read from it.
	sortKeys(unsafe.String(unsafe.SliceData(buffer), len(buffer)), keys, 0)
	scratch.series = append(scratch.series[:0], selected...)
	for index := range keys {
		selected[index] = scratch.series[keys[index].index]
	}
}

// sortKeys sorts keys that share their first depth bytes, by the bytes from there: a three-way radix quicksort, which
// reads the long prefixes a query's keys share, the metric's name and its labels' names, once for each partition rather
// than once for each comparison. The keys are where they are in buffer and the series they are of: without a pointer,
// moving one has no write barrier.
func sortKeys(buffer string, keys []sortKey, depth int) {
	for len(keys) > 1 {
		if len(keys) < 16 {
			slices.SortFunc(keys, func(x, y sortKey) int { return strings.Compare(x.key(buffer)[depth:], y.key(buffer)[depth:]) })
			return
		}
		pivot := keyByte(keys[len(keys)/2].key(buffer), depth)
		// keys[:lt] are below the pivot byte, keys[lt:gt] equal to it and keys[gt:] above.
		lt, gt := 0, len(keys)
		for i := 0; i < gt; {
			switch c := keyByte(keys[i].key(buffer), depth); {
			case c < pivot:
				keys[lt], keys[i] = keys[i], keys[lt]
				lt++
				i++
			case c > pivot:
				gt--
				keys[gt], keys[i] = keys[i], keys[gt]
			default:
				i++
			}
		}
		sortKeys(buffer, keys[:lt], depth)
		sortKeys(buffer, keys[gt:], depth)
		if pivot < 0 {
			// The keys that ended are equal.
			return
		}
		all := lt == 0 && gt == len(keys)
		keys, depth = keys[lt:gt], depth+1
		if all {
			// All of them share the byte: they share as much as they share with the first, found in one pass.
			depth = commonPrefix(buffer, keys, depth)
		}
	}
}

// commonPrefix returns how many bytes the keys, which share their first depth, share.
func commonPrefix(buffer string, keys []sortKey, depth int) int {
	first := keys[0].key(buffer)
	common := len(first)
	for _, k := range keys[1:] {
		key := k.key(buffer)
		n := min(common, len(key))
		i := depth
		for i+8 <= n && first[i:i+8] == key[i:i+8] {
			i += 8
		}
		for i < n && first[i] == key[i] {
			i++
		}
		if common = i; common == depth {
			return depth
		}
	}
	return common
}

// keyByte is the key's byte at depth, or -1 past its end, which sorts before every byte.
func keyByte(key string, depth int) int {
	if depth < len(key) {
		return int(key[depth])
	}
	return -1
}

// sortKey is a series' key, buffer[start:end], and where the series is.
type sortKey struct {
	start, end, index uint32
}

func (k sortKey) key(buffer string) string { return buffer[k.start:k.end] }

type labelSortScratch struct {
	buffer []byte
	keys   []sortKey
	series []rawSeries
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

// The slabs a query's series' chunk lists are cut from are kept between queries, like the data buffers: a list for each
// series was a few percent of what an ingester allocated.
const chunkSlab = 4096

var chunkSlabs sync.Pool

// takeChunkSlab returns an empty slab that holds n chunks, which giveChunkSlabs may get back.
func takeChunkSlab(n int) []chunks.Chunk {
	if n <= chunkSlab {
		if slab, ok := chunkSlabs.Get().(*[]chunks.Chunk); ok {
			return (*slab)[:0]
		}
		n = chunkSlab
	}
	return make([]chunks.Chunk, 0, n)
}

// giveChunkSlabs keeps the slabs of a query that is done with its chunks, cleared: they point into the buffers, which
// another tenant's query takes. Only what a slab holds is: a kept slab is empty past it.
func giveChunkSlabs(slabs [][]chunks.Chunk) {
	for _, slab := range slabs {
		if cap(slab) == chunkSlab {
			clear(slab)
			slab = slab[:0]
			chunkSlabs.Put(&slab)
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
	s.current = engineChunkSeries{stored: raw.stored, chunks: raw.Chunks, builder: &s.builder, slab: &s.slab}
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
	limit := 0
	// The smallest values need them all to be found.
	if hints != nil && !hints.LimitSmallest {
		limit = hints.Limit
	}
	values, err := q.e.store.LabelValuesLimited(q.e.tenantID, name, q.mint, q.maxt, storeMatchers(matchers), limit)
	if err != nil {
		return nil, nil, err
	}
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
	compiled, err := compilePromMatchers(nil, matchers)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}
	selected := q.e.selectLabels(start, end, compiled)
	defer giveSelection(selected)
	if hints != nil && hints.Limit > 0 && len(selected) > hints.Limit {
		selected = selected[:hints.Limit]
	}
	series := make([]labelsOnlySeries, len(selected))
	var builder promlabels.ScratchBuilder
	for index := range selected {
		series[index] = labelsOnlySeries{toPromLabels(selected[index].stored, &builder)}
	}
	return &sliceSeriesSet{series: series, index: -1}
}

// selectLabels returns the series matching compiled with samples in [start, end], sorted by labels, without their
// chunks. A series both in memory and in a cold block is in them once: sorted, its copies are next to each other.
func (e *Engine) selectLabels(start, end int64, compiled []compiledMatcher) []rawSeries {
	selected := takeSelection()
	memoryIsHead := e.store.memoryIsHead
	e.perShard(func(_ int, t *tenant, _ *chunks.DiskMapper, cold *coldState) {
		t.series.matching(compiled, func(entry *seriesEntry) bool {
			if memoryIsHead && entry.series.hasChunkIn(start, end) || !memoryIsHead && entry.series.hasDataIn(start, end) {
				selected = append(selected, rawSeries{stored: entry.labels})
			}
			return true
		})
		cold.matching(e.tenantID, compiled, start, end, func(series *coldSeries) {
			selected = append(selected, rawSeries{stored: series.labels()})
		})
	})
	sortByLabels(selected)
	return slices.CompactFunc(selected, func(x, y rawSeries) bool { return x.stored == y.stored })
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

// The slices lookups of series select into are kept between them: growing one was most of what a lookup allocated.
var selections sync.Pool

func takeSelection() []rawSeries {
	if selected, ok := selections.Get().(*[]rawSeries); ok {
		return (*selected)[:0]
	}
	return nil
}

// giveSelection keeps selected for another lookup, unless it is large. Only what it holds is cleared: past it, a slice
// that was only appended to and compacted is empty, and clearing a large one's capacity after each small lookup took
// longer than the lookup.
func giveSelection(selected []rawSeries) {
	if cap(selected) > 1<<16 {
		return
	}
	clear(selected)
	selected = selected[:0]
	selections.Put(&selected)
}

// sliceSeriesSet holds the series themselves, as boxing each into a storage.Series was an allocation for every one.
type sliceSeriesSet struct {
	series []labelsOnlySeries
	index  int
}

func (s *sliceSeriesSet) Next() bool {
	s.index++
	return s.index < len(s.series)
}
func (s *sliceSeriesSet) At() storage.Series                { return &s.series[s.index] }
func (s *sliceSeriesSet) Err() error                        { return nil }
func (s *sliceSeriesSet) Warnings() annotations.Annotations { return nil }

// ExemplarQuerier returns a querier of the tenant's exemplars.
func (e *Engine) ExemplarQuerier(context.Context) (storage.ExemplarQuerier, error) {
	return engineExemplarQuerier{e}, nil
}

type engineExemplarQuerier struct{ e *Engine }

// Select returns the exemplars in [start, end] of the series matching any of the matcher sets.
func (q engineExemplarQuerier) Select(start, end int64, matcherSets ...[]*promlabels.Matcher) ([]exemplar.QueryResult, error) {
	compiled := make([][]idMatcher, 0, len(matcherSets))
	for _, set := range matcherSets {
		c, err := compilePromMatchers(nil, set)
		if err != nil {
			return nil, err
		}
		compiled = append(compiled, byNameIDs(c))
	}
	q.e.store.exemplarsLock.Lock()
	tenantExemplars, ok := q.e.store.exemplars[q.e.tenantID]
	var selected []exemplars.Selected[labels.Labels]
	if ok {
		selected = tenantExemplars.Select(start, end, func(stored *labels.Labels) bool {
			for _, set := range compiled {
				if matchesByID(*stored, set) {
					return true
				}
			}
			return false
		})
	}
	q.e.store.exemplarsLock.Unlock()
	results := make([]exemplar.QueryResult, 0, len(selected))
	var builder promlabels.ScratchBuilder
	for _, series := range selected {
		result := exemplar.QueryResult{SeriesLabels: toPromLabels(series.Labels, &builder), Exemplars: make([]exemplar.Exemplar, 0, len(series.Exemplars))}
		for _, stored := range series.Exemplars {
			builder.Reset()
			for _, label := range stored.Labels {
				builder.Add(label.Name, label.Value)
			}
			result.Exemplars = append(result.Exemplars, exemplar.Exemplar{Labels: builder.Labels(), Value: stored.Value, Ts: stored.TimestampMs, HasTs: true})
		}
		results = append(results, result)
	}
	// By the series' labels as Prometheus's: they compare as strings, where the store's compared decoded.
	slices.SortFunc(results, func(a, b exemplar.QueryResult) int { return promlabels.Compare(a.SeriesLabels, b.SeriesLabels) })
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
	// The workers of a request's lookups of each value share the cache, read-locked once it is built.
	mu    sync.RWMutex
	asked map[string]int
	// The labels of mostly distinct values, whose postings aren't cached: a value's are a series or two, which the
	// store's postings find, and caching them was a map of every series of the label.
	uncached map[string]bool
	byValue  map[string]map[string][]storage.SeriesRef
	// The series of each metric name asked for: a metric's are its name group's, which are listed without reading the
	// labels of the others, and the label APIs ask for a metric's with each value of another label.
	byMetric map[string][]storage.SeriesRef
	// The values of every label of the series matching a selector, built once the reader is asked for the names or values
	// of its series a second time: the label names and values API asks for the names and then for the values of each, which
	// was a pass over the selector's series for each name.
	selectorAsked  map[string]int
	selectorValues map[string]map[string]map[string]struct{}
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
// A label most series have is looked for in every series: its postings list them in no order, and sorting them to visit
// each once cost more than reading the labels of the few without it.
func (ix *engineIndex) forEachWith(name string, visit func(entry *seriesEntry, value string)) {
	id, known := labels.Lookup(name)
	if name != metricNameLabel && !known {
		return
	}
	for _, shard := range ix.e.store.shards {
		shard.RLock()
		if t, ok := shard.tenants[ix.e.tenantID]; ok {
			switch {
			case name == metricNameLabel:
				// A name group's series all have its name.
				for groupID := range t.series.groups {
					value, entries := t.series.groupNames[groupID], t.series.groups[groupID].entries
					for index := range entries {
						visit(&entries[index], value)
					}
				}
			case 2*t.series.seriesWith(id, known) >= t.series.len:
				t.series.forEach(func(entry *seriesEntry) {
					if value, ok := labelValue(entry.labels, id); ok {
						visit(entry, value)
					}
				})
			default:
				t.series.visitRefs(t.series.refsWith(id, known), func(entry *seriesEntry) bool {
					if value, ok := labelValue(entry.labels, id); ok {
						visit(entry, value)
					}
					return true
				})
			}
		}
		shard.RUnlock()
	}
}

// forEachIdleEvicted calls visit with the cold series matching compiled that the engine evicted for being idle and that
// Prometheus's head still holds, as it only collects a series once a head compaction leaves it without samples since the
// head's min valid time: a shard at a time, with the shard read-locked, as what visit gets is in the shard's blocks.
func (ix *engineIndex) forEachIdleEvicted(compiled []compiledMatcher, visit func(series *coldSeries)) {
	ix.forEachIdleEvictedIn(compiled, func(series *coldSeries, _ *tenant, _ uint64) { visit(series) })
}

// forEachIdleEvictedIn is forEachIdleEvicted, also giving visit the tenant of the series' shard and its labels' hash.
func (ix *engineIndex) forEachIdleEvictedIn(compiled []compiledMatcher, visit func(series *coldSeries, t *tenant, hash uint64)) {
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
		if t, ok := shard.tenants[ix.e.tenantID]; ok {
			coldView(shard.cold.blocks).matchingIndexed(ix.e.tenantID, compiled, headMin, math.MaxInt64, func(table *coldTenantIndex, index int, series *coldSeries) {
				hash := series.block.labelsHash(table, index)
				for i := range idle {
					if idle[i].holds(hash) {
						visit(series, t, hash)
						return
					}
				}
			})
		}
		shard.RUnlock()
	}
}

func postingsOf(refs []storage.SeriesRef) index.Postings {
	sortSeriesRefs(refs)
	return index.NewListPostings(slices.Compact(refs))
}

// sortSeriesRefs sorts refs, a byte of them at a time from the lowest for many of them, skipping the bytes they all have
// the same: refs are a tenant's count of series with the shard below, in a few bytes, which postings list in no order.
func sortSeriesRefs(refs []storage.SeriesRef) {
	if len(refs) < 256 {
		slices.Sort(refs)
		return
	}
	var differ storage.SeriesRef
	for _, ref := range refs[1:] {
		differ |= ref ^ refs[0]
	}
	scratch := make([]storage.SeriesRef, len(refs))
	from, to := refs, scratch
	for shift := uint(0); shift < 64; shift += 8 {
		if (differ>>shift)&0xff == 0 {
			continue
		}
		var offsets [256]int
		for _, ref := range from {
			offsets[(ref>>shift)&0xff]++
		}
		position := 0
		for digit, count := range offsets {
			offsets[digit] = position
			position += count
		}
		for _, ref := range from {
			digit := (ref >> shift) & 0xff
			to[offsets[digit]] = ref
			offsets[digit]++
		}
		from, to = to, from
	}
	if &from[0] != &refs[0] {
		copy(refs, from)
	}
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
	compiled, err := compilePromMatchers(nil, matchers)
	if err != nil {
		return nil, err
	}
	if values, ok := ix.selectorLabelValues(compiled, matchers); ok {
		out := sortedKeys(values[name])
		if hints != nil {
			out = limited(out, hints.Limit)
		}
		return out, nil
	}
	set := map[string]struct{}{}
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
	ix.askedForSelector(matchers)
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
	if name == metricNameLabel && len(values) == 1 {
		// Sorted and unique already, and shared with the reader's other lookups of the metric.
		return index.NewListPostings(ix.metricPostings(values[0])), nil
	}
	if len(values) == 1 {
		if refs, ok := ix.cachedPostings(name, values[0]); ok {
			// Sorted and unique already, and shared with the reader's other lookups of the value.
			return index.NewListPostings(refs), nil
		}
	}
	if len(values) == 1 && name != metricNameLabel {
		return ix.postingsOfValue(name, values[0]), nil
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

// promSelectorKey identifies matchers among the selectors a reader is asked about.
func promSelectorKey(matchers []*promlabels.Matcher) string {
	var key strings.Builder
	for _, m := range matchers {
		key.WriteString(m.Name)
		key.WriteByte(0xff)
		key.WriteByte(byte(m.Type))
		key.WriteString(m.Value)
		key.WriteByte(0xff)
	}
	return key.String()
}

// askedForSelector counts a lookup of the names or values of the selector's series, and returns its key.
func (ix *engineIndex) askedForSelector(matchers []*promlabels.Matcher) (string, int) {
	if len(matchers) == 0 {
		return "", 0
	}
	key := promSelectorKey(matchers)
	ix.mu.Lock()
	defer ix.mu.Unlock()
	if ix.selectorAsked == nil {
		ix.selectorAsked = map[string]int{}
	}
	ix.selectorAsked[key]++
	return key, ix.selectorAsked[key]
}

// selectorLabelValues returns the values of each label of the series matching the selector, once the reader was asked
// about them before: one pass over the series answers the reader's lookups of every name.
func (ix *engineIndex) selectorLabelValues(compiled []compiledMatcher, matchers []*promlabels.Matcher) (map[string]map[string]struct{}, bool) {
	if len(matchers) == 0 {
		return nil, false
	}
	key := promSelectorKey(matchers)
	ix.mu.RLock()
	values, ok := ix.selectorValues[key]
	ix.mu.RUnlock()
	if ok {
		return values, true
	}
	if _, asked := ix.askedForSelector(matchers); asked < 2 {
		return nil, false
	}
	values = map[string]map[string]struct{}{}
	add := func(name, value string) {
		set, ok := values[name]
		if !ok {
			set = map[string]struct{}{}
			values[name] = set
		}
		set[value] = struct{}{}
	}
	// The values point into the series' labels, which are their own allocations: they stay valid for the reader.
	ix.forEachMatching(compiled, func(entry *seriesEntry) { entry.labels.Range(add) })
	// A cold series' labels are decoded into an allocation of their own, out of the block's mapping.
	ix.forEachIdleEvicted(compiled, func(series *coldSeries) { series.labels().Range(add) })
	ix.mu.Lock()
	defer ix.mu.Unlock()
	if ix.selectorValues == nil {
		ix.selectorValues = map[string]map[string]map[string]struct{}{}
	}
	ix.selectorValues[key] = values
	return values, true
}

// minCachedSeriesPerValue is how many series a label's values have on average for a reader to cache their postings.
const minCachedSeriesPerValue = 4

// labelCardinality returns how many head series have the label, and about how many distinct values: the store's
// postings are by a hash of the value, which collide rarely.
func (ix *engineIndex) labelCardinality(name string) (series, distinct int) {
	if name == metricNameLabel {
		return math.MaxInt32, 1
	}
	id, known := labels.Lookup(name)
	if !known {
		return 0, 0
	}
	for _, shard := range ix.e.store.shards {
		shard.RLock()
		if t, ok := shard.tenants[ix.e.tenantID]; ok {
			series += t.series.seriesWith(id, known)
			if int(id) < len(t.series.postings) {
				distinct += len(t.series.postings[id])
			}
		}
		shard.RUnlock()
	}
	return series, distinct
}

// metricPostings returns the series of the metric name, sorted.
func (ix *engineIndex) metricPostings(name string) []storage.SeriesRef {
	ix.mu.RLock()
	refs, ok := ix.byMetric[name]
	ix.mu.RUnlock()
	if ok {
		return refs
	}
	for _, shard := range ix.e.store.shards {
		shard.RLock()
		if t, ok := shard.tenants[ix.e.tenantID]; ok {
			if groupID, ok := t.series.names[name]; ok {
				for _, entry := range t.series.groups[groupID].entries {
					refs = append(refs, storage.SeriesRef(entry.series.ref))
				}
			}
		}
		shard.RUnlock()
	}
	sortSeriesRefs(refs)
	ix.mu.Lock()
	defer ix.mu.Unlock()
	if ix.byMetric == nil {
		ix.byMetric = map[string][]storage.SeriesRef{}
	}
	ix.byMetric[name] = refs
	return refs
}

// LabelValuesCounts returns how many head series matching matchers have each value of each of the labels, counting only
// the ones ingested since cutoffMs, math.MinInt64 for all of them: what the cardinality API counts with the postings of
// each value and the matchers', from one pass over the matchers' series.
func (ix *engineIndex) LabelValuesCounts(ctx context.Context, names []string, matchers []*promlabels.Matcher, cutoffMs int64) (map[string]map[string]uint64, error) {
	compiled, err := compilePromMatchers(nil, matchers)
	if err != nil {
		return nil, err
	}
	type counted struct {
		id     uint32
		counts map[string]uint64
	}
	out := make(map[string]map[string]uint64, len(names))
	var wanted []counted
	for _, name := range names {
		counts := map[string]uint64{}
		out[name] = counts
		if id, known := labels.Lookup(name); known {
			wanted = append(wanted, counted{id: id, counts: counts})
		}
	}
	if len(wanted) == 0 {
		return out, nil
	}
	visited := 0
	ix.forEachMatching(compiled, func(entry *seriesEntry) {
		if visited++; visited%checkContextEvery == 0 && ctx.Err() != nil {
			return
		}
		if cutoffMs != math.MinInt64 && !entry.series.isActive(cutoffMs) {
			return
		}
		for _, w := range wanted {
			if value, ok := labelValue(entry.labels, w.id); ok {
				// The values point into the series' labels, which are their own allocations: they stay valid for the
				// reader.
				w.counts[value]++
			}
		}
	})
	if cutoffMs == math.MinInt64 {
		// Prometheus's head still counts the series the engine evicted for being idle, once: those that took samples
		// again are in memory too.
		ix.forEachIdleEvictedIn(compiled, func(series *coldSeries, t *tenant, hash uint64) {
			if t.series.hasHash(series.Get(metricNameLabel), hash) {
				return
			}
			for _, w := range wanted {
				if value, ok := series.lookup(labels.Name(w.id)); ok {
					if _, seen := w.counts[value]; !seen {
						// The value is in the block's mapping, which a compaction may unmap once the shard is unlocked.
						value = strings.Clone(value)
					}
					w.counts[value]++
				}
			}
		})
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// checkContextEvery is how many series a pass visits between checks of its context.
const checkContextEvery = 1 << 14

// ActiveSeriesMatching calls visit with the labels of each head series matching matchers that was ingested since
// cutoffMs, as the active series API lists them from the matchers' postings and the active series, and with its native
// histogram bucket count: with nativeHistograms, only the series whose last samples were native histograms. The labels
// point into the series' own, which are their own allocations, from a slice that is reused once visit returns; visit
// isn't called with a shard locked.
func (ix *engineIndex) ActiveSeriesMatching(ctx context.Context, matchers []*promlabels.Matcher, cutoffMs int64, nativeHistograms bool, visit func(lbls []mimirpb.LabelAdapter, buckets int) error) error {
	compiled, err := compilePromMatchers(nil, matchers)
	if err != nil {
		return err
	}
	scratch := takeActiveScratch()
	defer scratch.give()
	ix.forEachMatching(compiled, func(entry *seriesEntry) {
		series := &entry.series
		if !series.isActive(cutoffMs) {
			return
		}
		buckets := 0
		if nativeHistograms {
			if !series.nativeHistogram {
				return
			}
			buckets = int(series.lastBucketCount)
		}
		scratch.selected = append(scratch.selected, activeSeries{entry.labels, buckets})
	})
	for index, series := range scratch.selected {
		if index%checkContextEvery == 0 && ctx.Err() != nil {
			return ctx.Err()
		}
		scratch.adapters = scratch.adapters[:0]
		series.labels.Range(func(name, value string) {
			scratch.adapters = append(scratch.adapters, mimirpb.LabelAdapter{Name: name, Value: value})
		})
		if err := visit(scratch.adapters, series.buckets); err != nil {
			return err
		}
	}
	return nil
}

type activeSeries struct {
	labels  labels.Labels
	buckets int
}

// activeScratch holds the slices an active series listing selects and lists the series in, kept between listings:
// growing them was most of what one allocated.
type activeScratch struct {
	selected []activeSeries
	adapters []mimirpb.LabelAdapter
}

var activeScratches sync.Pool

func takeActiveScratch() *activeScratch {
	if scratch, ok := activeScratches.Get().(*activeScratch); ok {
		return scratch
	}
	return &activeScratch{}
}

// give keeps the slices for another listing, unless they are large, cleared: they point into the labels of one
// tenant's series.
func (s *activeScratch) give() {
	if cap(s.selected) > 1<<16 {
		return
	}
	clear(s.selected)
	// Longer series' labels were written past its length: it holds one series' labels, which is short.
	clear(s.adapters[:cap(s.adapters)])
	s.selected, s.adapters = s.selected[:0], s.adapters[:0]
	activeScratches.Put(s)
}

// IsLabelValueActive reports whether a head series with the label's value was ingested since cutoffMs, as the ingester's
// active series check of the value's postings does, stopping at the first.
func (ix *engineIndex) IsLabelValueActive(_ context.Context, name, value string, cutoffMs int64) (bool, error) {
	active := false
	if name == metricNameLabel {
		for index, shard := range ix.e.store.shards {
			shard.RLock()
			if t, ok := ix.e.tenantLocked(index); ok {
				if groupID, ok := t.series.names[value]; ok {
					entries := t.series.groups[groupID].entries
					for i := range entries {
						if entries[i].series.isActive(cutoffMs) {
							active = true
							break
						}
					}
				}
			}
			shard.RUnlock()
			if active {
				return true, nil
			}
		}
		return false, nil
	}
	id, known := labels.Lookup(name)
	if !known {
		return false, nil
	}
	key := postingKey(value)
	var one [1]seriesRef
	// A posting holds a value's hash: collisions are checked against the labels.
	visit := func(entry *seriesEntry) bool {
		if entry.series.isActive(cutoffMs) {
			if v, ok := labelValue(entry.labels, id); ok && v == value {
				active = true
				return false
			}
		}
		return true
	}
	for index, shard := range ix.e.store.shards {
		shard.RLock()
		if t, ok := ix.e.tenantLocked(index); ok && int(id) < len(t.series.postings) {
			if posting, ok := t.series.postings[id][key]; ok {
				if single, many := t.series.posting(posting); many == nil {
					one[0] = t.series.refs[single]
					t.series.visitRefs(one[:], visit)
				} else {
					for _, seriesID := range many {
						one[0] = t.series.refs[seriesID]
						if t.series.visitRefs(one[:], visit); active {
							break
						}
					}
				}
			}
		}
		shard.RUnlock()
		if active {
			return true, nil
		}
	}
	return false, nil
}

// postingsOfValue is Postings of one value from the store's postings, which lists in each shard the series of the
// value's hash.
func (ix *engineIndex) postingsOfValue(name, value string) index.Postings {
	id, known := labels.Lookup(name)
	if !known {
		return index.EmptyPostings()
	}
	key := postingKey(value)
	var refs []storage.SeriesRef
	// A posting holds a value's hash: collisions are checked against the labels.
	visit := func(entry *seriesEntry) bool {
		if v, ok := labelValue(entry.labels, id); ok && v == value {
			refs = append(refs, storage.SeriesRef(entry.series.ref))
		}
		return true
	}
	var one [1]seriesRef
	for index, shard := range ix.e.store.shards {
		shard.RLock()
		if t, ok := ix.e.tenantLocked(index); ok && int(id) < len(t.series.postings) {
			if posting, ok := t.series.postings[id][key]; ok {
				if single, many := t.series.posting(posting); many == nil {
					one[0] = t.series.refs[single]
					t.series.visitRefs(one[:], visit)
				} else {
					for _, seriesID := range many {
						one[0] = t.series.refs[seriesID]
						t.series.visitRefs(one[:], visit)
					}
				}
			}
		}
		shard.RUnlock()
	}
	if len(refs) == 0 {
		return index.EmptyPostings()
	}
	return postingsOf(refs)
}

// cachedPostings returns the series with the label's value from the postings of all its values, once the reader asked
// for another of its values.
func (ix *engineIndex) cachedPostings(name, value string) ([]storage.SeriesRef, bool) {
	ix.mu.RLock()
	values, ok := ix.byValue[name]
	ix.mu.RUnlock()
	if ok {
		return values[value], true
	}
	ix.mu.Lock()
	defer ix.mu.Unlock()
	values, ok = ix.byValue[name]
	if !ok {
		if ix.uncached[name] {
			return nil, false
		}
		if ix.asked == nil {
			ix.asked = map[string]int{}
		}
		if ix.asked[name]++; ix.asked[name] < 2 {
			return nil, false
		}
		if series, distinct := ix.labelCardinality(name); series < minCachedSeriesPerValue*distinct {
			if ix.uncached == nil {
				ix.uncached = map[string]bool{}
			}
			ix.uncached[name] = true
			return nil, false
		}
		values = map[string][]storage.SeriesRef{}
		// The values point into the series' labels, which are their own allocations: they stay valid for the reader.
		ix.forEachWith(name, func(entry *seriesEntry, v string) {
			values[v] = append(values[v], storage.SeriesRef(entry.series.ref))
		})
		for _, list := range values {
			sortSeriesRefs(list)
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

// idMatcher is a matcher with its label name's id, which the scan of many series' labels reads the value of without
// looking the name up in each.
type idMatcher struct {
	compiledMatcher
	id    uint32
	known bool
}

func byNameIDs(compiled []compiledMatcher) []idMatcher {
	out := make([]idMatcher, len(compiled))
	for index := range compiled {
		out[index].compiledMatcher = compiled[index]
		out[index].id, out[index].known = labels.Lookup(compiled[index].name)
	}
	return out
}

// matchesByID is matches, with the matchers' label name ids.
func matchesByID(stored labels.Labels, matchers []idMatcher) bool {
	for index := range matchers {
		matcher := &matchers[index]
		if matcher.kind == kindShard {
			if labels.StableHash(stored)%matcher.shardCount != matcher.shardIndex {
				return false
			}
			continue
		}
		value := ""
		if matcher.known {
			value, _ = stored.Lookup(matcher.id)
		}
		if !matcher.matchesValue(value) {
			return false
		}
	}
	return true
}
