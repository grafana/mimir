// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"encoding/binary"
	"math"
	"slices"
	"sort"
	"time"

	"google.golang.org/protobuf/encoding/protowire"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/seriesstore/chunks"
	"github.com/grafana/mimir/pkg/storage/seriesstore/exemplars"
	"github.com/grafana/mimir/pkg/storage/seriesstore/labels"
)

// QuerySeriesView is a selected series: its `QueryStreamSeries` labels, encoded, and its chunks.
type QuerySeriesView struct {
	EncodedLabels []byte
	Chunks        []EncodedChunk
}

// EncodedChunk is a chunk as a `cortex.Chunk` message.
type EncodedChunk struct {
	StartTimestampMs int64
	EndTimestampMs   int64
	// XOR and histogram chunks start with a big-endian sample count.
	Samples uint32
	Wire    []byte
}

// SeriesView is a series' labels and exemplars.
type SeriesView struct {
	Labels    [][2]string
	Exemplars []exemplars.Exemplar
}

// ActiveSeriesView is an active series with its native histogram buckets.
type ActiveSeriesView struct {
	Labels      [][2]string
	BucketCount uint64
}

// UserStatsView is a tenant's series and ingestion rates.
type UserStatsView struct {
	NumSeries         uint64
	IngestionRate     float64
	APIIngestionRate  float64
	RuleIngestionRate float64
}

// arena hands out byte slices and chunk lists from shared blocks, so a query's many small encoded
// chunks and labels cost a few allocations instead of one each.
type arena struct {
	block  []byte
	chunks []EncodedChunk
}

// chunkSlice returns an empty chunk list with room for size chunks.
func (a *arena) chunkSlice(size int) []EncodedChunk {
	if size > 256 {
		return make([]EncodedChunk, 0, size)
	}
	if cap(a.chunks)-len(a.chunks) < size {
		a.chunks = make([]EncodedChunk, 0, min(max(2*cap(a.chunks), 64), 4096))
	}
	start := len(a.chunks)
	a.chunks = a.chunks[:start+size]
	return a.chunks[start : start : start+size]
}

// Blocks start small, since most queries return few series, and double up to the largest.
const (
	firstArenaBlock = 4 * 1024
	arenaBlock      = 64 * 1024
)

func (a *arena) alloc(size int) []byte {
	if size > arenaBlock/4 {
		return make([]byte, 0, size)
	}
	if cap(a.block)-len(a.block) < size {
		next := min(max(2*cap(a.block), firstArenaBlock), arenaBlock)
		for next < size {
			next *= 2
		}
		a.block = make([]byte, 0, next)
	}
	start := len(a.block)
	a.block = a.block[:start+size]
	return a.block[start : start : start+size]
}

// perShardWithCold runs query on the tenant's series and cold blocks in every shard, one after the
// other on the calling goroutine: like Go's head, a query runs on one thread and concurrent
// queries use the cores. Spreading each small query over the store's threads spent more CPU
// finding work than running it.
func (s *Store) perShardWithCold(tenantID string, query func(t *tenant, disk *chunks.DiskMapper, cold *coldState)) {
	for _, shard := range s.shards {
		shard.RLock()
		if t, ok := shard.tenants[tenantID]; ok {
			query(t, shard.disk, shard.cold)
		}
		shard.RUnlock()
	}
}

type selectedSeries struct {
	labels labels.Labels
	view   QuerySeriesView
}

type coldCandidateSeries struct {
	labels labels.Labels
	chunks []ChunkMeta
}

// SelectChunks returns the series a query selects, with their chunks in its range, sorted by
// labels: the distributor k-way merges each ingester's stream and requires label order, which the
// Go ingester gets from sorted postings.
func (s *Store) SelectChunks(tenantID string, start, end int64, matchers []LabelMatcher) ([]QuerySeriesView, error) {
	compiled, err := compileMatchers(matchers)
	if err != nil {
		return nil, err
	}
	var shardMatchers, indexMatchers []LabelMatcher
	for _, matcher := range matchers {
		if matcher.Type == MatchEqual && matcher.Name == shardLabel {
			shardMatchers = append(shardMatchers, matcher)
		} else {
			indexMatchers = append(indexMatchers, matcher)
		}
	}
	// Cold series are checked against the shard by each block's cached hashes, and against the
	// other matchers by their labels.
	coldLabels, err := compileMatchers(indexMatchers)
	if err != nil {
		return nil, err
	}
	coldShard, err := compileMatchers(shardMatchers)
	if err != nil {
		return nil, err
	}
	prunedBefore := s.prunedBefore.Load()
	var (
		selected []selectedSeries
		a        arena
		metas    []ChunkMeta
	)
	s.perShardWithCold(tenantID, func(t *tenant, disk *chunks.DiskMapper, cold *coldState) {
		// Cold series, by labels: one may be in several blocks, and also back in memory.
		coldSeries := s.selectCold(tenantID, cold, compiled, coldLabels, coldShard, start, end, prunedBefore)
		t.series.matching(compiled, func(entry *seriesEntry) bool {
			if countersEnabled {
				chunkBoundsDecodes.Add(1)
			}
			series := &entry.series
			// Decoded once: the chunks read for their bounds are the ones returned.
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
				if cold, ok := coldSeries[entry.labels]; ok {
					coldChunks, hasCold = cold.chunks, true
					delete(coldSeries, entry.labels)
				}
			}
			if !overlaps && !hasCold {
				return true
			}
			chunks := queryChunks(series, metas, coldChunks, disk, start, end, &a)
			if len(chunks) > 0 {
				selected = append(selected, selectedSeries{entry.labels, QuerySeriesView{EncodedLabels: encodeSeriesLabels(entry.labels, &a), Chunks: chunks}})
			}
			return true
		})
		for stored, cold := range coldSeries {
			chunks := queryChunks(nil, nil, cold.chunks, disk, start, end, &a)
			if len(chunks) > 0 {
				selected = append(selected, selectedSeries{stored, QuerySeriesView{EncodedLabels: encodeSeriesLabels(stored, &a), Chunks: chunks}})
			}
		}
	})
	names := labels.TakeSnapshot()
	slices.SortFunc(selected, func(x, y selectedSeries) int { return compareLabels(names, x.labels, y.labels) })
	views := make([]QuerySeriesView, len(selected))
	for index := range selected {
		views[index] = selected[index].view
	}
	return views, nil
}

// selectCold returns the tenant's cold series of one store shard that match, by labels, with
// their chunks newer than prunedBefore.
func (s *Store) selectCold(tenantID string, cold *coldState, compiled, coldLabels, coldShard []compiledMatcher, start, end, prunedBefore int64) map[labels.Labels]*coldCandidateSeries {
	if len(cold.blocks) == 0 {
		return nil
	}
	var result map[labels.Labels]*coldCandidateSeries
	// Like the head's, each regex matcher's result for the label values it saw.
	remembered := make([]map[string]bool, len(coldLabels))
	for _, block := range cold.blocks {
		table := block.tenant(tenantID)
		if table == nil || !block.overlaps(tenantID, start, end) {
			continue
		}
		// For each label matcher that accepts the empty value, the series with its label when few
		// are: the others match it without reading their labels.
		var sets [][]uint32
		setsComputed := false
		for _, index := range block.candidates(tenantID, compiled) {
			// A hash comparison, where the rest decodes the series.
			if len(coldShard) > 0 && !inQueryShard(block.shardHash(table, int(index)), coldShard) {
				continue
			}
			series := block.seriesIn(table, int(index))
			if !series.hasData(start, end) {
				continue
			}
			if !setsComputed {
				sets = make([][]uint32, len(coldLabels))
				for m := range coldLabels {
					matcher := &coldLabels[m]
					name, ok := matcher.labelName()
					if !ok || name == metricNameLabel || !matcher.matchesValue("") {
						continue
					}
					with := block.withLabel(table, name)
					if len(with)*4 < table.seriesCount {
						sets[m] = with
					}
				}
				setsComputed = true
			}
			matched := true
			for m := range coldLabels {
				matcher := &coldLabels[m]
				if with := sets[m]; with != nil {
					if _, found := slices.BinarySearch(with, index); !found {
						continue
					}
				}
				if matcher.kind != kindRegex && matcher.kind != kindNotRegex {
					if !matches(&series, coldLabels[m:m+1]) {
						matched = false
						break
					}
					continue
				}
				countLabelValueRead()
				value := series.Get(matcher.name)
				result, ok := remembered[m][value]
				if !ok {
					if countersEnabled {
						regexEvaluations.Add(1)
					}
					result = matcher.matchesValue(value)
					if remembered[m] == nil {
						remembered[m] = map[string]bool{}
					}
					if len(remembered[m]) < rememberedValues {
						remembered[m][value] = result
					}
				}
				if !result {
					matched = false
					break
				}
			}
			if !matched {
				continue
			}
			stored := series.labels()
			if result == nil {
				result = map[labels.Labels]*coldCandidateSeries{}
			}
			candidate, ok := result[stored]
			if !ok {
				candidate = &coldCandidateSeries{labels: stored}
				result[stored] = candidate
			}
			it := series.chunkIter()
			for chunk, more := it.next(); more; chunk, more = it.next() {
				if chunk.MaxTime >= prunedBefore {
					candidate.chunks = append(candidate.chunks, chunk)
				}
			}
		}
	}
	return result
}

// queryChunks returns the series' chunks in [start, end]: completed ones from chunks (its chunk
// list, decoded) and coldChunks, and its open ones. With out-of-order samples, overlapping chunks
// are merged as the Go ingester's out-of-order querier does; otherwise every chunk is returned
// as stored.
func queryChunks(series *Series, stored, coldChunks []ChunkMeta, disk *chunks.DiskMapper, start, end int64, a *arena) []EncodedChunk {
	var storage [8]chunks.Chunk
	raw := queryRawChunks(series, stored, coldChunks, disk, start, end, storage[:0])
	out := a.chunkSlice(len(raw))
	for index := range raw {
		out = append(out, wireChunk(raw[index].MinTime, raw[index].MaxTime, raw[index].Encoding, raw[index].Data, a))
	}
	return out
}

// queryRawChunks appends to out the series' chunks in [start, end], as queryChunks returns them.
// Their data may point into the chunk files or the series' open chunks: callers copy it before
// releasing the shard's lock.
func queryRawChunks(series *Series, stored, coldChunks []ChunkMeta, disk *chunks.DiskMapper, start, end int64, out []chunks.Chunk) []chunks.Chunk {
	overlaps := func(minTime, maxTime int64) bool { return minTime <= end && maxTime >= start }
	var (
		candidates        []chunks.Candidate
		inOrder, ooo      int
		anyOutOfOrder     bool
		candidatesStorage [8]chunks.Candidate
	)
	candidates = candidatesStorage[:0]
	add := func(chunk chunks.Chunk, outOfOrder bool) {
		counter := &inOrder
		if outOfOrder {
			counter = &ooo
		}
		*counter++
		if overlaps(chunk.MinTime, chunk.MaxTime) {
			candidates = append(candidates, chunks.Candidate{Chunk: chunk, OutOfOrder: outOfOrder, Index: *counter})
			if outOfOrder {
				anyOutOfOrder = true
			}
		}
	}
	// A series' cold chunks come first: they are older than what it has in memory, like a block's
	// before the head's. A chunk's data stays where it is, in the chunk files or the series, until
	// a merge needs it.
	for _, list := range [2][]ChunkMeta{coldChunks, stored} {
		for index := range list {
			meta := &list[index]
			chunk := chunks.Chunk{MinTime: meta.MinTime, MaxTime: meta.MaxTime, Encoding: int32(meta.Encoding)}
			if overlaps(meta.MinTime, meta.MaxTime) {
				chunk.Data = disk.Read(meta.Ref, meta.Len)
			}
			add(chunk, meta.OutOfOrder)
		}
	}
	if series != nil {
		if fh := series.floatHead; fh != nil {
			add(chunks.Chunk{MinTime: fh.minTime, MaxTime: fh.lastTimestamp(), Encoding: int32(xorEncoding), Data: fh.appender.Bytes()}, false)
		}
		if hh := series.histogramHead; hh != nil && overlaps(hh.FirstTimestamp(), hh.Last().Timestamp) {
			encoded := hh.Encoded()
			add(chunks.Chunk{MinTime: hh.FirstTimestamp(), MaxTime: hh.Last().Timestamp, Encoding: encoded.Encoding, Data: encoded.Data}, false)
		}
		// The open out-of-order chunk shares one position, like Prometheus's OOO head chunk.
		ooo++
		if len(series.outOfOrder) > 0 {
			head, err := chunks.EncodeOutOfOrder(series.outOfOrder)
			if err == nil {
				for _, chunk := range head {
					if overlaps(chunk.MinTime, chunk.MaxTime) {
						candidates = append(candidates, chunks.Candidate{Chunk: chunk, OutOfOrder: true, Index: ooo})
						anyOutOfOrder = true
					}
				}
			}
		}
	}
	if anyOutOfOrder {
		merged, err := chunks.MergeOverlapping(candidates)
		if err != nil {
			return out
		}
		return append(out, merged...)
	}
	sort.SliceStable(candidates, func(x, y int) bool { return candidates[x].Chunk.MinTime < candidates[y].Chunk.MinTime })
	for index := range candidates {
		out = append(out, candidates[index].Chunk)
	}
	return out
}

func wireChunk(minTime, maxTime int64, encoding int32, data []byte, a *arena) EncodedChunk {
	var samples uint32
	if len(data) >= 2 {
		samples = uint32(binary.BigEndian.Uint16(data))
	}
	return EncodedChunk{StartTimestampMs: minTime, EndTimestampMs: maxTime, Samples: samples, Wire: encodeChunk(minTime, maxTime, encoding, data, a)}
}

// encodeChunk writes a `cortex.Chunk` message as protobuf encoders do, straight from the chunk's
// data: building the message first copied every returned chunk twice more. Like proto3, fields at
// their default are left out, in tag order.
func encodeChunk(minTime, maxTime int64, encoding int32, data []byte, a *arena) []byte {
	size := 0
	for _, value := range [3]uint64{uint64(minTime), uint64(maxTime), uint64(int64(encoding))} {
		if value != 0 {
			size += 1 + protowire.SizeVarint(value)
		}
	}
	if len(data) > 0 {
		size += 1 + protowire.SizeBytes(len(data))
	}
	var out []byte
	if a != nil {
		out = a.alloc(size)
	} else {
		out = make([]byte, 0, size)
	}
	for tag, value := range [3]uint64{uint64(minTime), uint64(maxTime), uint64(int64(encoding))} {
		if value != 0 {
			out = protowire.AppendTag(out, protowire.Number(tag+1), protowire.VarintType)
			out = protowire.AppendVarint(out, value)
		}
	}
	if len(data) > 0 {
		out = protowire.AppendTag(out, 4, protowire.BytesType)
		out = protowire.AppendBytes(out, data)
	}
	return out
}

// encodeSeriesLabels writes the `QueryStreamSeries` of the labels, without chunks, directly:
// building a message of copied labels to encode it was an eighth of a busy ingester's query CPU.
// Like proto3, empty fields are left out.
func encodeSeriesLabels(stored labels.Labels, a *arena) []byte {
	fieldLen := func(value string) int {
		if value == "" {
			return 0
		}
		return 1 + protowire.SizeBytes(len(value))
	}
	size := 0
	it := stored.Iter()
	for name, value, ok := it.Next(); ok; name, value, ok = it.Next() {
		inner := fieldLen(name) + fieldLen(value)
		size += 1 + protowire.SizeBytes(inner)
	}
	var out []byte
	if a != nil {
		out = a.alloc(size)
	} else {
		out = make([]byte, 0, size)
	}
	it = stored.Iter()
	for name, value, ok := it.Next(); ok; name, value, ok = it.Next() {
		out = protowire.AppendTag(out, 1, protowire.BytesType)
		out = protowire.AppendVarint(out, uint64(fieldLen(name)+fieldLen(value)))
		if name != "" {
			out = protowire.AppendTag(out, 1, protowire.BytesType)
			out = protowire.AppendString(out, name)
		}
		if value != "" {
			out = protowire.AppendTag(out, 2, protowire.BytesType)
			out = protowire.AppendString(out, value)
		}
	}
	return out
}

// SelectExemplars returns the exemplars of the matching series in [start, end], sorted by series
// labels as the distributor merges them.
func (s *Store) SelectExemplars(tenantID string, start, end int64, matchers []LabelMatcher) ([]SeriesView, error) {
	compiled, err := compileMatchers(matchers)
	if err != nil {
		return nil, err
	}
	s.exemplarsLock.Lock()
	storage, ok := s.exemplars[tenantID]
	if !ok {
		s.exemplarsLock.Unlock()
		return nil, nil
	}
	selected := storage.Select(start, end, func(stored *labels.Labels) bool { return matches(*stored, compiled) })
	s.exemplarsLock.Unlock()
	slices.SortFunc(selected, func(a, b exemplars.Selected[labels.Labels]) int { return labels.Compare(a.Labels, b.Labels) })
	views := make([]SeriesView, len(selected))
	for index := range selected {
		views[index] = SeriesView{Labels: selected[index].Labels.Pairs(), Exemplars: selected[index].Exemplars}
	}
	return views, nil
}

// SelectLabels returns the labels of the matching series with samples in [start, end].
func (s *Store) SelectLabels(tenantID string, start, end int64, matchers []LabelMatcher) ([][][2]string, error) {
	compiled, err := compileMatchers(matchers)
	if err != nil {
		return nil, err
	}
	var result [][][2]string
	s.perShardWithCold(tenantID, func(t *tenant, _ *chunks.DiskMapper, cold *coldState) {
		selected := map[labels.Labels]struct{}{}
		t.series.matching(compiled, func(entry *seriesEntry) bool {
			if s.memoryIsHead && entry.series.hasChunkIn(start, end) || !s.memoryIsHead && entry.series.hasDataIn(start, end) {
				selected[entry.labels] = struct{}{}
			}
			return true
		})
		cold.matching(tenantID, compiled, start, end, func(series *coldSeries) {
			selected[series.labels()] = struct{}{}
		})
		for stored := range selected {
			result = append(result, stored.Pairs())
		}
	})
	return result, nil
}

// headView is the emulated Go head of a tenant, which is all that the Go ingester's head-only APIs
// see.
type headView struct {
	headMin, minTime, maxTime int64
	// The engine's out-of-order head bounds and the start of its oldest emulated block.
	minOOOTime, maxOOOTime int64
	blocks                 []emulatedBlock
	// Whether every series in memory is in the head, as the engine's head garbage collection
	// keeps it; the store's Kafka path keeps series out of the head until its next head tick.
	memoryIsHead bool
}

var emptyHeadView = headView{headMin: math.MinInt64, minTime: math.MaxInt64, maxTime: math.MinInt64}

func (h headView) holds(series *Series) bool {
	if series.headEvicted {
		return false
	}
	if h.memoryIsHead {
		return true
	}
	newest, ok := series.newest()
	return ok && newest >= h.headMin
}

// labelWindow is which series a label lookup over [start, end] sees: like Prometheus's head index,
// every head series once the range overlaps the head, and like compacted blocks, every series
// with data in a block range the lookup overlaps.
type labelWindow struct {
	head             bool
	view             headView
	hasBlocks        bool
	lower, upper     int64
	rangeStart, rEnd int64
	// The engine's blocks the lookup overlaps, which hold their series whatever the range.
	blocks []emulatedBlock
}

func (h headView) labelWindow(start, end int64) labelWindow {
	headLower := max(h.headMin, h.minTime)
	w := labelWindow{
		head:       h.maxTime != math.MinInt64 && start <= h.maxTime && end >= headLower,
		view:       h,
		rangeStart: start,
		rEnd:       end,
	}
	if h.memoryIsHead {
		return h.engineLabelWindow(w, start, end)
	}
	if h.headMin != math.MinInt64 {
		lower := rangeStart(start)
		upper := satSub(min(rangeEnd(end), h.headMin), 1)
		if lower <= upper {
			w.hasBlocks, w.lower, w.upper = true, lower, upper
		}
	}
	return w
}

// engineLabelWindow is Prometheus's DB.Querier: the head's index when the range overlaps the
// head's in-order bounds, which its label lookups check even when the range only overlaps the
// out-of-order head, and every block the range overlaps.
func (h headView) engineLabelWindow(w labelWindow, start, end int64) labelWindow {
	w.head = h.minTime != math.MaxInt64 && end >= h.minTime && start <= h.maxTime
	for _, block := range h.blocks {
		if block.overlaps(start, end) {
			w.blocks = append(w.blocks, block)
		}
	}
	return w
}

func (w *labelWindow) inBlocks(hash uint64) bool {
	for i := range w.blocks {
		if w.blocks[i].holds(hash) {
			return true
		}
	}
	return false
}

// coldMatching calls visit for the cold series matching matchers the window sees.
func (w *labelWindow) coldMatching(cold *coldState, tenantID string, matchers []compiledMatcher, visit func(*coldSeries)) {
	if len(w.blocks) > 0 {
		cold.matching(tenantID, matchers, math.MinInt64, math.MaxInt64, func(series *coldSeries) {
			if w.inBlocks(series.labels().Hash()) {
				visit(series)
			}
		})
		return
	}
	if w.hasBlocks {
		cold.matching(tenantID, matchers, w.lower, w.upper, visit)
	}
}

func (w *labelWindow) includes(entry *seriesEntry) bool {
	series := &entry.series
	return (w.head && w.view.holds(series)) ||
		(len(w.blocks) > 0 && w.inBlocks(entry.hash)) ||
		(w.hasBlocks && series.hasDataIn(w.lower, w.upper)) ||
		// Evicted series are in their own compacted block.
		(series.headEvicted && series.hasDataIn(w.rangeStart, w.rEnd))
}

func (s *Store) headView(tenantID string) headView {
	home := s.shards[0]
	home.RLock()
	defer home.RUnlock()
	t, ok := home.tenants[tenantID]
	if !ok {
		return emptyHeadView
	}
	return headView{headMin: t.headMin, minTime: t.minTime, maxTime: t.maxTime, memoryIsHead: s.memoryIsHead, minOOOTime: t.minOOOTime, maxOOOTime: t.maxOOOTime, blocks: t.blocks}
}

// LabelNames returns the label names of the matching series the label window of [start, end]
// sees, sorted.
func (s *Store) LabelNames(tenantID string, start, end int64, matchers []LabelMatcher) ([]string, error) {
	compiled, err := compileMatchers(matchers)
	if err != nil {
		return nil, err
	}
	window := s.headView(tenantID).labelWindow(start, end)
	names := map[string]struct{}{}
	s.perShardWithCold(tenantID, func(t *tenant, _ *chunks.DiskMapper, cold *coldState) {
		t.series.matching(compiled, func(entry *seriesEntry) bool {
			if window.includes(entry) {
				it := entry.labels.Iter()
				for name, _, ok := it.Next(); ok; name, _, ok = it.Next() {
					names[name] = struct{}{}
				}
			}
			return true
		})
		window.coldMatching(cold, tenantID, compiled, func(series *coldSeries) {
			series.Range(func(name, _ string) { names[name] = struct{}{} })
		})
	})
	return sortedKeys(names), nil
}

// LabelValues returns the values of name of the matching series the label window of [start, end]
// sees, sorted.
func (s *Store) LabelValues(tenantID, name string, start, end int64, matchers []LabelMatcher) ([]string, error) {
	compiled, err := compileMatchers(matchers)
	if err != nil {
		return nil, err
	}
	window := s.headView(tenantID).labelWindow(start, end)
	values := map[string]struct{}{}
	id, known := labels.Lookup(name)
	s.perShardWithCold(tenantID, func(t *tenant, _ *chunks.DiskMapper, cold *coldState) {
		t.series.matching(compiled, func(entry *seriesEntry) bool {
			if known && window.includes(entry) {
				if value, ok := labelValue(entry.labels, id); ok {
					values[value] = struct{}{}
				}
			}
			return true
		})
		window.coldMatching(cold, tenantID, compiled, func(series *coldSeries) {
			if value, ok := series.lookup(name); ok {
				values[value] = struct{}{}
			}
		})
	})
	return sortedKeys(values), nil
}

// labelValue returns the value of the label with name id, and whether the labels have it.
func labelValue(stored labels.Labels, id uint32) (string, bool) {
	rest := string(stored)
	for len(rest) > 0 {
		name := uint32(takeUvarintString(&rest))
		size := takeUvarintString(&rest)
		if name == id {
			return rest[:size], true
		}
		rest = rest[size:]
	}
	return "", false
}

func sortedKeys(set map[string]struct{}) []string {
	keys := make([]string, 0, len(set))
	for key := range set {
		keys = append(keys, detach(key))
	}
	slices.Sort(keys)
	return keys
}

// detach copies a string that may share a series' label buffer, so results don't keep it alive.
func detach(s string) string {
	return string([]byte(s))
}

func (s *Store) HasTenant(tenantID string) bool {
	home := s.shards[0]
	home.RLock()
	defer home.RUnlock()
	_, ok := home.tenants[tenantID]
	return ok
}

// NumSeries returns the tenant's series in memory.
func (s *Store) NumSeries(tenantID string) uint64 {
	var total uint64
	for _, shard := range s.shards {
		shard.RLock()
		if t, ok := shard.tenants[tenantID]; ok {
			total += uint64(t.series.len)
		}
		shard.RUnlock()
	}
	return total
}

// ActiveSeries returns the tenant's active series that match, only native histograms with
// histogramsOnly.
func (s *Store) ActiveSeries(tenantID string, matchers []LabelMatcher, histogramsOnly bool) ([]ActiveSeriesView, error) {
	compiled, err := compileMatchers(matchers)
	if err != nil {
		return nil, err
	}
	cutoff := satSub(nowMs(), s.activeWindowMs)
	var views []ActiveSeriesView
	for _, shard := range s.shards {
		shard.RLock()
		if t, ok := shard.tenants[tenantID]; ok {
			t.series.matching(compiled, func(entry *seriesEntry) bool {
				series := &entry.series
				if series.isActive(cutoff) && (!histogramsOnly || series.nativeHistogram) {
					views = append(views, ActiveSeriesView{Labels: entry.labels.Pairs(), BucketCount: uint64(series.lastBucketCount)})
				}
				return true
			})
		}
		shard.RUnlock()
	}
	return views, nil
}

// SetOwnedRanges sets each tenant's token ranges of this partition.
func (s *Store) SetOwnedRanges(ranges map[string]TenantRanges) {
	s.ownedRanges.Store(&ranges)
}

// TenantIDs returns the stored tenants, sorted.
func (s *Store) TenantIDs() []string {
	home := s.shards[0]
	home.RLock()
	defer home.RUnlock()
	ids := make([]string, 0, len(home.tenants))
	for id := range home.tenants {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

func tenantStats(t *tenant, active bool, cutoff int64, head headView) UserStatsView {
	// Like `Head.NumSeries`, only series still in the head count.
	var numSeries uint64
	t.series.forEach(func(entry *seriesEntry) {
		if active && entry.series.isActive(cutoff) || !active && head.holds(&entry.series) {
			numSeries++
		}
	})
	now := time.Now()
	var api, rule uint64
	for _, entry := range t.ingested {
		if now.Sub(entry.at) > time.Minute {
			continue
		}
		if entry.source == 1 {
			rule += entry.samples
		} else {
			api += entry.samples
		}
	}
	return UserStatsView{
		NumSeries:         numSeries,
		IngestionRate:     float64(api+rule) / 60,
		APIIngestionRate:  float64(api) / 60,
		RuleIngestionRate: float64(rule) / 60,
	}
}

func addStats(total, shard UserStatsView) UserStatsView {
	return UserStatsView{
		NumSeries:         total.NumSeries + shard.NumSeries,
		IngestionRate:     total.IngestionRate + shard.IngestionRate,
		APIIngestionRate:  total.APIIngestionRate + shard.APIIngestionRate,
		RuleIngestionRate: total.RuleIngestionRate + shard.RuleIngestionRate,
	}
}

// UserStats returns the tenant's head series, or active series, and ingestion rates.
func (s *Store) UserStats(tenantID string, active bool) UserStatsView {
	cutoff := satSub(nowMs(), s.activeWindowMs)
	head := s.headView(tenantID)
	var total UserStatsView
	for _, shard := range s.shards {
		shard.RLock()
		if t, ok := shard.tenants[tenantID]; ok {
			total = addStats(total, tenantStats(t, active, cutoff, head))
		}
		shard.RUnlock()
	}
	return total
}

// TenantStats is a tenant's UserStatsView.
type TenantStats struct {
	Tenant string
	Stats  UserStatsView
}

// AllUserStats returns every tenant's stats, sorted by tenant.
func (s *Store) AllUserStats(active bool) []TenantStats {
	cutoff := satSub(nowMs(), s.activeWindowMs)
	heads := map[string]headView{}
	home := s.shards[0]
	home.RLock()
	for id, t := range home.tenants {
		heads[id] = headView{headMin: t.headMin, minTime: t.minTime, maxTime: t.maxTime}
	}
	home.RUnlock()
	perShard := make([]map[string]UserStatsView, len(s.shards))
	_ = s.parallel(len(s.shards), func(index int) error {
		shard := s.shards[index]
		shard.RLock()
		defer shard.RUnlock()
		stats := make(map[string]UserStatsView, len(shard.tenants))
		for id, t := range shard.tenants {
			head, ok := heads[id]
			if !ok {
				head = emptyHeadView
			}
			stats[id] = tenantStats(t, active, cutoff, head)
		}
		perShard[index] = stats
		return nil
	})
	merged := map[string]UserStatsView{}
	for _, stats := range perShard {
		for id, value := range stats {
			merged[id] = addStats(merged[id], value)
		}
	}
	result := make([]TenantStats, 0, len(merged))
	for id, stats := range merged {
		result = append(result, TenantStats{id, stats})
	}
	slices.SortFunc(result, func(a, b TenantStats) int {
		switch {
		case a.Tenant < b.Tenant:
			return -1
		case a.Tenant > b.Tenant:
			return 1
		}
		return 0
	})
	return result
}

// LabelNamesAndValues returns, like Go, the head index's label names and values of the matching
// series: its series, or the active ones.
func (s *Store) LabelNamesAndValues(tenantID string, matchers []LabelMatcher, active bool) (map[string]map[string]struct{}, error) {
	compiled, err := compileMatchers(matchers)
	if err != nil {
		return nil, err
	}
	cutoff := satSub(nowMs(), s.activeWindowMs)
	head := s.headView(tenantID)
	result := map[string]map[string]struct{}{}
	for _, shard := range s.shards {
		shard.RLock()
		if t, ok := shard.tenants[tenantID]; ok {
			t.series.matching(compiled, func(entry *seriesEntry) bool {
				if active && !entry.series.isActive(cutoff) || !active && !head.holds(&entry.series) {
					return true
				}
				it := entry.labels.Iter()
				for name, value, ok := it.Next(); ok; name, value, ok = it.Next() {
					values, ok := result[name]
					if !ok {
						values = map[string]struct{}{}
						result[detach(name)] = values
					}
					if _, ok := values[value]; !ok {
						values[detach(value)] = struct{}{}
					}
				}
				return true
			})
		}
		shard.RUnlock()
	}
	return result, nil
}

// LabelValuesCardinality returns, like Go, the head index's series count of each value of
// labelNames among the matching series: its series, or the active ones.
func (s *Store) LabelValuesCardinality(tenantID string, labelNames []string, matchers []LabelMatcher, active bool) (map[string]map[string]uint64, error) {
	compiled, err := compileMatchers(matchers)
	if err != nil {
		return nil, err
	}
	cutoff := satSub(nowMs(), s.activeWindowMs)
	head := s.headView(tenantID)
	wanted := map[string]struct{}{}
	for _, name := range labelNames {
		wanted[name] = struct{}{}
	}
	result := map[string]map[string]uint64{}
	for _, shard := range s.shards {
		shard.RLock()
		if t, ok := shard.tenants[tenantID]; ok {
			t.series.matching(compiled, func(entry *seriesEntry) bool {
				if active && !entry.series.isActive(cutoff) || !active && !head.holds(&entry.series) {
					return true
				}
				it := entry.labels.Iter()
				for name, value, ok := it.Next(); ok; name, value, ok = it.Next() {
					if _, ok := wanted[name]; !ok {
						continue
					}
					values, ok := result[name]
					if !ok {
						values = map[string]uint64{}
						result[detach(name)] = values
					}
					if _, ok := values[value]; ok {
						values[value]++
					} else {
						values[detach(value)] = 1
					}
				}
				return true
			})
		}
		shard.RUnlock()
	}
	return result, nil
}

// Metadata returns the tenant's metric metadata, by metric family and then type, help and unit.
func (s *Store) Metadata(tenantID string) []mimirpb.MetricMetadata {
	var result []mimirpb.MetricMetadata
	for _, shard := range s.shards {
		shard.RLock()
		if t, ok := shard.tenants[tenantID]; ok {
			result = append(result, sortedMetadata(t)...)
		}
		shard.RUnlock()
	}
	return result
}

func sortedMetadata(t *tenant) []mimirpb.MetricMetadata {
	names := make([]string, 0, len(t.metadata))
	for name := range t.metadata {
		names = append(names, name)
	}
	slices.Sort(names)
	var result []mimirpb.MetricMetadata
	for _, name := range names {
		set := t.metadata[name]
		keys := make([]metadataKey, 0, len(set))
		for key := range set {
			keys = append(keys, key)
		}
		slices.SortFunc(keys, compareMetadataKeys)
		for _, key := range keys {
			result = append(result, set[key].metadata)
		}
	}
	return result
}

func compareMetadataKeys(a, b metadataKey) int {
	switch {
	case a.metricType != b.metricType:
		if a.metricType < b.metricType {
			return -1
		}
		return 1
	case a.help != b.help:
		if a.help < b.help {
			return -1
		}
		return 1
	case a.unit != b.unit:
		if a.unit < b.unit {
			return -1
		}
		return 1
	}
	return 0
}

// compareLabels orders labels like labels.Compare, pair by pair by name then value, reading
// the encoded ids: labels of one query mostly share their names, which then need no lookup.
func compareLabels(names labels.Snapshot, a, b labels.Labels) int {
	x, y := string(a), string(b)
	for {
		switch {
		case len(x) == 0 && len(y) == 0:
			return 0
		case len(x) == 0:
			return -1
		case len(y) == 0:
			return 1
		}
		xName, yName := uint32(takeUvarintString(&x)), uint32(takeUvarintString(&y))
		if xName != yName {
			if c := cmpStrings(names.Name(xName), names.Name(yName)); c != 0 {
				return c
			}
		}
		xSize, ySize := takeUvarintString(&x), takeUvarintString(&y)
		if c := cmpStrings(x[:xSize], y[:ySize]); c != 0 {
			return c
		}
		x, y = x[xSize:], y[ySize:]
	}
}

func cmpStrings(a, b string) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}
