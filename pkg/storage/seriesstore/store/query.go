// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"cmp"
	"encoding/binary"
	"math"
	"slices"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/grafana/mimir/pkg/storage/seriesstore/chunks"
	"github.com/grafana/mimir/pkg/storage/seriesstore/labels"
)

type coldCandidateSeries struct {
	labels labels.Labels
	chunks []ChunkMeta
	// Its series in memory took its chunks.
	taken bool
}

// coldSelection is the cold series of a shard a query selects, by labels: one may be in several blocks, and also back in
// memory. It is reused for each shard, and kept between queries, with the candidates' chunk lists: growing them was a
// few percent of what an ingester allocated.
type coldSelection struct {
	byLabels   map[labels.Labels]int32
	candidates []coldCandidateSeries
	// How many candidates the selection had at most, which give clears.
	written int
}

var coldSelections sync.Pool

func takeColdSelection() *coldSelection {
	if selection, ok := coldSelections.Get().(*coldSelection); ok {
		return selection
	}
	return &coldSelection{}
}

// reset empties the selection for another shard.
func (c *coldSelection) reset() {
	if len(c.byLabels) > 0 {
		clear(c.byLabels)
	}
	c.written = max(c.written, len(c.candidates))
	c.candidates = c.candidates[:0]
}

// candidate returns the candidate with the labels, added without chunks when there's none yet.
func (c *coldSelection) candidate(stored labels.Labels) *coldCandidateSeries {
	if index, ok := c.byLabels[stored]; ok {
		return &c.candidates[index]
	}
	if c.byLabels == nil {
		c.byLabels = map[labels.Labels]int32{}
	}
	c.byLabels[stored] = int32(len(c.candidates))
	if len(c.candidates) < cap(c.candidates) {
		c.candidates = c.candidates[:len(c.candidates)+1]
	} else {
		c.candidates = append(c.candidates, coldCandidateSeries{})
	}
	candidate := &c.candidates[len(c.candidates)-1]
	// The chunk list an earlier candidate grew is reused.
	candidate.labels, candidate.chunks, candidate.taken = stored, candidate.chunks[:0], false
	return candidate
}

// take returns the chunks of the cold series with the labels, which then aren't among the ones left.
func (c *coldSelection) take(stored labels.Labels) ([]ChunkMeta, bool) {
	if len(c.candidates) == 0 {
		return nil, false
	}
	index, ok := c.byLabels[stored]
	if !ok {
		return nil, false
	}
	candidate := &c.candidates[index]
	candidate.taken = true
	return candidate.chunks, true
}

// give keeps the selection for another query, unless it is large, with the labels cleared: they point into one tenant's
// cold blocks, which are unmapped once merged.
func (c *coldSelection) give() {
	c.reset()
	if c.written > 1<<14 {
		return
	}
	written := c.candidates[:c.written]
	for index := range written {
		written[index].labels = ""
		if cap(written[index].chunks) > 1<<10 {
			written[index].chunks = nil
		}
	}
	c.written = 0
	coldSelections.Put(c)
}

// selectCold returns the tenant's cold series of store shard shard that match, by labels, with
// the chunks the retention kept.
// coldParts are the parts of coldLabels when the caller has them made, which the blocks' candidates caches then keep.
func (s *Store) selectCold(tenantID string, cold *coldState, compiled, coldLabels, coldShard []compiledMatcher, coldKey uint64, coldParts []selectorPart, start, end int64, kept *retained, shard int, into *coldSelection) {
	if !cold.overlaps(start, end) {
		return
	}
	// Like the head's, each regex matcher's result for the label values it saw.
	remembered := make([]map[string]bool, len(coldLabels))
	var pairs [][2]string
	// Most selectors name one metric, and most blocks have few of the metrics.
	nameHash, byName := metricNameHash(compiled)
	// One for every series, as matches gets its address and so it lives on the heap: made at the first candidate, as
	// most calls skip every block.
	var series *coldSeries
	selector := coldSelector{id: coldKey, matchers: coldLabels, parts: coldParts}
	var hits, misses int64
	if len(coldParts) > 0 {
		selector.verified = unsafe.SliceData(coldParts)
	}
	for _, block := range cold.blocks {
		table := block.tenant(tenantID)
		if table == nil || table.minTime > end || table.maxTime < start {
			continue
		}
		if byName && !block.mayHaveName(table, nameHash) {
			continue
		}
		// For each label matcher that accepts the empty value, the series with its label when few
		// are: the others match it without reading their labels.
		var sets [][]uint32
		setsComputed := false
		candidates, hit, matchedBits, labelsChecked := block.matchedCachedIn(table, tenantID, &selector, compiled)
		if hit {
			hits++
		} else {
			misses++
		}
		for position, index := range candidates {
			if labelsChecked && matchedBits&(uint64(1)<<position) == 0 {
				continue
			}
			// A hash comparison, where the rest decodes the series.
			if len(coldShard) > 0 && !inQueryShard(block.shardHash(table, int(index)), coldShard) {
				continue
			}
			if series == nil {
				series = new(coldSeries)
			}
			*series = block.seriesIn(table, int(index))
			if !series.hasData(start, end) {
				continue
			}
			if !setsComputed && !labelsChecked {
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
			if !labelsChecked {
				for m := range coldLabels {
					matcher := &coldLabels[m]
					if with := sets[m]; with != nil {
						if _, found := slices.BinarySearch(with, index); !found {
							continue
						}
					}
					if matcher.kind != kindRegex && matcher.kind != kindNotRegex {
						if !matches(series, coldLabels[m:m+1]) {
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
			}
			if !matched {
				continue
			}
			candidate := into.candidate(series.labelsInto(&pairs))
			it := series.chunkIter()
			for chunk, more := it.next(); more; chunk, more = it.next() {
				if kept.keepsSeries(chunk, shard, block.labelsHash(table, int(index))) {
					candidate.chunks = append(candidate.chunks, chunk)
				}
			}
		}
	}
	countColdCandidates(cold, hits, misses)
}

// queryRawChunks appends to out the series' chunks in [start, end], as queryChunks returns them.
// Their data may point into the chunk files or the series' open chunks: callers copy it before
// releasing the shard's lock.
//
// The candidates buffer is the caller's, which keeps it from series to series: a buffer of the function's own, which its
// closures made escape to the heap, was a third of what a query allocated.
func queryRawChunks(series *Series, stored, coldChunks []ChunkMeta, disk *chunks.DiskMapper, start, end int64, out []chunks.Chunk, buffer *[]chunks.Candidate) []chunks.Chunk {
	overlaps := func(minTime, maxTime int64) bool { return minTime <= end && maxTime >= start }
	var (
		candidates    = (*buffer)[:0]
		inOrder, ooo  int
		anyOutOfOrder bool
	)
	defer func() {
		clear(candidates)
		*buffer = candidates[:0]
	}()
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
		if hh := series.histogram(); hh != nil && overlaps(hh.FirstTimestamp(), hh.Last().Timestamp) {
			encoded := hh.Encoded()
			add(chunks.Chunk{MinTime: hh.FirstTimestamp(), MaxTime: hh.Last().Timestamp, Encoding: encoded.Encoding, Data: encoded.Data}, false)
		}
		// The open out-of-order chunk shares one position, like Prometheus's OOO head chunk.
		ooo++
		if samples := series.ooo(); len(samples) > 0 && series.oooFloatsOnly() {
			// Encoded only if it overlaps no other chunk: merged with one, it is read as it is.
			if first, last := samples[0].T, samples[len(samples)-1].T; overlaps(first, last) {
				candidates = append(candidates, chunks.Candidate{Chunk: chunks.Chunk{MinTime: first, MaxTime: last, Encoding: int32(xorEncoding)}, OutOfOrder: true, Index: ooo, Floats: samples})
				anyOutOfOrder = true
			}
		} else if len(series.ooo()) > 0 {
			head, err := chunks.EncodeOutOfOrder(series.ooo())
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
	slices.SortStableFunc(candidates, func(x, y chunks.Candidate) int { return cmp.Compare(x.Chunk.MinTime, y.Chunk.MinTime) })
	for index := range candidates {
		out = append(out, candidates[index].Chunk)
	}
	return out
}

// headView is the emulated Go head of a tenant, which is all that the Go ingester's head-only APIs
// see.
type headView struct {
	headMin, minTime, maxTime int64
	blocks                    []emulatedBlock
}

// labelWindow is which series a label lookup over [start, end] sees: like Prometheus's head index,
// every head series once the range overlaps the head, and like compacted blocks, every series
// with data in a block range the lookup overlaps.
type labelWindow struct {
	head             bool
	rangeStart, rEnd int64
	// The engine's blocks the lookup overlaps, which hold their series whatever the range.
	blocks []emulatedBlock
}

// The head's index is included when the range overlaps its in-order bounds, even for out-of-order queries.
func (h headView) labelWindow(start, end int64) labelWindow {
	w := labelWindow{head: h.minTime != math.MaxInt64 && end >= h.minTime && start <= h.maxTime, rangeStart: start, rEnd: end}
	for _, block := range h.blocks {
		// Prometheus's head still holds the series the engine evicted for being idle, and its index has them whatever
		// the range.
		if block.overlaps(start, end) || w.head && block.idleEvicted && block.maxTime > h.headMin {
			w.blocks = append(w.blocks, block)
		}
	}
	return w
}

// blocksRange is the range of the blocks the window sees: their series left the head with samples in their block, which
// the cold blocks they went to have, so the cold data out of it is of no use to the window, though a series' older data
// may be.
func (w labelWindow) blocksRange() (start, end int64) {
	start, end = math.MaxInt64, math.MinInt64
	for i := range w.blocks {
		start, end = min(start, w.blocks[i].minTime), max(end, w.blocks[i].maxTime)
	}
	return start, end
}

func inBlocks(blocks []emulatedBlock, hash uint64) bool {
	for i := range blocks {
		if blocks[i].holds(hash) {
			return true
		}
	}
	return false
}

// coldMatching calls visit for the cold series matching matchers the window sees, of those useful says would add to
// the result: whether the window sees a series is a search in the hashes of every block, where most series of a
// lookup add nothing, having the names or the value of one before them.
func (w labelWindow) coldMatching(cold coldView, tenantID string, matchers []compiledMatcher, useful func(*coldSeries) bool, visit func(*coldSeries)) {
	if len(w.blocks) == 0 {
		return
	}
	start, end := w.blocksRange()
	cold.matchingIndexed(tenantID, matchers, start, end, func(table *coldTenantIndex, index int, series *coldSeries) {
		if useful(series) && inBlocks(w.blocks, series.block.labelsHash(table, index)) {
			visit(series)
		}
	})
}

// Unfiltered names need one included series per name, rather than every series' labels.
func (w labelWindow) coldLabelNames(cold coldView, tenantID string, names map[string]struct{}) {
	if len(w.blocks) == 0 {
		return
	}
	start, end := w.blocksRange()
	for _, block := range cold {
		if !block.overlaps(tenantID, start, end) {
			continue
		}
		table := block.tenant(tenantID)
		budget := 128
	namesLoop:
		for local, name := range block.names {
			if _, seen := names[name]; seen {
				continue
			}
			low, high := block.nameRange(table, uint32(local))
			found := false
			for at := low; at < high && !found; at++ {
				_, _, offset := block.entry(table, at)
				list := block.postingData(table, offset)
				for len(list) > 0 {
					index := int(binary.LittleEndian.Uint32(list))
					list = list[4:]
					if budget == 0 {
						break namesLoop
					}
					if len(w.blocks) > 0 && !inBlocks(w.blocks, block.labelsHash(table, index)) {
						budget--
						continue
					}
					series := block.seriesIn(table, index)
					if series.hasData(start, end) {
						names[name] = struct{}{}
						found = true
						break
					}
					budget--
				}
			}
		}
		if budget == 0 {
			// A narrow window can reject every series of several names. Scan once
			// after bounded probes instead of revisiting those series for each name.
			w.coldMatching(coldView{block}, tenantID, nil, func(series *coldSeries) bool { return true }, func(series *coldSeries) {
				series.rangeNameIDs(func(id uint64) { names[block.names[id]] = struct{}{} })
			})
		}
	}
}

// coldLabelValues adds to values those of the label that the cold series the window sees and the matchers select have.
// A block's index of the label's values says which series have each value, and one of them that the window and the
// matchers accept says the value is there, rather than every series of the block being visited. A block whose label has
// too many values, or whose series checked that aren't accepted outnumber the budget, has its series visited instead.
func (w labelWindow) coldLabelValues(cold coldView, tenantID, name string, matchers []compiledMatcher, values map[string]struct{}, limit valueLimit) {
	if len(w.blocks) == 0 {
		return
	}
	start, end := w.blocksRange()
	verdicts := newRegexVerdicts(matchers)
	accept := func(block *coldBlock, table *coldTenantIndex, index int, series *coldSeries) bool {
		return series.hasData(start, end) && matchesWithVerdicts(series, matchers, verdicts) && inBlocks(w.blocks, block.labelsHash(table, index))
	}
	nameHash, byName := metricNameHash(matchers)
	var series coldSeries
	for _, block := range cold {
		if limit.reached(values) {
			return
		}
		if !block.overlaps(tenantID, start, end) {
			continue
		}
		table := block.tenant(tenantID)
		if byName && !block.mayHaveName(table, nameHash) {
			continue
		}
		blockMatchers, none := withoutAbsentLabels(block, matchers)
		if none {
			continue
		}
		check := accept
		if len(blockMatchers) != len(matchers) {
			blockVerdicts := newRegexVerdicts(blockMatchers)
			check = func(block *coldBlock, table *coldTenantIndex, index int, series *coldSeries) bool {
				return series.hasData(start, end) && matchesWithVerdicts(series, blockMatchers, blockVerdicts) && inBlocks(w.blocks, block.labelsHash(table, index))
			}
		}
		// Every series of a block the window holds has samples in it, so without matchers its values are all the
		// label's in the block, like a TSDB block's index has them, without a series checked against the window: a
		// lookup of the metric names over a day checked every series of every block.
		if len(blockMatchers) == 0 && len(w.blocks) == 0 && table.minTime >= start && table.maxTime <= end {
			coldBlockValues(block, table, name, values, limit)
			continue
		}
		// A predicate on the returned label can reject values before visiting their
		// postings, without building another index by decoding every series.
		// Metric-name predicates already reuse cached candidate lists below.
		var valueMatcher *compiledMatcher
		if len(blockMatchers) == 1 {
			if matcherName, ok := blockMatchers[0].labelName(); ok && matcherName == name {
				valueMatcher = &blockMatchers[0]
			}
		}
		if valueMatcher != nil && name != metricNameLabel {
			coldValuesByPostings(block, table, name, check, values, limit, valueMatcher)
			continue
		}
		index := block.valueIndex(table, name)
		var candidates []uint32
		satisfied := -1
		haveCandidates := false
		if index != nil && hasPostingsMatcher(blockMatchers) {
			// A value's series that the matchers don't select are each checked, which is most of them for a matcher of
			// a few metrics: their postings are fewer than the series the index would check, the most it checks.
			candidates, satisfied = block.candidatesSatisfying(tenantID, blockMatchers)
			haveCandidates = true
			if len(candidates) > coldValueIndexBudget(index) && coldValuesByIndex(block, table, index, check, values, limit) {
				continue
			}
		} else if index != nil && coldValuesByIndex(block, table, index, check, values, limit) {
			continue
		}
		if index == nil && len(blockMatchers) == 0 {
			// Too many values for the block's index: without matchers, a series of each of the label's postings.
			coldValuesByPostings(block, table, name, check, values, limit, nil)
			continue
		}
		if !haveCandidates {
			candidates, satisfied = block.candidatesSatisfying(tenantID, blockMatchers)
		}
		if satisfied >= 0 {
			// The candidates' names are the regex's: the other matchers are left to check.
			rest := slices.Delete(slices.Clone(blockMatchers), satisfied, satisfied+1)
			restVerdicts := newRegexVerdicts(rest)
			check = func(block *coldBlock, table *coldTenantIndex, index int, series *coldSeries) bool {
				return series.hasData(start, end) && matchesWithVerdicts(series, rest, restVerdicts) && inBlocks(w.blocks, block.labelsHash(table, index))
			}
		}
		local, ok := block.localIDs[name]
		if !ok {
			continue
		}
		for _, at := range candidates {
			if limit.reached(values) {
				return
			}
			series = block.seriesIn(table, int(at))
			value, ok := series.lookupLocal(local)
			if !ok {
				continue
			}
			if _, seen := values[value]; seen {
				continue
			}
			if check(block, table, int(at), &series) {
				// The value is in the block's mapping, which a compaction may unmap once the shard is unlocked, before
				// the sets are merged.
				values[strings.Clone(value)] = struct{}{}
			}
		}
	}
}

// withoutAbsentLabels drops the matchers of labels none of the block's series have, which read them as empty: each
// accepts every series of the block or none, which none reports, rather than being checked against each of them.
func withoutAbsentLabels(block *coldBlock, matchers []compiledMatcher) (kept []compiledMatcher, none bool) {
	absent := func(matcher *compiledMatcher) bool {
		name, ok := matcher.labelName()
		if !ok {
			return false
		}
		_, has := block.localIDs[name]
		return !has
	}
	dropped := false
	for index := range matchers {
		if absent(&matchers[index]) {
			if !matchers[index].matchesValue("") {
				return nil, true
			}
			dropped = true
		}
	}
	if !dropped {
		return matchers, false
	}
	for index := range matchers {
		if !absent(&matchers[index]) {
			kept = append(kept, matchers[index])
		}
	}
	return kept, false
}

// coldBlockValues adds every value of the label in the tenant's table of the block: the metric names from the block's
// list of them, the others from its index of the label's values, or from a series of each of the label's postings,
// which are by the value's 64-bit hash.
func coldBlockValues(block *coldBlock, table *coldTenantIndex, name string, values map[string]struct{}, limit valueLimit) {
	if name == metricNameLabel {
		for _, metric := range block.metricNamesOf(table) {
			if limit.reached(values) {
				return
			}
			values[metric.name] = struct{}{}
		}
		return
	}
	if index := block.valueIndex(table, name); index != nil {
		for _, value := range index.values {
			if limit.reached(values) {
				return
			}
			values[value] = struct{}{}
		}
		return
	}
	local, ok := block.localIDs[name]
	if !ok {
		return
	}
	low, _ := block.nameRange(table, local)
	var list []uint32
	for at := low; at < table.postingsCount && !limit.reached(values); at++ {
		entryName, _, offset := block.entry(table, at)
		if entryName != local {
			break
		}
		if list = block.list(table, offset, list[:0]); len(list) == 0 {
			continue
		}
		series := block.seriesIn(table, int(list[0]))
		if value, ok := series.lookup(name); ok {
			if _, seen := values[value]; !seen {
				// The value is in the block's mapping, which a compaction may unmap once the shard is unlocked.
				values[strings.Clone(value)] = struct{}{}
			}
		}
	}
}

// coldValuesByPostings adds the values of the label that a series accept accepts has, from each of the block's postings
// of the label: they are by the value's 64-bit hash, so one series of each that accept accepts has its value.
func coldValuesByPostings(block *coldBlock, table *coldTenantIndex, name string, accept func(*coldBlock, *coldTenantIndex, int, *coldSeries) bool, values map[string]struct{}, limit valueLimit, matcher *compiledMatcher) {
	local, ok := block.localIDs[name]
	if !ok {
		return
	}
	low, _ := block.nameRange(table, local)
	var series coldSeries
	for at := low; at < table.postingsCount && !limit.reached(values); at++ {
		entryName, _, offset := block.entry(table, at)
		if entryName != local {
			break
		}
		list := block.postingData(table, offset)
		if len(list) == 0 {
			continue
		}
		// Every series of a posting has its value: one the lookup has, of the head or another block, needs no series
		// of the posting checked against the window, which most values of a label of many are.
		series = block.seriesIn(table, int(binary.LittleEndian.Uint32(list)))
		value, ok := series.lookupLocal(local)
		if !ok || matcher != nil && !matcher.matchesValue(value) {
			continue
		}
		if _, seen := values[value]; seen {
			continue
		}
		for len(list) > 0 {
			index := binary.LittleEndian.Uint32(list)
			list = list[4:]
			series = block.seriesIn(table, int(index))
			if accept(block, table, int(index), &series) {
				// The value is in the block's mapping, which a compaction may unmap once the shard is unlocked.
				values[strings.Clone(value)] = struct{}{}
				break
			}
		}
	}
}

// coldValueIndexBudget is how many of the index's series that aren't accepted coldValuesByIndex checks.
func coldValueIndexBudget(index *coldValueIndex) int {
	return 64 + 4*len(index.values)
}

// coldValuesByIndex adds the values of the index that one of their series accept accepts, and reports false when the
// series it checked that weren't accepted outnumber the budget, when visiting the block's selected series costs less.
func coldValuesByIndex(block *coldBlock, table *coldTenantIndex, index *coldValueIndex, accept func(*coldBlock, *coldTenantIndex, int, *coldSeries) bool, values map[string]struct{}, limit valueLimit) bool {
	budget := coldValueIndexBudget(index)
	var series coldSeries
	for position, value := range index.values {
		if limit.reached(values) {
			return true
		}
		if _, seen := values[value]; seen {
			continue
		}
		for _, at := range index.series[position] {
			series = block.seriesIn(table, int(at))
			if accept(block, table, int(at), &series) {
				values[value] = struct{}{}
				break
			}
			if budget--; budget < 0 {
				return false
			}
		}
	}
	return true
}

func (w labelWindow) includes(entry *seriesEntry) bool {
	series := &entry.series
	return (w.head && !series.headEvicted) ||
		(len(w.blocks) > 0 && inBlocks(w.blocks, entry.hash)) ||
		// Evicted series are in their own compacted block.
		(series.headEvicted && series.hasDataIn(w.rangeStart, w.rEnd))
}

func (s *Store) headView() headView {
	home := s.shards[0]
	home.RLock()
	defer home.RUnlock()
	t := home.tenant.Load()
	return headView{headMin: t.headMin, minTime: t.minTime, maxTime: t.maxTime, blocks: t.blocks}
}

// perShardWithColdParallel is perShardWithCold with a goroutine per shard, for lookups that scan
// every series: each query is given its shard's index and writes only to its own results.
func (s *Store) perShardWithColdParallel(query func(shard int, t *tenant, read *shardRead)) {
	timer := startReadTimer(phaseLabels)
	var wg sync.WaitGroup
	for index, shard := range s.shards {
		wg.Add(1)
		go func() {
			defer wg.Done()
			shard.RLock()
			t := shard.tenant.Load()
			if t == nil {
				shard.RUnlock()
				return
			}
			// The cold blocks as of the series the head part reads, which the cold part reads with the shard unlocked:
			// a scan of cold blocks held the lock for up to a second, which pushes to the shard, and the reads after
			// them, waited for.
			read := shardRead{shard: shard, t: t, locked: true, removals: t.series.removals, moved: t.series.entriesMoved, cold: shard.cold.snapshot(), timer: timer, heldAt: timer.held()}
			defer read.endCold()
			defer read.headDone()
			query(index, t, &read)
		}()
	}
	wg.Wait()
}

// shardRead is a read of a shard whose head part holds the shard's lock, until headDone, but for the moments pause
// lets the pushes waiting on it through.
type shardRead struct {
	shard *shardState
	t     *tenant
	// The read's lock holds are timed for a sample of the reads: each from heldAt.
	timer  readTimer
	heldAt time.Time
	locked bool
	// The cold blocks the cold part reads: those as of the head part's last start.
	cold coldView
	// The shard's series removals and moves as of the head part's start, which a pause that sees them change starts
	// again.
	removals, moved uint64
	visits          int
	restarts        int
	started         bool
	stale           bool
}

const (
	// pauseEvery is how many series the head part of a read visits between letting the pushes waiting on the shard
	// through, a power of two: a walk of every series of a shard held its lock for up to a second, which pushes, and
	// every read after them, waited for.
	pauseEvery = 128
	// maxReadRestarts is how many times series removed during its pauses start a read's head part again, before it
	// pauses no more: removals as frequent as its pauses don't keep it from finishing.
	maxReadRestarts = 3
)

// again reports whether the head part has to run, first or again: a pause saw series removed, which moved the series
// it had yet to visit. The values it found stay: a series' values are its own whether or not it was removed since.
// The cold snapshot is taken again too, as the removed series may have left for a new cold block.
func (r *shardRead) again() bool {
	if !r.started {
		r.started = true
		return true
	}
	if !r.stale {
		return false
	}
	r.stale = false
	r.restarts++
	if countersEnabled {
		readRestarts.Add(1)
	}
	r.removals, r.moved = r.t.series.removals, r.t.series.entriesMoved
	r.shard.cold.endSnapshot()
	r.cold = r.shard.cold.snapshot()
	return true
}

// pause lets the pushes waiting on the shard through every pauseEvery calls, and reports whether the head part can
// carry on: false when series were removed meanwhile, which the head part stops for, to start again.
func (r *shardRead) pause() bool {
	// Small enough to be inlined into the visits of every series.
	r.visits++
	if r.visits&(pauseEvery-1) != 0 && !r.stale {
		return true
	}
	return r.pauseNow()
}

// pauseNow is pause, every pauseEvery calls or once the read is stale.
func (r *shardRead) pauseNow() bool {
	if r.stale {
		return false
	}
	if r.restarts >= maxReadRestarts || !r.locked {
		return true
	}
	// A writer waiting on the shard holds the read here until it is done.
	r.timer.release(r.heldAt)
	r.shard.RUnlock()
	r.shard.RLock()
	r.heldAt = r.timer.held()
	if countersEnabled {
		readPauses.Add(1)
	}
	if r.t.series.removals != r.removals || r.t.series.entriesMoved != r.moved {
		r.stale = true
		return false
	}
	return true
}

// headDone releases the shard's lock: what follows only reads the snapshot of the cold blocks.
func (r *shardRead) headDone() {
	r.stale = false
	if r.locked {
		r.locked = false
		r.timer.release(r.heldAt)
		r.shard.RUnlock()
	}
}

// endCold ends the read of the cold snapshot.
func (r *shardRead) endCold() {
	r.shard.cold.endSnapshot()
}

// LabelNames returns the label names of the matching series the label window of [start, end]
// sees, sorted.
func (s *Store) LabelNames(start, end int64, matchers []LabelMatcher) ([]string, error) {
	compiled, err := compileMatchers(matchers)
	if err != nil {
		return nil, err
	}
	window := s.headView().labelWindow(start, end)
	perShard := make([]map[string]struct{}, len(s.shards))
	s.perShardWithColdParallel(func(shard int, t *tenant, read *shardRead) {
		names := map[string]struct{}{}
		perShard[shard] = names
		for read.again() {
			if cached, ok := s.headLabelNames(t, compiled, window); ok {
				for _, name := range cached {
					names[name] = struct{}{}
				}
			} else if len(compiled) == 0 {
				// Every series, so each is only checked for the names not seen yet: a series' names are
				// the same few as the previous series'.
				var seen []bool
				t.series.forEachWhile(func(entry *seriesEntry) bool {
					if !read.pause() {
						return false
					}
					if !window.includes(entry) {
						return true
					}
					forEachLabelID(entry.labels, func(id uint32) {
						if int(id) >= len(seen) {
							seen = append(seen, make([]bool, int(id)+1-len(seen))...)
						}
						seen[id] = true
					})
					return true
				})
				for id, ok := range seen {
					if ok {
						names[labels.Name(uint32(id))] = struct{}{}
					}
				}
			} else {
				t.series.matchingPaused(compiled, read, func(entry *seriesEntry) bool {
					if !read.pause() {
						return false
					}
					if window.includes(entry) {
						it := entry.labels.Iter()
						for name, _, ok := it.Next(); ok; name, _, ok = it.Next() {
							names[name] = struct{}{}
						}
					}
					return true
				})
			}
		}
		read.headDone()
		cold := read.cold
		if len(compiled) == 0 {
			window.coldLabelNames(cold, s.tenantID, names)
			return
		}
		// Names are collected by block-local id, without decoding the values or inserting every
		// series' names: a block's names are the same few for most of its series.
		seen := map[*coldBlock][]bool{}
		idsOf := func(block *coldBlock) []bool {
			ids, ok := seen[block]
			if !ok {
				ids = make([]bool, len(block.names))
				seen[block] = ids
			}
			return ids
		}
		window.coldMatching(cold, s.tenantID, compiled, func(series *coldSeries) bool {
			ids, unseen := idsOf(series.block), false
			series.rangeNameIDs(func(id uint64) { unseen = unseen || !ids[id] })
			return unseen
		}, func(series *coldSeries) {
			ids := idsOf(series.block)
			series.rangeNameIDs(func(id uint64) { ids[id] = true })
		})
		for block, ids := range seen {
			for id, ok := range ids {
				if ok {
					names[block.names[id]] = struct{}{}
				}
			}
		}
	})
	return sortedKeys(mergeSets(perShard)), nil
}

// labelValuesHook runs with the sets of values found in each shard, once the shards are unlocked, for tests.
var labelValuesHook func(sets []map[string]struct{})

// LabelValues returns the values of name of the matching series the label window of [start, end]
// sees, sorted.
func (s *Store) LabelValues(name string, start, end int64, matchers []LabelMatcher) ([]string, error) {
	return s.LabelValuesLimited(name, start, end, matchers, 0)
}

// valueLimit is a lookup's limit on how many values each shard finds, 0 for none.
type valueLimit int

func (l valueLimit) reached(values map[string]struct{}) bool {
	return l > 0 && len(values) >= int(l)
}

// limitedPauser is a read's pauser that also stops a lookup once it found as many values as the limit.
type limitedPauser struct {
	read   *shardRead
	values map[string]struct{}
	limit  valueLimit
}

func (p limitedPauser) pause() bool {
	return !p.limit.reached(p.values) && p.read.pause()
}

// LabelValuesLimited is LabelValues returning at least limit of the values when it has that many, sorted, as the
// TSDB's ingester does when the caller doesn't ask for the smallest: any limit of them, so each shard stops looking
// once it found that many, where a limited lookup of a label of many values found them all. 0 for no limit.
func (s *Store) LabelValuesLimited(name string, start, end int64, matchers []LabelMatcher, limit int) ([]string, error) {
	compiled, err := compileMatchers(matchers)
	if err != nil {
		return nil, err
	}
	window := s.headView().labelWindow(start, end)
	id, known := labels.Lookup(name)
	perShard := make([]map[string]struct{}, len(s.shards))
	s.perShardWithColdParallel(func(shard int, t *tenant, read *shardRead) {
		size := 0
		if len(compiled) == 0 && known && int(id) < len(t.series.postings) {
			// Without matchers the shard has about as many values as postings of the label, which growing the set
			// to would rehash it each time it doubles; a limited lookup has as many as its limit.
			size = len(t.series.postings[id])
			if limit > 0 {
				size = min(size, limit)
			}
		}
		values := make(map[string]struct{}, size)
		perShard[shard] = values
		limit := valueLimit(limit)
		for read.again() {
			if cached, ok := s.headLabelValues(t, name, id, known, compiled, window, limit); ok {
				for value := range cached {
					if limit.reached(values) {
						break
					}
					values[value] = struct{}{}
				}
			} else if known && !t.series.labelValuesByPostings(id, compiled, window.includes, limitedPauser{read, values, limit}, values) {
				// A series' value is most often the previous series' value, which is already in the set.
				previous := ""
				t.series.matchingPaused(compiled, read, func(entry *seriesEntry) bool {
					if limit.reached(values) || !read.pause() {
						return false
					}
					// The value first: when it is known, the series adds nothing whether the window sees it or not.
					if value, ok := labelValue(entry.labels, id); ok && value != previous {
						if _, seen := values[value]; !seen && window.includes(entry) {
							values[value] = struct{}{}
							previous = value
						}
					}
					return true
				})
			}
		}
		read.headDone()
		if !limit.reached(values) {
			window.coldLabelValues(read.cold, s.tenantID, name, compiled, values, limit)
		}
	})
	if labelValuesHook != nil {
		labelValuesHook(perShard)
	}
	return sortedKeys(mergeSets(perShard)), nil
}

// headLabelNames returns the names of a shard's series without visiting them, when no matcher
// narrows them and the window sees all of them.
func (s *Store) headLabelNames(t *tenant, matchers []compiledMatcher, window labelWindow) ([]string, bool) {
	if len(matchers) > 0 || !window.head {
		return nil, false
	}
	return t.series.headLabelNames(s.evictEpoch.Load())
}

// headLabelValues is headLabelNames for a label's values.
func (s *Store) headLabelValues(t *tenant, name string, id uint32, known bool, matchers []compiledMatcher, window labelWindow, limit valueLimit) (map[string]struct{}, bool) {
	if len(matchers) > 0 || !window.head {
		return nil, false
	}
	if name == metricNameLabel {
		if _, ok := t.series.headLabelNames(s.evictEpoch.Load()); !ok {
			return nil, false
		}
		names := map[string]struct{}{}
		for groupID := range t.series.groups {
			if limit.reached(names) {
				break
			}
			if len(t.series.groups[groupID].entries) > 0 {
				names[t.series.groupNames[groupID]] = struct{}{}
			}
		}
		return names, true
	}
	if !known {
		return map[string]struct{}{}, true
	}
	return t.series.headLabelValues(id, s.evictEpoch.Load())
}

// mergeSets returns the union of sets, reusing the largest.
func mergeSets(sets []map[string]struct{}) map[string]struct{} {
	largest := 0
	for index, set := range sets {
		if len(set) > len(sets[largest]) {
			largest = index
		}
	}
	merged := sets[largest]
	if merged == nil {
		return map[string]struct{}{}
	}
	for index, set := range sets {
		if index != largest {
			for value := range set {
				merged[value] = struct{}{}
			}
		}
	}
	return merged
}

// forEachLabelID calls visit with the name id of each of the stored labels.
func forEachLabelID(stored labels.Labels, visit func(id uint32)) {
	stored.RangeIDs(func(id uint32, _ string) bool { visit(id); return true })
}

// labelValue returns the value of the label with name id, and whether the labels have it.
func labelValue(stored labels.Labels, id uint32) (string, bool) {
	return stored.Lookup(id)
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

// NumSeries returns the tenant's series in memory.
func (s *Store) NumSeries() uint64 {
	var total uint64
	for _, shard := range s.shards {
		shard.RLock()
		if t := shard.tenant.Load(); t != nil {
			total += uint64(t.series.len)
		}
		shard.RUnlock()
	}
	return total
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
	start, equal := labels.SharedPairs(a, b)
	if equal {
		return 0
	}
	x, y := string(a)[start:], string(b)[start:]
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
		xValue, yValue := labels.TakeValue(xName, &x), labels.TakeValue(yName, &y)
		if c := cmpStrings(xValue, yValue); c != 0 {
			return c
		}
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
