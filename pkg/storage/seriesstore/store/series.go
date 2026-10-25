// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"math/bits"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/seriesstore/chunks"
)

// The wire encoding of float chunks.
const xorEncoding = uint8(chunks.EncodingXOR)

// defaultChunkRangeMs is the chunk range of an engine opened without one: a series' chunks end at its boundaries, as a
// head's blocks do.
const defaultChunkRangeMs = int64(2 * 60 * 60 * 1000)

const (
	samplesPerChunk              = 120
	outOfOrderCapacity           = 32
	targetBytesPerHistogramChunk = 1024
	minSamplesPerHistogramChunk  = 10
	// Like Prometheus's `MaxBytesPerXORChunkBeforeAppend`: past this, the head and merged chunks
	// start a new chunk, so no chunk exceeds 1024 bytes whatever its samples.
	maxXORBytesBeforeAppend = 1024 - 19
)

// floatHead is a series' open float chunk.
type floatHead struct {
	appender chunks.XORAppender
	minTime  int64
	nextAt   int64
}

func (h *floatHead) lastTimestamp() int64 {
	timestamp, _ := h.appender.LastTimestamp()
	return timestamp
}

// oooSample is a sample of the open out-of-order chunk: a float, or a histogram when h is set.
type oooSample = chunks.Sample

// Series keeps only its open chunks on the heap; completed chunks are referenced in the chunk
// files.
type Series struct {
	// The engine's series reference, never reused; 0 outside an engine.
	ref    uint64
	chunks chunkList
	// The last chunk's reference and min time, which the next chunk's deltas are from, for the
	// chunk list ending at chunksTailEnd: dense histogram series cut a chunk every few samples,
	// and finding the last chunk otherwise means decoding the whole list on every cut.
	chunksTailRef, chunksTailMinTime int64
	floatHead                        *floatHead
	// What few series have, off the series: most are floats without out-of-order samples, and the
	// slot is paid for by every series.
	extra          *seriesExtra
	lastIngestedMs int64
	// Mimir's `ShardByAllLabels`, which decides which partition owns the series.
	ownedHash       uint32
	lastBucketCount uint32
	// The custom trackers the series matches, as a bit for each of the first 32, computed for the tenant's tracker
	// generation: most series match some, and an allocation each costs more than the slot's other fields.
	trackerBits       uint32
	trackerGeneration uint32
	// Which chunk list's end chunksTailRef and chunksTailMinTime are for. Here, not by them, for the padding.
	chunksTailEnd int32
	// When an owned series recompute first found it non-owned, in Unix seconds, or 0.
	nonOwnedSinceS uint32
	// The series' native metric metadata, 8 bytes in every build.
	seriesMetadata
	// The series' native resource's current run.
	seriesResource
	// Whether the last request's samples ended with a native histogram, as Mimir's active series
	// tracker records it.
	nativeHistogram bool
	// Whether the last request that had samples for the series came by OTLP, as Mimir's active series tracker records it.
	otlp bool
	// Whether the series is in the emulated Go head window, as of the last head tick.
	inHead bool
	// Whether Mimir's owned series recompute cleared the tenant's active series (it owned no
	// ranges) since this series' last sample. Unlike deleting one series, clearing leaves the cost
	// attribution counts in place.
	activeCleared bool
	// Whether the cost attribution tracker counts the series as active, which Engine.ActiveSeries keeps up to date.
	counted bool
	// Evicted from the emulated head as non-owned, like Mimir's early compaction of non-owned
	// series, until its next sample; its data stays queryable as a compacted block's.
	headEvicted bool
	// Whether out-of-order samples were written out to chunks since the out-of-order compaction last went over the
	// series, so a pass that doesn't hold the shard locked all along still finds what was written after it looked.
	oooChunked bool
	// Go's `labels.StableHash`, by which sharded queries pick series. Kept like Go's head keeps it:
	// rehashing every series' labels for each query shard was a fifth of a busy ingester's CPU.
	shardHash uint64
}

// seriesExtra is the state of a series that has histograms, out-of-order samples or custom tracker matches.
type seriesExtra struct {
	// The open histogram chunk, kept encoded like Prometheus's head chunk.
	histogramHead   *chunks.HistogramAppender
	histogramNextAt int64
	// Whether histogramNextAt was already estimated from the chunk's fill rate.
	histogramEndComputed bool
	// The open out-of-order chunk, like Prometheus's OOO head chunk, sorted by timestamp.
	outOfOrder []oooSample
	// The native histogram buckets the cost attribution tracker counted the series with, plus one: zero is none.
	countedBuckets int32
	// How many of the out-of-order samples are histograms, which a query merges as they are when none is.
	oooHistograms int32
}

func (s *Series) histogram() *chunks.HistogramAppender {
	if s.extra == nil {
		return nil
	}
	return s.extra.histogramHead
}

func (s *Series) setHistogram(head *chunks.HistogramAppender) {
	if s.extra == nil {
		if head == nil {
			return
		}
		s.extra = &seriesExtra{}
	}
	s.extra.histogramHead = head
	s.dropEmptyExtra()
}

func (s *Series) histogramNextAtValue() int64 {
	if s.extra == nil {
		return 0
	}
	return s.extra.histogramNextAt
}

func (s *Series) setHistogramNextAt(at int64) {
	if s.extra == nil {
		if at == 0 {
			return
		}
		s.extra = &seriesExtra{}
	}
	s.extra.histogramNextAt = at
}

func (s *Series) histogramEndComputedValue() bool {
	return s.extra != nil && s.extra.histogramEndComputed
}

func (s *Series) setHistogramEndComputed(computed bool) {
	if s.extra == nil {
		if !computed {
			return
		}
		s.extra = &seriesExtra{}
	}
	s.extra.histogramEndComputed = computed
}

func (s *Series) ooo() []oooSample {
	if s.extra == nil {
		return nil
	}
	return s.extra.outOfOrder
}

func (s *Series) setOutOfOrder(samples []oooSample) {
	if s.extra == nil {
		if len(samples) == 0 {
			return
		}
		s.extra = &seriesExtra{}
	}
	s.extra.outOfOrder = samples
	s.extra.oooHistograms = 0
	for index := range samples {
		if samples[index].H != nil {
			s.extra.oooHistograms++
		}
	}
	s.dropEmptyExtra()
}

// oooFloatsOnly reports whether the out-of-order samples are all floats.
func (s *Series) oooFloatsOnly() bool {
	return s.extra == nil || s.extra.oooHistograms == 0
}

// bitTrackers is how many custom trackers a series keeps its matches of in its slot as bits. The slot's last bit says
// it holds the id of the set of the matches in trackerSets instead, for a series that matches one past them.
const (
	bitTrackers   = 31
	trackerSetBit = uint32(1) << 31
)

// trackerSets holds each set of matches of more custom trackers than a series' bits, once for every series that has
// it: series match the same few sets, and a copy each is an allocation of every one of millions of series.
var trackerSets struct {
	sync.Mutex
	ids map[string]uint32
	// Read without the lock: replaced, never changed.
	sets atomic.Pointer[[][]uint16]
}

// trackerSetID is the id of the set of matches, which is kept from then on.
func trackerSetID(matches []uint16) uint32 {
	key := unsafe.String((*byte)(unsafe.Pointer(unsafe.SliceData(matches))), len(matches)*2)
	trackerSets.Lock()
	defer trackerSets.Unlock()
	if id, ok := trackerSets.ids[key]; ok {
		return id
	}
	var sets [][]uint16
	if current := trackerSets.sets.Load(); current != nil {
		sets = *current
	}
	id := uint32(len(sets))
	next := append(sets[:len(sets):len(sets)], slices.Clone(matches))
	trackerSets.sets.Store(&next)
	if trackerSets.ids == nil {
		trackerSets.ids = map[string]uint32{}
	}
	trackerSets.ids[strings.Clone(key)] = id
	return id
}

// matchedTrackersInto appends the custom trackers the series matches to buf[:0].
func (s *Series) matchedTrackersInto(buf []uint16) []uint16 {
	buf = buf[:0]
	if s.trackerBits&trackerSetBit != 0 {
		return append(buf, (*trackerSets.sets.Load())[s.trackerBits&^trackerSetBit]...)
	}
	for bits := s.trackerBits; bits != 0; bits &= bits - 1 {
		buf = append(buf, uint16(bits2index(bits)))
	}
	return buf
}

func bits2index(b uint32) int { return bits.TrailingZeros32(b) }

func (s *Series) setMatchedTrackers(matches []uint16) {
	s.trackerBits = 0
	for _, match := range matches {
		if match >= bitTrackers {
			s.trackerBits = trackerSetBit | trackerSetID(matches)
			return
		}
		s.trackerBits |= 1 << match
	}
}

// countedBucketCount is the buckets the cost attribution tracker counted the series with, or -1 for a float.
func (s *Series) countedBucketCount() int {
	if s.extra == nil {
		return -1
	}
	return int(s.extra.countedBuckets) - 1
}

func (s *Series) setCountedBucketCount(buckets int) {
	if s.extra == nil {
		if buckets < 0 {
			return
		}
		s.extra = &seriesExtra{}
	}
	s.extra.countedBuckets = int32(max(buckets, -1) + 1)
	s.dropEmptyExtra()
}

// dropEmptyExtra frees the extra state once a series has none of it, as a histogram series that went idle.
func (s *Series) dropEmptyExtra() {
	if e := s.extra; e.histogramHead == nil && e.histogramNextAt == 0 && !e.histogramEndComputed && len(e.outOfOrder) == 0 && e.countedBuckets == 0 {
		s.extra = nil
	}
}

func (s *Series) isActive(cutoff int64) bool {
	return !s.activeCleared && s.lastIngestedMs >= cutoff
}

func (s *Series) hasSamples() bool {
	return s.floatHead != nil || s.histogram() != nil || !s.chunks.isEmpty() || len(s.ooo()) > 0
}

// maxTime is the newest in-order sample, float or histogram, like the head chunk's max time.
func (s *Series) maxTime() (int64, bool) {
	var (
		newest int64
		ok     bool
	)
	if s.floatHead != nil {
		newest, ok = s.floatHead.lastTimestamp(), true
	}
	if s.histogram() != nil {
		if last := s.histogram().Last().Timestamp; !ok || last > newest {
			newest, ok = last, true
		}
	}
	return newest, ok
}

// newest is a series' newest sample; heads hold the newest when present.
func (s *Series) newest() (int64, bool) {
	if newest, ok := s.maxTime(); ok {
		return newest, true
	}
	var (
		newest int64
		ok     bool
	)
	it := s.chunks.iter()
	for chunk, more := it.next(); more; chunk, more = it.next() {
		if !ok || chunk.MaxTime > newest {
			newest, ok = chunk.MaxTime, true
		}
	}
	if n := len(s.ooo()); n > 0 {
		if last := s.ooo()[n-1].T; !ok || last > newest {
			newest, ok = last, true
		}
	}
	return newest, ok
}

// oldest is a series' oldest sample.
func (s *Series) oldest() (int64, bool) {
	var (
		oldest int64
		ok     bool
	)
	consider := func(timestamp int64) {
		if !ok || timestamp < oldest {
			oldest, ok = timestamp, true
		}
	}
	it := s.chunks.iter()
	for chunk, more := it.next(); more; chunk, more = it.next() {
		consider(chunk.MinTime)
	}
	if s.floatHead != nil {
		consider(s.floatHead.minTime)
	}
	if s.histogram() != nil {
		consider(s.histogram().FirstTimestamp())
	}
	if len(s.ooo()) > 0 {
		consider(s.ooo()[0].T)
	}
	return oldest, ok
}

// headBounds calls visit with the bounds of the series' open chunks.
func (s *Series) headBounds(visit func(minTime, maxTime int64) bool) bool {
	if s.floatHead != nil && visit(s.floatHead.minTime, s.floatHead.lastTimestamp()) {
		return true
	}
	if s.histogram() != nil && visit(s.histogram().FirstTimestamp(), s.histogram().Last().Timestamp) {
		return true
	}
	if n := len(s.ooo()); n > 0 && visit(s.ooo()[0].T, s.ooo()[n-1].T) {
		return true
	}
	return false
}

// hasDataIn reports whether any chunk, completed or open, overlaps [start, end].
func (s *Series) hasDataIn(start, end int64) bool {
	it := s.chunks.iter()
	for chunk, more := it.next(); more; chunk, more = it.next() {
		if chunk.MinTime <= end && chunk.MaxTime >= start {
			return true
		}
	}
	return s.headOverlaps(start, end)
}

// hasChunkIn is hasDataIn with the open out-of-order chunk split by encoding, as Prometheus's
// reads split its out-of-order head chunk: a query only sees a series through chunks overlapping
// its range, and a mixed chunk's first and last samples can straddle a range none of its parts do.
func (s *Series) hasChunkIn(start, end int64) bool {
	it := s.chunks.iter()
	for chunk, more := it.next(); more; chunk, more = it.next() {
		if chunk.MinTime <= end && chunk.MaxTime >= start {
			return true
		}
	}
	if s.floatHead != nil && s.floatHead.minTime <= end && s.floatHead.lastTimestamp() >= start {
		return true
	}
	if s.histogram() != nil && s.histogram().FirstTimestamp() <= end && s.histogram().Last().Timestamp >= start {
		return true
	}
	if n := len(s.ooo()); n > 0 && s.ooo()[0].T <= end && s.ooo()[n-1].T >= start {
		encoded, err := chunks.EncodeOutOfOrder(s.ooo())
		if err != nil {
			return true
		}
		for _, chunk := range encoded {
			if chunk.MinTime <= end && chunk.MaxTime >= start {
				return true
			}
		}
	}
	return false
}

func (s *Series) headOverlaps(start, end int64) bool {
	if s.floatHead != nil && s.floatHead.minTime <= end && s.floatHead.lastTimestamp() >= start {
		return true
	}
	if s.histogram() != nil && s.histogram().FirstTimestamp() <= end && s.histogram().Last().Timestamp >= start {
		return true
	}
	if n := len(s.ooo()); n > 0 && s.ooo()[0].T <= end && s.ooo()[n-1].T >= start {
		return true
	}
	return false
}

// histogramSamples returns the open histogram chunk's samples, the first carrying the chunk
// header as its hint, for rebuilding it.
func (s *Series) histogramSamples() ([]mimirpb.Histogram, error) {
	if s.histogram() == nil {
		return nil, nil
	}
	return s.histogram().Samples()
}

func histogramBucketCount(h *mimirpb.Histogram) uint64 {
	var count uint64
	for _, span := range h.PositiveSpans {
		count += uint64(span.Length)
	}
	for _, span := range h.NegativeSpans {
		count += uint64(span.Length)
	}
	return count
}

// setChunks replaces the series' chunk list; every assignment goes through it, so the tail kept
// for pushChunk never describes another list.
func (s *Series) setChunks(list chunkList) {
	s.chunks = list
	s.chunksTailEnd = -1
}

// pushChunk adds a chunk after the series' others.
func (s *Series) pushChunk(meta ChunkMeta) {
	if int(s.chunksTailEnd) != len(s.chunks) {
		s.chunksTailRef, s.chunksTailMinTime = s.chunks.last()
	}
	s.chunks.pushAfter(meta, s.chunksTailRef, s.chunksTailMinTime)
	s.chunksTailRef, s.chunksTailMinTime = int64(meta.Ref), meta.MinTime
	s.chunksTailEnd = int32(len(s.chunks))
}
