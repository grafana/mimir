// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"fmt"
	"math"
	"slices"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/seriesstore/chunks"
	"github.com/grafana/mimir/pkg/storage/seriesstore/labels"
	"github.com/grafana/mimir/pkg/storage/seriesstore/record"
)

// appendRules are the head state a flush's samples are checked against, taken when the flush's
// head appender is created like the one Mimir's ingest pusher creates for each flush.
type appendRules struct {
	headMaxTime  int64
	minValidTime int64
	oooWindowMs  int64
}

// newAppendRules is Prometheus's `appendableMinValidTime`: half a block range behind the head's
// max time, and never before where the last compaction truncated the head.
func newAppendRules(headMaxTime, truncatedTo, oooWindowMs int64) appendRules {
	minValid := int64(math.MinInt64)
	if headMaxTime != math.MinInt64 {
		minValid = max(satSub(headMaxTime, minValidTimeWindowMs), truncatedTo)
	}
	return appendRules{headMaxTime: headMaxTime, minValidTime: minValid, oooWindowMs: oooWindowMs}
}

type appendKind uint8

const (
	appendInOrder appendKind = iota
	appendDuplicate
	appendOutOfOrder
)

// classify is Prometheus's `appendable` for a sample at timestamp in a series whose in-order
// samples end at seriesMax (hasMax false for a series without any). ok false is a rejection.
func (r *appendRules) classify(timestamp, seriesMax int64, hasMax bool) (appendKind, DiscardReason, bool) {
	switch {
	case hasMax && timestamp > seriesMax && timestamp >= r.minValidTime:
		return appendInOrder, 0, true
	case hasMax && timestamp == seriesMax:
		return appendDuplicate, 0, true
	case !hasMax && timestamp >= r.minValidTime:
		return appendInOrder, 0, true
	}
	if r.oooWindowMs > 0 {
		if timestamp >= satSub(r.headMaxTime, r.oooWindowMs) {
			return appendOutOfOrder, 0, true
		}
		return 0, DiscardTooOld, false
	}
	if timestamp < r.minValidTime {
		return 0, DiscardOutOfBounds, false
	}
	return 0, DiscardOutOfOrder, false
}

// committedHead is a series as committed before the current flush: Prometheus's head appender
// checks samples against it, and only drops those that no longer fit when committing.
type committedHead struct {
	max           int64
	hasMax        bool
	lastFloatTime int64
	lastFloatBits uint64
	hasLastFloat  bool
	lastHistogram *mimirpb.Histogram
}

func committedOf(series *Series) committedHead {
	c := committedHead{}
	c.max, c.hasMax = series.maxTime()
	if series.floatHead != nil {
		value, _ := series.floatHead.appender.LastValue()
		c.lastFloatTime, c.lastFloatBits, c.hasLastFloat = series.floatHead.lastTimestamp(), math.Float64bits(value), true
	}
	if series.histogram() != nil {
		last := *series.histogram().Last()
		c.lastHistogram = &last
	}
	return c
}

// appended is what an append did: whether the sample went in order or to the out-of-order chunk,
// and whether it opened a chunk, for `cortex_ingester_tsdb_head_chunks_created_total`. A duplicate
// of a stored sample is accepted without storing it again.
type appended struct {
	noop       bool
	outOfOrder bool
	opened     bool
}

type seriesContext struct {
	tenantID      string
	rules         appendRules
	keepExemplars bool
	index         int
	position      int
	flush         uint64
	ingestedMs    int64
}

// ingestSeries appends decoded, whose labels are sorted and unique with hash hash.
func ingestSeries(t *tenant, disk *chunks.DiskMapper, decoded *record.DecodedSeries, hash uint64, context seriesContext, committed map[committedKey]committedHead, outcome *shardOutcome) error {
	pairs := decoded.Labels
	name := labels.Pairs(pairs).Get(metricNameLabel)
	entry, inserted := t.series.getOrInsert(name, hash, pairs, func() labels.Labels { return labels.FromSorted(pairs) })
	series := &entry.series
	if inserted {
		series.ownedHash = ShardByAllLabels(context.tenantID, entry.labels)
		series.shardHash = labels.StableHash(entry.labels)
	}
	existed := series.hasSamples()
	rules := &context.rules
	key := committedKey{context.flush, hash}
	head, ok := committed[key]
	if !ok {
		head = committedOf(series)
		committed[key] = head
	}
	// Like Mimir's active series tracker, the bucket count comes from the request's last histogram
	// when no float follows it.
	bucketCount, hasBucketCount := uint32(0), false
	if n := len(decoded.Histograms); n > 0 {
		last := &decoded.Histograms[n-1]
		if len(decoded.Samples) == 0 || decoded.Samples[len(decoded.Samples)-1].TimestampMs < last.Timestamp {
			bucketCount, hasBucketCount = uint32(histogramBucketCount(last)), true
		}
	}
	var accepted, outOfOrder, chunksCreated uint64
	count := func(result appended) {
		switch {
		case result.noop:
		case result.outOfOrder:
			outOfOrder++
			if result.opened {
				chunksCreated++
			}
		default:
			if result.opened {
				chunksCreated++
			}
		}
	}
	discard := func(reason DiscardReason) {
		outcome.discarded[discardKey{context.index, reason}]++
	}
	// Like Mimir's push path, the created timestamp adds one zero sample before the first sample
	// it precedes: a float, or a zero histogram of the same layout before a histogram.
	created := decoded.CreatedTimestamp
	createdPending := created > 0
	firstHistogram, hasFirstHistogram := int64(0), len(decoded.Histograms) > 0
	if hasFirstHistogram {
		firstHistogram = decoded.Histograms[0].Timestamp
	}
	for index := range decoded.Samples {
		sample := decoded.Samples[index]
		if createdPending && created < sample.TimestampMs && (!hasFirstHistogram || firstHistogram >= sample.TimestampMs) {
			createdPending = false
			result, reason, ok, err := appendCreatedZero(series, disk, rules, &head, oooSample{T: created}, created)
			if err != nil {
				return err
			}
			switch {
			case !ok:
				discard(reason)
			case result != nil:
				accepted++
				count(*result)
			}
		}
		result, reason, ok, err := appendFloat(series, disk, rules, &head, sample.TimestampMs, sample.Value)
		if err != nil {
			return err
		}
		if ok {
			accepted++
			count(result)
		} else {
			discard(reason)
		}
	}
	for index := range decoded.Histograms {
		h := &decoded.Histograms[index]
		if createdPending && created < h.Timestamp {
			createdPending = false
			zero := mimirpb.Histogram{
				Timestamp:     created,
				Schema:        h.Schema,
				ZeroThreshold: h.ZeroThreshold,
				CustomValues:  slices.Clone(h.CustomValues),
				ResetHint:     mimirpb.Histogram_YES,
			}
			if h.IsFloatHistogram() {
				zero.Count = &mimirpb.Histogram_CountFloat{CountFloat: 0}
				zero.ZeroCount = &mimirpb.Histogram_ZeroCountFloat{ZeroCountFloat: 0}
			} else {
				zero.Count = &mimirpb.Histogram_CountInt{CountInt: 0}
				zero.ZeroCount = &mimirpb.Histogram_ZeroCountInt{ZeroCountInt: 0}
			}
			result, reason, ok, err := appendCreatedZero(series, disk, rules, &head, oooSample{T: created, H: &zero}, created)
			if err != nil {
				return err
			}
			switch {
			case !ok:
				discard(reason)
			case result != nil:
				accepted++
				count(*result)
			}
		}
		result, reason, ok, err := appendHistogram(series, disk, rules, &head, h)
		if err != nil {
			return err
		}
		if ok {
			accepted++
			count(result)
		} else {
			discard(reason)
		}
	}
	if accepted > 0 {
		if hasBucketCount {
			series.lastBucketCount = bucketCount
		}
		series.nativeHistogram = hasBucketCount
		series.lastIngestedMs = context.ingestedMs
		series.activeCleared = false
		series.headEvicted = false
		outcome.accepted[context.index] += accepted
	}
	if outOfOrder > 0 {
		outcome.outOfOrder[context.index] += outOfOrder
	}
	if chunksCreated > 0 {
		outcome.chunksCreated[context.index] += chunksCreated
	}
	// Exemplars need an existing series, like `AppendExemplar`.
	if context.keepExemplars && len(decoded.Exemplars) > 0 {
		if !existed && accepted == 0 {
			outcome.exemplarsNoSeries += uint64(len(decoded.Exemplars))
		} else {
			outcome.exemplars = append(outcome.exemplars, pendingExemplars{
				index: context.index, position: context.position, flush: context.flush, hash: hash,
				labels: entry.labels, exemplars: decoded.Exemplars,
			})
		}
	}
	return nil
}

// appendCreatedZero appends a created-timestamp zero sample, only in order; like Mimir, being out
// of order or a duplicate is not an error, other rejections are counted. A nil result with ok
// true appended nothing.
func appendCreatedZero(series *Series, disk *chunks.DiskMapper, rules *appendRules, head *committedHead, value oooSample, timestamp int64) (*appended, DiscardReason, bool, error) {
	kind, reason, ok := rules.classify(timestamp, head.max, head.hasMax)
	switch {
	case ok && kind == appendInOrder:
	case ok:
		return nil, 0, true, nil
	case reason == DiscardOutOfOrder:
		return nil, 0, true, nil
	default:
		return nil, reason, false, nil
	}
	// Counted as ingested when appended, like a successful `AppendSTZeroSample`.
	var (
		result appended
		err    error
	)
	if value.H == nil {
		result, _, ok, err = appendFloat(series, disk, rules, head, timestamp, 0)
	} else {
		result, _, ok, err = appendHistogram(series, disk, rules, head, value.H)
	}
	if err != nil || !ok {
		return nil, 0, true, err
	}
	return &result, 0, true, nil
}

// appendFloat follows the Prometheus head appender: in-order samples extend the open chunk, a
// repeated timestamp keeps the first value, and older samples within the out-of-order window go
// to a separate out-of-order chunk. The error is a storage failure, ok false a rejection.
func appendFloat(series *Series, disk *chunks.DiskMapper, rules *appendRules, head *committedHead, timestamp int64, value float64) (appended, DiscardReason, bool, error) {
	kind, reason, ok := rules.classify(timestamp, head.max, head.hasMax)
	if !ok {
		return appended{}, reason, false, nil
	}
	if kind == appendDuplicate {
		if head.hasLastFloat && head.lastFloatTime == timestamp && head.lastFloatBits == math.Float64bits(value) {
			return appended{noop: true}, 0, true, nil
		}
		return appended{}, DiscardNewValueForTimestamp, false, nil
	}
	// Like Prometheus's commit, the accepted sample is checked again against the series as this
	// flush left it, and anything that no longer fits is dropped without an error.
	seriesMax, hasMax := series.maxTime()
	kind, _, ok = rules.classify(timestamp, seriesMax, hasMax)
	switch {
	case !ok || kind == appendDuplicate:
		return appended{noop: true}, 0, true, nil
	case kind == appendOutOfOrder:
		result, err := insertOutOfOrder(series, disk, oooSample{T: timestamp, F: value})
		return result, 0, true, err
	}
	result, err := appendFloatInOrder(series, disk, timestamp, value, false)
	if err != nil {
		return appended{}, 0, false, err
	}
	return result, 0, true, nil
}

// appendFloatInOrder appends a sample the head appender accepted in order, cutting the open chunk
// on its size, then its estimated end time or sample count, like Prometheus's head.
func appendFloatInOrder(series *Series, disk *chunks.DiskMapper, timestamp int64, value float64, oneOpenChunk bool) (appended, error) {
	// Prometheus's head has one open chunk: a float after histograms starts a new one. The Kafka
	// path keeps both open, like the Rust store it's checked against.
	if oneOpenChunk && series.histogram() != nil {
		if err := cutHistogramHead(series, disk); err != nil {
			return appended{}, err
		}
	}
	// Like Prometheus's head appender, the chunk's size is checked before its sample count.
	if series.floatHead != nil && len(series.floatHead.appender.Bytes()) > maxXORBytesBeforeAppend {
		if err := cutFloatHead(series, disk); err != nil {
			return appended{}, err
		}
	}
	if fh := series.floatHead; fh != nil {
		samples := fh.appender.Len()
		if samples == samplesPerChunk/4 {
			fh.nextAt = computeChunkEndTime(fh.minTime, fh.lastTimestamp(), fh.nextAt, 4)
		}
		if timestamp >= fh.nextAt || samples >= samplesPerChunk*2 {
			if err := cutFloatHead(series, disk); err != nil {
				return appended{}, err
			}
		}
	}
	opened := series.floatHead == nil
	if opened {
		series.floatHead = &floatHead{appender: chunks.NewXORAppender(), minTime: timestamp, nextAt: rangeEnd(timestamp)}
	}
	fh := series.floatHead
	// A float after an older float in the head can only follow a histogram at a later time, which
	// the in-order check above rules out.
	if last, ok := fh.appender.LastTimestamp(); ok && timestamp <= last {
		return appended{noop: true}, nil
	}
	fh.appender.Append(timestamp, value)
	return appended{opened: opened}, nil
}

func appendHistogram(series *Series, disk *chunks.DiskMapper, rules *appendRules, head *committedHead, h *mimirpb.Histogram) (appended, DiscardReason, bool, error) {
	// Like the head appender, invalid histograms are rejected before anything else.
	if !chunks.IsValidHistogram(h) {
		return appended{}, DiscardInvalidNativeHistogram, false, nil
	}
	kind, reason, ok := rules.classify(h.Timestamp, head.max, head.hasMax)
	if !ok {
		return appended{}, reason, false, nil
	}
	if kind == appendDuplicate {
		if head.lastHistogram != nil && head.lastHistogram.Timestamp == h.Timestamp && chunks.EqualHistogramValues(head.lastHistogram, h) {
			return appended{noop: true}, 0, true, nil
		}
		return appended{}, DiscardNewValueForTimestamp, false, nil
	}
	// Checked again when committing, like floats.
	seriesMax, hasMax := series.maxTime()
	kind, _, ok = rules.classify(h.Timestamp, seriesMax, hasMax)
	switch {
	case !ok || kind == appendDuplicate:
		return appended{noop: true}, 0, true, nil
	case kind == appendOutOfOrder:
		copied := *h
		result, err := insertOutOfOrder(series, disk, oooSample{T: h.Timestamp, H: &copied})
		return result, 0, true, err
	}
	result, err := appendHistogramInOrder(series, disk, h, false)
	if err != nil {
		return appended{}, 0, false, err
	}
	return result, 0, true, nil
}

// appendHistogramInOrder appends a histogram the head appender accepted in order.
func appendHistogramInOrder(series *Series, disk *chunks.DiskMapper, h *mimirpb.Histogram, oneOpenChunk bool) (appended, error) {
	// A histogram after floats starts a new chunk, like the head's one open chunk.
	if oneOpenChunk && series.floatHead != nil {
		if err := cutFloatHead(series, disk); err != nil {
			return appended{}, err
		}
	}
	timestamp := h.Timestamp
	// Prometheus's `histogramsAppendPreprocessor`: cut on the estimated end time or twice the
	// target size, with at least a few samples unless a new block range starts.
	if hh := series.histogram(); hh != nil {
		samples := hh.Len()
		bytes := hh.EncodedLen()
		nextRangeStart := series.histogramNextAtValue()
		if series.histogramEndComputedValue() {
			nextRangeStart = rangeEnd(hh.FirstTimestamp())
		}
		if !series.histogramEndComputedValue() && bytes >= targetBytesPerHistogramChunk/4 {
			series.setHistogramNextAt(computeChunkEndTime(hh.FirstTimestamp(), hh.Last().Timestamp, series.histogramNextAtValue(), float64(targetBytesPerHistogramChunk)/float64(bytes)))
			series.setHistogramEndComputed(true)
		}
		if (timestamp >= series.histogramNextAtValue() || bytes >= targetBytesPerHistogramChunk*2) &&
			(samples >= minSamplesPerHistogramChunk || timestamp >= nextRangeStart) {
			next := chunks.NewHistogramAppender(h, hh)
			if err := cutHistogramHead(series, disk); err != nil {
				return appended{}, err
			}
			series.setHistogram(next)
			series.setHistogramNextAt(rangeEnd(timestamp))
			series.setHistogramEndComputed(false)
			return appended{opened: true}, nil
		}
	}
	if series.histogram() == nil {
		series.setHistogram(chunks.NewHistogramAppender(h, nil))
		series.setHistogramNextAt(rangeEnd(timestamp))
		series.setHistogramEndComputed(false)
		return appended{opened: true}, nil
	}
	result, next, err := series.histogram().Append(h)
	if err != nil {
		return appended{}, err
	}
	if result != chunks.AppendedNewChunk {
		return appended{}, nil
	}
	if err := cutHistogramHead(series, disk); err != nil {
		return appended{}, err
	}
	series.setHistogram(next)
	series.setHistogramNextAt(rangeEnd(timestamp))
	series.setHistogramEndComputed(false)
	return appended{opened: true}, nil
}

func cutFloatHead(series *Series, disk *chunks.DiskMapper) error {
	fh := series.floatHead
	if fh == nil {
		return nil
	}
	series.floatHead = nil
	return writeChunk(series, disk, int32(xorEncoding), fh.appender.Bytes(), fh.minTime, fh.lastTimestamp(), false)
}

func cutHistogramHead(series *Series, disk *chunks.DiskMapper) error {
	hh := series.histogram()
	if hh == nil {
		return nil
	}
	series.setHistogram(nil)
	encoded := hh.Encoded()
	return writeChunk(series, disk, encoded.Encoding, encoded.Data, hh.FirstTimestamp(), hh.Last().Timestamp, false)
}

// insertOutOfOrder is the OOO head chunk's `Insert`: a timestamp already in the open out-of-order
// chunk is dropped; once it holds outOfOrderCapacity samples it is written out like a
// memory-mapped OOO chunk.
func insertOutOfOrder(series *Series, disk *chunks.DiskMapper, sample oooSample) (appended, error) {
	index, found := slices.BinarySearchFunc(series.ooo(), sample.T, func(stored oooSample, timestamp int64) int {
		switch {
		case stored.T < timestamp:
			return -1
		case stored.T > timestamp:
			return 1
		default:
			return 0
		}
	})
	if found {
		return appended{noop: true}, nil
	}
	series.setOutOfOrder(slices.Insert(series.ooo(), index, sample))
	opened := len(series.ooo()) == 1
	if len(series.ooo()) >= outOfOrderCapacity {
		if err := flushOutOfOrder(series, disk); err != nil {
			return appended{}, err
		}
	}
	return appended{outOfOrder: true, opened: opened}, nil
}

func flushOutOfOrder(series *Series, disk *chunks.DiskMapper) error {
	if len(series.ooo()) == 0 {
		return nil
	}
	samples := series.ooo()
	series.setOutOfOrder(nil)
	encoded, err := chunks.EncodeOutOfOrder(samples)
	if err != nil {
		return err
	}
	series.oooChunked = true
	for _, chunk := range encoded {
		if err := writeChunk(series, disk, chunk.Encoding, chunk.Data, chunk.MinTime, chunk.MaxTime, true); err != nil {
			return err
		}
	}
	return nil
}

func writeChunk(series *Series, disk *chunks.DiskMapper, encoding int32, data []byte, minTime, maxTime int64, outOfOrder bool) error {
	if len(data) > math.MaxUint32 {
		return fmt.Errorf("chunk of %d bytes exceeds u32", len(data))
	}
	if encoding < 0 || encoding > math.MaxUint8 {
		return fmt.Errorf("chunk encoding %d exceeds u8", encoding)
	}
	ref, err := disk.Write(data, maxTime)
	if err != nil {
		return err
	}
	series.pushChunk(ChunkMeta{Ref: ref, MinTime: minTime, MaxTime: maxTime, Len: uint32(len(data)), Encoding: uint8(encoding), OutOfOrder: outOfOrder})
	return nil
}
