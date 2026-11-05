// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"fmt"
	"math"
	"slices"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/seriesstore/chunks"
)

// appended is what an append did: whether the sample went in order or to the out-of-order chunk,
// and whether it opened a chunk, for `cortex_ingester_tsdb_head_chunks_created_total`. A duplicate
// of a stored sample is accepted without storing it again.
type appended struct {
	noop       bool
	outOfOrder bool
	opened     bool
}

// appendFloatInOrder appends a sample the head appender accepted in order, cutting the open chunk
// on its size, then its estimated end time or sample count, like Prometheus's head.
func appendFloatInOrder(series *Series, disk *chunks.DiskMapper, timestamp int64, value float64, oneOpenChunk bool) (appended, error) {
	// A float after histograms starts a new chunk, as the head keeps one open chunk.
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
	if outOfOrder {
		disk.NoteOutOfOrder(ref)
	}
	series.pushChunk(ChunkMeta{Ref: ref, MinTime: minTime, MaxTime: maxTime, Len: uint32(len(data)), Encoding: uint8(encoding), OutOfOrder: outOfOrder})
	return nil
}

// detachedHistogram is a copy of h that shares no memory with it: an out-of-order histogram stays in the series as it
// is until the out-of-order head is compacted, where the one appended points into the request or the Kafka record it
// was decoded from, whose buffer the next request reuses, possibly another tenant's.
func detachedHistogram(h *mimirpb.Histogram) mimirpb.Histogram {
	copied := *h
	switch count := h.Count.(type) {
	case *mimirpb.Histogram_CountInt:
		copied.Count = &mimirpb.Histogram_CountInt{CountInt: count.CountInt}
	case *mimirpb.Histogram_CountFloat:
		copied.Count = &mimirpb.Histogram_CountFloat{CountFloat: count.CountFloat}
	}
	switch zero := h.ZeroCount.(type) {
	case *mimirpb.Histogram_ZeroCountInt:
		copied.ZeroCount = &mimirpb.Histogram_ZeroCountInt{ZeroCountInt: zero.ZeroCountInt}
	case *mimirpb.Histogram_ZeroCountFloat:
		copied.ZeroCount = &mimirpb.Histogram_ZeroCountFloat{ZeroCountFloat: zero.ZeroCountFloat}
	}
	copied.NegativeSpans = slices.Clone(h.NegativeSpans)
	copied.NegativeDeltas = slices.Clone(h.NegativeDeltas)
	copied.NegativeCounts = slices.Clone(h.NegativeCounts)
	copied.PositiveSpans = slices.Clone(h.PositiveSpans)
	copied.PositiveDeltas = slices.Clone(h.PositiveDeltas)
	copied.PositiveCounts = slices.Clone(h.PositiveCounts)
	copied.CustomValues = slices.Clone(h.CustomValues)
	return copied
}
