// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/chunk/chunk.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package chunk

import (
	"io"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

const (
	// ChunkLen is the length of a chunk in bytes.
	ChunkLen = 1024
)

// EncodedChunk is the interface for encoded chunks. Chunks are generally not
// goroutine-safe.
type EncodedChunk interface {
	// Add adds a SamplePair to the chunks, performs any necessary
	// re-encoding, and creates any necessary overflow chunk.
	// The returned EncodedChunk is the overflow chunk if it was created.
	// The returned EncodedChunk is nil if the sample got appended to the same chunk.
	Add(sample model.SamplePair) (EncodedChunk, error)
	// Add adds a histogram to the chunks, performs any necessary
	// re-encoding, and creates any necessary overflow chunk.
	// The returned EncodedChunk is the overflow chunk if it was created.
	// The returned EncodedChunk is nil if the histogram got appended to the same chunk.
	AddHistogram(timestamp int64, histogram *histogram.Histogram) (EncodedChunk, error)

	// NewIterator returns an iterator for the chunks.
	// The iterator passed as argument is for re-use. Depending on implementation,
	// the iterator can be re-used or a new iterator can be allocated.
	NewIterator(Iterator) Iterator
	Marshal(io.Writer) error
	UnmarshalFromBuf([]byte) error
	Encoding() Encoding

	// Len returns the number of samples in the chunk.  Implementations may be
	// expensive.
	Len() int
}

// Iterator enables efficient access to the content of a chunk. It is
// generally not safe to use an Iterator concurrently with or after chunk
// mutation.
type Iterator interface {
	// Scans the next value in the chunk. Directly after the iterator has
	// been created, the next value is the first value in the
	// chunk. Otherwise, it is the value following the last value scanned or
	// found (by one of the Find... methods). Returns chunkenc.ValNone if either the
	// end of the chunk is reached or an error has occurred.
	Scan() chunkenc.ValueType
	// Finds the oldest value at or after the provided time. Returns chunkenc.ValNone
	// if either the chunk contains no value at or after the provided time,
	// or an error has occurred.
	FindAtOrAfter(model.Time) chunkenc.ValueType
	// Returns the last value scanned (by the scan method) or found (by one
	// of the find... methods). It returns model.ZeroSamplePair before any of
	// those methods were called.
	Value() model.SamplePair
	// Returns a batch of the provisded size; NB not idempotent!  Should only be called
	// once per Scan.
	Batch(size int, valueType chunkenc.ValueType) Batch
	// Returns the last error encountered. In general, an error signals data
	// corruption in the chunk and requires quarantining.
	Err() error
}

// BatchSize is samples per batch; this was choose by benchmarking all sizes from
// 1 to 128.
const BatchSize = 12

// Batch is a sorted set of (timestamp, value) pairs.  They are intended to be
// small, and passed by value.
type Batch struct {
	Timestamps [BatchSize]int64

	ValueTypes           chunkenc.ValueType
	SampleValues         *[BatchSize]float64
	HistogramValues      *[BatchSize]*histogram.Histogram
	FloatHistogramValues *[BatchSize]*histogram.FloatHistogram

	Index  int
	Length int
}

// Chunk contains encoded timeseries data
type Chunk struct {
	From    model.Time    `json:"from"`
	Through model.Time    `json:"through"`
	Metric  labels.Labels `json:"metric"`
	Data    EncodedChunk  `json:"-"`
}

// NewChunk creates a new chunk
func NewChunk(metric labels.Labels, c EncodedChunk, from, through model.Time) Chunk {
	return Chunk{
		From:    from,
		Through: through,
		Metric:  metric,
		Data:    c,
	}
}

// Samples returns all SamplePairs for the chunk.
func (c *Chunk) Samples(from, through model.Time) ([]model.SamplePair, error) {
	it := c.Data.NewIterator(nil)
	return rangeValues(it, from, through)
}

// rangeValues is a utility function that retrieves all values within the given
// range from an Iterator.
func rangeValues(it Iterator, oldestInclusive, newestInclusive model.Time) ([]model.SamplePair, error) {
	result := []model.SamplePair{}
	if it.FindAtOrAfter(oldestInclusive) == chunkenc.ValNone {
		return result, it.Err()
	}
	for !it.Value().Timestamp.After(newestInclusive) {
		result = append(result, it.Value())
		if it.Scan() == chunkenc.ValNone {
			break
		}
	}
	return result, it.Err()
}
