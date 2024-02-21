// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/chunk/chunk.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package chunk

import (
	"fmt"
	"io"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/grafana/mimir/pkg/mimirpb"
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
	// AddHistogram adds a histogram to the chunks, performs any necessary
	// re-encoding, and creates any necessary overflow chunk.
	// The returned EncodedChunk is the overflow chunk if it was created.
	// The returned EncodedChunk is nil if the histogram got appended to the same chunk.
	AddHistogram(timestamp int64, histogram *histogram.Histogram) (EncodedChunk, error)

	// AddFloatHistogram adds a float histogram to the chunks, performs any necessary
	// re-encoding, and creates any necessary overflow chunk.
	// The returned EncodedChunk is the overflow chunk if it was created.
	// The returned EncodedChunk is nil if the histogram got appended to the same chunk.
	AddFloatHistogram(timestamp int64, histogram *histogram.FloatHistogram) (EncodedChunk, error)

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
	AtHistogram(*histogram.Histogram) (int64, *histogram.Histogram)
	AtFloatHistogram(*histogram.FloatHistogram) (int64, *histogram.FloatHistogram)
	Timestamp() int64
	// Batch returns a batch of the provisded size; NB not idempotent!  Should only be called
	// once per Scan.
	// When the optional *Batch argument is passed, it will be replaced with the content
	// of the new batch. Depending on the implementation, the optional *Batch argument can
	// also be used to optimize memory allocations. For example, if it already contains
	// pointers to objects of type histogram.Histogram or histogram.FloatHistogram,
	// the latter can be reused for the object of the new Batch.
	Batch(size int, valueType chunkenc.ValueType, b *Batch) *Batch
	// Returns the last error encountered. In general, an error signals data
	// corruption in the chunk and requires quarantining.
	Err() error
}

// BatchSize is samples per batch; this was choose by benchmarking all sizes from
// 1 to 128.
const BatchSize = 12

// Batches are sorted sets of (timestamp, value) pairs, where all values are of the same type (i.e. floats/histograms).
//
// Batch is intended to be small, and passed by value!
type Batch struct {
	Timestamps [BatchSize]int64
	// Values stores float values related to this batch if ValueType is chunkenc.ValFloat.
	Values [BatchSize]float64
	// Histograms stores histogram values related to this batch if ValueType is chunkenc.ValHistogram.
	Histograms [BatchSize]*histogram.Histogram
	// Histograms stores float histogram values related to this batch if ValueType is chunkenc.ValFloatHistogram.
	FloatHistograms [BatchSize]*histogram.FloatHistogram
	ValueType       chunkenc.ValueType
	Index           int
	Length          int
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

// Samples returns all SamplePairs and Histograms for the chunk.
func (c *Chunk) Samples(from, through model.Time) ([]model.SamplePair, []mimirpb.Histogram, error) {
	it := c.Data.NewIterator(nil)
	return rangeValues(it, from, through)
}

// rangeValues is a utility function that retrieves all values within the given
// range from an Iterator.
func rangeValues(it Iterator, oldestInclusive, newestInclusive model.Time) ([]model.SamplePair, []mimirpb.Histogram, error) {
	resultFloat := []model.SamplePair{}
	resultHist := []mimirpb.Histogram{}
	currValType := it.FindAtOrAfter(oldestInclusive)
	if currValType == chunkenc.ValNone {
		return resultFloat, resultHist, it.Err()
	}
	for !model.Time(it.Timestamp()).After(newestInclusive) {
		switch currValType {
		case chunkenc.ValFloat:
			resultFloat = append(resultFloat, it.Value())
		case chunkenc.ValHistogram:
			t, h := it.AtHistogram(nil) // Nil argument as we pass the data to the protobuf as-is without copy.
			resultHist = append(resultHist, mimirpb.FromHistogramToHistogramProto(t, h))
		case chunkenc.ValFloatHistogram:
			t, h := it.AtFloatHistogram(nil) // Nil argument as we pass the data to the protobuf as-is without copy.
			resultHist = append(resultHist, mimirpb.FromFloatHistogramToHistogramProto(t, h))
		default:
			return nil, nil, fmt.Errorf("unknown value type %v in iterator", currValType)
		}
		currValType = it.Scan()
		if currValType == chunkenc.ValNone {
			break
		}
	}
	return resultFloat, resultHist, it.Err()
}
