// SPDX-License-Identifier: AGPL-3.0-only

package chunks

import (
	"cmp"
	"fmt"
	"slices"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/grafana/mimir/pkg/mimirpb"
)

// Serving overlapping in-order and out-of-order chunks the way the Go ingester does with an
// out-of-order window: overlapping chunks are grouped like Prometheus's getOOOSeriesChunks, merged
// with Prometheus's own chainSampleIterator (whose heap decides which sample wins a timestamp tie),
// and re-encoded like populateChunksFromIterable.

// Chunk is an encoded chunk: min time, max time, wire encoding and bytes.
type Chunk struct {
	MinTime  int64
	MaxTime  int64
	Encoding int32
	Data     []byte
}

// Sample is a float sample, or a histogram one when H is set.
type Sample struct {
	T int64
	F float64
	H *mimirpb.Histogram
}

// Candidate is a chunk available to a query. Chunks with the same min time sort like Prometheus's
// head chunk references: in-order chunks before out-of-order ones, each by position.
type Candidate struct {
	Chunk      Chunk
	OutOfOrder bool
	Index      int
}

func chunkencEncoding(encoding int32) (chunkenc.Encoding, error) {
	switch encoding {
	case EncodingXOR:
		return chunkenc.EncXOR, nil
	case EncodingHistogram:
		return chunkenc.EncHistogram, nil
	case EncodingFloatHistogram:
		return chunkenc.EncFloatHistogram, nil
	default:
		return chunkenc.EncNone, fmt.Errorf("unknown chunk encoding %d", encoding)
	}
}

// Decode returns a chunk's samples.
func Decode(encoding int32, data []byte) ([]Sample, error) {
	if encoding == EncodingXOR {
		floats, err := DecodeXOR(data)
		if err != nil {
			return nil, err
		}
		samples := make([]Sample, len(floats))
		for index, sample := range floats {
			samples[index] = Sample{T: sample.T, F: sample.V}
		}
		return samples, nil
	}
	histograms, err := DecodeHistograms(encoding, data)
	if err != nil {
		return nil, err
	}
	samples := make([]Sample, len(histograms))
	for index := range histograms {
		samples[index] = Sample{T: histograms[index].Timestamp, H: &histograms[index]}
	}
	return samples, nil
}

// EncodeOutOfOrder encodes an out-of-order head's samples, sorted by timestamp, like
// OOOChunk.ToEncodedChunks: a new chunk per value type change, and where Prometheus's histogram
// appender starts one.
func EncodeOutOfOrder(samples []Sample) ([]Chunk, error) {
	var (
		chunks       = make([]Chunk, 0, 1)
		chunk        chunkenc.Chunk
		app          chunkenc.Appender
		cmint, cmaxt int64
		prevEncoding = chunkenc.EncNone
	)
	finish := func() {
		chunks = append(chunks, Chunk{MinTime: cmint, MaxTime: cmaxt, Encoding: wireEncoding(chunk.Encoding()), Data: chunk.Bytes()})
	}
	for _, sample := range samples {
		encoding := chunkenc.EncXOR
		if sample.H != nil {
			encoding = chunkenc.EncHistogram
			if sample.H.IsFloatHistogram() {
				encoding = chunkenc.EncFloatHistogram
			}
		}
		prevApp := app
		if encoding != prevEncoding {
			if prevEncoding != chunkenc.EncNone {
				finish()
			}
			cmint = sample.T
			var err error
			if chunk, err = chunkenc.NewEmptyChunk(encoding); err != nil {
				return chunks, err
			}
			if app, err = chunk.Appender(); err != nil {
				return chunks, err
			}
		}
		var (
			newChunk chunkenc.Chunk
			recoded  bool
		)
		switch encoding {
		case chunkenc.EncXOR:
			app.Append(0, sample.T, sample.F)
		case chunkenc.EncHistogram:
			newChunk, recoded, app, _ = app.AppendHistogram(prevApp, 0, sample.T, intModel(sample.H), false)
		case chunkenc.EncFloatHistogram:
			newChunk, recoded, app, _ = app.AppendFloatHistogram(prevApp, 0, sample.T, floatModel(sample.H), false)
		}
		if newChunk != nil {
			if !recoded {
				finish()
				cmint = sample.T
			}
			chunk = newChunk
		}
		cmaxt = sample.T
		prevEncoding = encoding
	}
	if prevEncoding != chunkenc.EncNone {
		finish()
	}
	return chunks, nil
}

// MergeOverlapping groups overlapping chunks and merges each group into re-encoded chunks; a chunk
// that overlaps no other is returned unchanged.
func MergeOverlapping(candidates []Candidate) ([]Chunk, error) {
	slices.SortStableFunc(candidates, func(a, b Candidate) int {
		if c := cmp.Compare(a.Chunk.MinTime, b.Chunk.MinTime); c != 0 {
			return c
		}
		if a.OutOfOrder != b.OutOfOrder {
			if a.OutOfOrder {
				return 1
			}
			return -1
		}
		return cmp.Compare(a.Index, b.Index)
	})
	var (
		output   []Chunk
		group    []Chunk
		groupMax int64
	)
	flush := func() error {
		switch len(group) {
		case 0:
		case 1:
			output = append(output, group[0])
		default:
			merged, err := mergeGroup(group)
			if err != nil {
				return err
			}
			output = append(output, merged...)
		}
		group = group[:0]
		return nil
	}
	for _, candidate := range candidates {
		if len(group) > 0 && candidate.Chunk.MinTime > groupMax {
			if err := flush(); err != nil {
				return nil, err
			}
		}
		if len(group) == 0 {
			groupMax = candidate.Chunk.MaxTime
		} else {
			groupMax = max(groupMax, candidate.Chunk.MaxTime)
		}
		group = append(group, candidate.Chunk)
	}
	if err := flush(); err != nil {
		return nil, err
	}
	return output, nil
}

// mergeGroup merges overlapping chunks through Prometheus's chain iterator and re-encodes them like
// populateChunksFromIterable: a new chunk on a value type change, when the current one is full,
// and where Prometheus's histogram appender starts one.
func mergeGroup(group []Chunk) ([]Chunk, error) {
	iterators := make([]chunkenc.Iterator, len(group))
	for index, chunk := range group {
		encoding, err := chunkencEncoding(chunk.Encoding)
		if err != nil {
			return nil, err
		}
		decoded, err := chunkenc.FromData(encoding, chunk.Data)
		if err != nil {
			return nil, err
		}
		iterators[index] = decoded.Iterator(nil)
	}
	chain := storage.ChainSampleIteratorFromIterators(nil, iterators)

	var (
		output        []Chunk
		floats        XORAppender
		current       chunkenc.Chunk
		app           chunkenc.Appender
		cmint, cmaxt  int64
		prevValueType = chunkenc.ValNone
	)
	finish := func() {
		switch prevValueType {
		case chunkenc.ValFloat:
			output = append(output, Chunk{MinTime: cmint, MaxTime: cmaxt, Encoding: EncodingXOR, Data: floats.Bytes()})
		case chunkenc.ValHistogram, chunkenc.ValFloatHistogram:
			output = append(output, Chunk{MinTime: cmint, MaxTime: cmaxt, Encoding: wireEncoding(current.Encoding()), Data: current.Bytes()})
		}
	}
	for valueType := chain.Next(); valueType != chunkenc.ValNone; valueType = chain.Next() {
		cut := valueType != prevValueType
		if !cut {
			switch valueType {
			case chunkenc.ValFloat:
				// Checked before appending, like the head, so a chunk stays within
				// chunkenc.MaxBytesPerXORChunk.
				cut = len(floats.Bytes()) > chunkenc.MaxBytesPerXORChunkBeforeAppend
			case chunkenc.ValHistogram, chunkenc.ValFloatHistogram:
				cut = len(current.Bytes()) > chunkenc.TargetBytesPerHistogramChunk &&
					current.NumSamples() > chunkenc.MinSamplesPerHistogramChunk
			}
		}
		if cut {
			finish()
			cmint = chain.AtT()
			if valueType == chunkenc.ValFloat {
				floats = NewXORAppender()
			} else {
				var err error
				if current, err = valueType.NewChunk(false, false); err != nil {
					return nil, err
				}
				if app, err = current.Appender(); err != nil {
					return nil, err
				}
			}
		}
		var (
			t        int64
			newChunk chunkenc.Chunk
			recoded  bool
			err      error
		)
		switch valueType {
		case chunkenc.ValFloat:
			var v float64
			t, v = chain.At()
			floats.Append(t, v)
		case chunkenc.ValHistogram:
			var h *histogram.Histogram
			t, h = chain.AtHistogram(nil)
			newChunk, recoded, app, err = app.AppendHistogram(nil, 0, t, h, false)
		case chunkenc.ValFloatHistogram:
			var h *histogram.FloatHistogram
			t, h = chain.AtFloatHistogram(nil)
			newChunk, recoded, app, err = app.AppendFloatHistogram(nil, 0, t, h, false)
		}
		if err != nil {
			return nil, fmt.Errorf("merge chunks: %w", err)
		}
		if newChunk != nil {
			if !recoded {
				finish()
				cmint = t
			}
			current = newChunk
		}
		cmaxt = t
		prevValueType = valueType
	}
	if err := chain.Err(); err != nil {
		return nil, fmt.Errorf("merge chunks: %w", err)
	}
	finish()
	return output, nil
}

func wireEncoding(encoding chunkenc.Encoding) int32 {
	switch encoding {
	case chunkenc.EncHistogram:
		return EncodingHistogram
	case chunkenc.EncFloatHistogram:
		return EncodingFloatHistogram
	default:
		return EncodingXOR
	}
}

// Copied: chunkenc widens the layout of the histogram it's given in place, and the samples stay
// in the out-of-order head.
func intModel(h *mimirpb.Histogram) *histogram.Histogram {
	return mimirpb.FromHistogramProtoToHistogram(h).Copy()
}

func floatModel(h *mimirpb.Histogram) *histogram.FloatHistogram {
	return mimirpb.FromFloatHistogramProtoToFloatHistogram(h).Copy()
}
