// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/chunk/encoding/prometheus_chunk.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package chunk

import (
	"io"

	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

// Wrapper around a generic Prometheus chunk.
type prometheusChunk struct {
	chunk chunkenc.Chunk
}

func (p *prometheusChunk) NewIterator(iterator Iterator) Iterator {
	if p.chunk == nil {
		return errorIterator("Prometheus chunk is not set")
	}

	if pit, ok := iterator.(*prometheusChunkIterator); ok {
		pit.c = p.chunk
		pit.it = p.chunk.Iterator(pit.it)
		return pit
	}

	return &prometheusChunkIterator{c: p.chunk, it: p.chunk.Iterator(nil)}
}

func (p *prometheusChunk) Marshal(i io.Writer) error {
	if p.chunk == nil {
		return errors.New("chunk data not set")
	}
	_, err := i.Write(p.chunk.Bytes())
	return err
}

func (p *prometheusChunk) Len() int {
	if p.chunk == nil {
		return 0
	}
	return p.chunk.NumSamples()
}

// Wrapper around a Prometheus XOR chunk.
type prometheusXorChunk struct {
	prometheusChunk
}

func newPrometheusXorChunk() *prometheusXorChunk {
	return &prometheusXorChunk{}
}

// Add adds another sample to the chunk. While Add works, it is only implemented
// to make tests work, and should not be used in production. In particular, it appends
// all samples to single chunk, and uses new Appender for each Add.
func (p *prometheusXorChunk) Add(m model.SamplePair) (EncodedChunk, error) {
	if p.chunk == nil {
		p.chunk = chunkenc.NewXORChunk()
	}

	app, err := p.chunk.Appender()
	if err != nil {
		return nil, err
	}

	app.Append(int64(m.Timestamp), float64(m.Value))
	return nil, nil
}

// AddHistogram adds another histogram to the chunk. While Add works, it is only implemented
// to make tests work, and should not be used in production. In particular, it appends
// all histograms to single chunk, and uses new Appender for each Add.
func (p *prometheusXorChunk) AddHistogram(timestamp int64, h *histogram.Histogram) (EncodedChunk, error) {
	panic("cannot add histogram to sample chunk")
}

func (p *prometheusXorChunk) UnmarshalFromBuf(bytes []byte) error {
	c, err := chunkenc.FromData(chunkenc.EncXOR, bytes)
	if err != nil {
		return errors.Wrap(err, "failed to create Prometheus chunk from bytes")
	}

	p.chunk = c
	return nil
}

func (p *prometheusXorChunk) Encoding() Encoding {
	return PrometheusXorChunk
}

// Wrapper around a Prometheus histogram chunk.
type prometheusHistogramChunk struct {
	prometheusChunk
}

func newPrometheusHistogramChunk() *prometheusHistogramChunk {
	return &prometheusHistogramChunk{}
}

// Add adds another sample to the chunk. While Add works, it is only implemented
// to make tests work, and should not be used in production. In particular, it appends
// all samples to single chunk, and uses new Appender for each Add.
func (p *prometheusHistogramChunk) Add(sample model.SamplePair) (EncodedChunk, error) {
	panic("cannot add sample to histogram chunk")
}

// AddHistogram adds another histogram to the chunk. While Add works, it is only implemented
// to make tests work, and should not be used in production. In particular, it appends
// all histograms to single chunk, and uses new Appender for each Add.
func (p *prometheusHistogramChunk) AddHistogram(timestamp int64, h *histogram.Histogram) (EncodedChunk, error) {
	if p.chunk == nil {
		p.chunk = chunkenc.NewHistogramChunk()
	}

	app, err := p.chunk.Appender()
	if err != nil {
		return nil, err
	}

	app.AppendHistogram(timestamp, h)
	return nil, nil
}

func (p *prometheusHistogramChunk) UnmarshalFromBuf(bytes []byte) error {
	c, err := chunkenc.FromData(chunkenc.EncHistogram, bytes)
	if err != nil {
		return errors.Wrap(err, "failed to create Prometheus chunk from bytes")
	}

	p.chunk = c
	return nil
}

func (p *prometheusHistogramChunk) Encoding() Encoding {
	return PrometheusHistogramChunk
}

type prometheusChunkIterator struct {
	c  chunkenc.Chunk // we need chunk, because FindAtOrAfter needs to start with fresh iterator.
	it chunkenc.Iterator
}

func (p *prometheusChunkIterator) Scan() chunkenc.ValueType {
	return p.it.Next()
}

func (p *prometheusChunkIterator) FindAtOrAfter(time model.Time) chunkenc.ValueType {
	// FindAtOrAfter must return OLDEST value at given time. That means we need to start with a fresh iterator,
	// otherwise we cannot guarantee OLDEST.
	p.it = p.c.Iterator(p.it)
	return p.it.Seek(int64(time))
}

func (p *prometheusChunkIterator) Value() model.SamplePair {
	ts, val := p.it.At()
	return model.SamplePair{
		Timestamp: model.Time(ts),
		Value:     model.SampleValue(val),
	}
}

func (p *prometheusChunkIterator) Batch(size int, valueType chunkenc.ValueType) Batch {
	var batch Batch
	var populate func(j int)
	switch valueType {
	case chunkenc.ValFloat:
		batch.ValueTypes = chunkenc.ValFloat
		batch.SampleValues = &[BatchSize]float64{}
		populate = func(j int) {
			t, v := p.it.At()
			batch.Timestamps[j] = t
			batch.SampleValues[j] = v
		}
	case chunkenc.ValHistogram:
		batch.ValueTypes = chunkenc.ValHistogram
		batch.HistogramValues = &[BatchSize]*histogram.Histogram{}
		populate = func(j int) {
			t, h := p.it.AtHistogram()
			batch.Timestamps[j] = t
			batch.HistogramValues[j] = h
		}
	case chunkenc.ValFloatHistogram:
		batch.ValueTypes = chunkenc.ValFloatHistogram
		batch.FloatHistogramValues = &[BatchSize]*histogram.FloatHistogram{}
		populate = func(j int) {
			t, fh := p.it.AtFloatHistogram()
			batch.Timestamps[j] = t
			batch.FloatHistogramValues[j] = fh
		}
	}

	j := 0
	for j < size {
		populate(j)
		j++
		if j < size && p.it.Next() == chunkenc.ValNone {
			break
		}
	}
	batch.Index = 0
	batch.Length = j
	return batch
}

func (p *prometheusChunkIterator) Err() error {
	return p.it.Err()
}

type errorIterator string

func (e errorIterator) Scan() chunkenc.ValueType                           { return chunkenc.ValNone }
func (e errorIterator) FindAtOrAfter(time model.Time) chunkenc.ValueType   { return chunkenc.ValNone }
func (e errorIterator) Value() model.SamplePair                            { panic("no values") }
func (e errorIterator) Batch(size int, valueType chunkenc.ValueType) Batch { panic("no values") }
func (e errorIterator) Err() error                                         { return errors.New(string(e)) }
