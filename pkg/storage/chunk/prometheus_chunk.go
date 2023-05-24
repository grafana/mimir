// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/chunk/encoding/prometheus_chunk.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package chunk

import (
	"fmt"
	"io"
	"unsafe"

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

func (p *prometheusXorChunk) AddHistogram(_ int64, _ *histogram.Histogram) (EncodedChunk, error) {
	return nil, fmt.Errorf("cannot add histogram to sample chunk")
}

func (p *prometheusXorChunk) AddFloatHistogram(_ int64, _ *histogram.FloatHistogram) (EncodedChunk, error) {
	return nil, fmt.Errorf("cannot add float histogram to sample chunk")
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

func (p *prometheusHistogramChunk) Add(_ model.SamplePair) (EncodedChunk, error) {
	return nil, fmt.Errorf("cannot add float sample to histogram chunk")
}

func (p *prometheusHistogramChunk) AddFloatHistogram(_ int64, _ *histogram.FloatHistogram) (EncodedChunk, error) {
	return nil, fmt.Errorf("cannot add float histogram to histogram chunk")
}

// AddHistogram adds another histogram to the chunk. While AddHistogram works, it is only implemented to make tests
// work, and should not be used in production. In particular, it appends all histograms to single chunk, and uses new
// Appender for each invocation.
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

// Wrapper around a Prometheus histogram chunk.
type prometheusFloatHistogramChunk struct {
	prometheusChunk
}

func newPrometheusFloatHistogramChunk() *prometheusFloatHistogramChunk {
	return &prometheusFloatHistogramChunk{}
}

func (p *prometheusFloatHistogramChunk) Add(_ model.SamplePair) (EncodedChunk, error) {
	return nil, fmt.Errorf("cannot add float sample to histogram chunk")
}

func (p *prometheusFloatHistogramChunk) AddHistogram(_ int64, _ *histogram.Histogram) (EncodedChunk, error) {
	return nil, fmt.Errorf("cannot add histogram sample to float histogram chunk")
}

// AddFloatHistogram adds another float histogram to the chunk. While AddFloatHistogram works, it is only implemented to make tests
// work, and should not be used in production. In particular, it appends all histograms to single chunk, and uses new
// Appender for each invocation.
func (p *prometheusFloatHistogramChunk) AddFloatHistogram(timestamp int64, h *histogram.FloatHistogram) (EncodedChunk, error) {
	if p.chunk == nil {
		p.chunk = chunkenc.NewFloatHistogramChunk()
	}

	app, err := p.chunk.Appender()
	if err != nil {
		return nil, err
	}

	app.AppendFloatHistogram(timestamp, h)
	return nil, nil
}

func (p *prometheusFloatHistogramChunk) UnmarshalFromBuf(bytes []byte) error {
	c, err := chunkenc.FromData(chunkenc.EncFloatHistogram, bytes)
	if err != nil {
		return errors.Wrap(err, "failed to create Prometheus chunk from bytes")
	}

	p.chunk = c
	return nil
}

func (p *prometheusFloatHistogramChunk) Encoding() Encoding {
	return PrometheusFloatHistogramChunk
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

func (p *prometheusChunkIterator) AtHistogram() (int64, *histogram.Histogram) {
	return p.it.AtHistogram()
}

func (p *prometheusChunkIterator) AtFloatHistogram() (int64, *histogram.FloatHistogram) {
	return p.it.AtFloatHistogram()
}

func (p *prometheusChunkIterator) Timestamp() int64 {
	return p.it.AtT()
}

func (p *prometheusChunkIterator) Batch(size int, valueType chunkenc.ValueType) Batch {
	var batch Batch
	batch.ValueType = valueType
	var populate func(j int)
	switch valueType {
	case chunkenc.ValNone:
		// Here in case we will introduce a linter that checks that all possible types are covered
		return batch
	case chunkenc.ValFloat:
		populate = func(j int) {
			t, v := p.it.At()
			batch.Timestamps[j] = t
			batch.Values[j] = v
		}
	case chunkenc.ValHistogram:
		populate = func(j int) {
			t, h := p.it.AtHistogram()
			batch.Timestamps[j] = t
			batch.PointerValues[j] = unsafe.Pointer(h)
		}
	case chunkenc.ValFloatHistogram:
		populate = func(j int) {
			t, fh := p.it.AtFloatHistogram()
			batch.Timestamps[j] = t
			batch.PointerValues[j] = unsafe.Pointer(fh)
		}
	default:
		panic(fmt.Sprintf("invalid chunk encoding %v", valueType))
	}

	j := 0
	for j < size {
		populate(j)
		j++
		if j < size {
			vt := p.it.Next()
			if vt == chunkenc.ValNone {
				break
			}
			if vt != valueType {
				panic(fmt.Sprintf("chunk encoding expected to be consistent in chunk start %v now %v", valueType, vt))
			}
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

func (e errorIterator) Scan() chunkenc.ValueType                      { return chunkenc.ValNone }
func (e errorIterator) FindAtOrAfter(_ model.Time) chunkenc.ValueType { return chunkenc.ValNone }
func (e errorIterator) Value() model.SamplePair                       { panic("no values") }
func (e errorIterator) AtHistogram() (int64, *histogram.Histogram)    { panic("no integer histograms") }
func (e errorIterator) AtFloatHistogram() (int64, *histogram.FloatHistogram) {
	panic("no float histograms")
}
func (e errorIterator) Timestamp() int64                        { panic("no samples") }
func (e errorIterator) Batch(_ int, _ chunkenc.ValueType) Batch { panic("no values") }
func (e errorIterator) Err() error                              { return errors.New(string(e)) }
