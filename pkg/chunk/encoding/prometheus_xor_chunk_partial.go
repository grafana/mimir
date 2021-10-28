// SPDX-License-Identifier: AGPL-3.0-only

package encoding

import (
	"io"

	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

// Wrapper around chunkenc.XORChunkPartial.
type prometheusXorChunkPartial struct {
	chunk chunkenc.Chunk
}

func newPrometheusXorChunkPartial() *prometheusXorChunkPartial {
	return &prometheusXorChunkPartial{}
}

func (p *prometheusXorChunkPartial) Add(m model.SamplePair) (Chunk, error) {
	return nil, errors.New("prometheusXorChunkPartial is read-only")
}

func (p *prometheusXorChunkPartial) NewIterator(iterator Iterator) Iterator {
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

func (p *prometheusXorChunkPartial) Marshal(i io.Writer) error {
	if p.chunk == nil {
		return errors.New("chunk data not set")
	}
	_, err := i.Write(p.chunk.Bytes())
	return err
}

func (p *prometheusXorChunkPartial) UnmarshalFromBuf(bytes []byte) error {
	c, err := chunkenc.FromData(chunkenc.EncXORPartial, bytes)
	if err != nil {
		return errors.Wrap(err, "failed to create Prometheus XORChunkPartial from bytes")
	}

	p.chunk = c
	return nil
}

func (p *prometheusXorChunkPartial) Encoding() Encoding {
	return PrometheusXorChunkPartial
}

func (p *prometheusXorChunkPartial) Utilization() float64 {
	// Used for reporting when chunk is used to store new data.
	return 0
}

func (p *prometheusXorChunkPartial) Slice(_, _ model.Time) Chunk {
	return p
}

func (p *prometheusXorChunkPartial) Rebound(from, to model.Time) (Chunk, error) {
	return nil, errors.New("Rebound not supported by prometheusXorChunkPartial")
}

func (p *prometheusXorChunkPartial) Len() int {
	if p.chunk == nil {
		return 0
	}
	return p.chunk.NumSamples()
}

func (p *prometheusXorChunkPartial) Size() int {
	if p.chunk == nil {
		return 0
	}
	return len(p.chunk.Bytes())
}
