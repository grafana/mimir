// SPDX-License-Identifier: AGPL-3.0-only

package operator

import (
	"sync"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/pool"
)

type Pool struct {
	fPointSlicePool         *pool.Pool
	matrixPool              *pool.Pool
	vectorPool              *pool.Pool
	seriesMetadataSlicePool *pool.Pool
	seriesBatchPool         sync.Pool
	floatSlicePool          *pool.Pool
	boolSlicePool           *pool.Pool
}

func NewPool() *Pool {
	return &Pool{
		// TODO: what is a reasonable upper limit here?
		fPointSlicePool: pool.New(1, 100000, 10, func(size int) interface{} {
			return make([]promql.FPoint, 0, size)
		}),
		// TODO: what is a reasonable upper limit here?
		matrixPool: pool.New(1, 100000, 10, func(size int) interface{} {
			return make(promql.Matrix, 0, size)
		}),
		// TODO: what is a reasonable upper limit here?
		vectorPool: pool.New(1, 100000, 10, func(size int) interface{} {
			return make(promql.Vector, 0, size)
		}),
		// TODO: what is a reasonable upper limit here?
		seriesMetadataSlicePool: pool.New(1, 100000, 10, func(size int) interface{} {
			return make([]SeriesMetadata, 0, size)
		}),
		seriesBatchPool: sync.Pool{New: func() any {
			return &SeriesBatch{
				series: make([]storage.Series, 0, 100), // TODO: what is a reasonable batch size?
				next:   nil,
			}
		}},
		// TODO: what is a reasonable upper limit here?
		floatSlicePool: pool.New(1, 100000, 10, func(size int) interface{} {
			return nil
		}),
		// TODO: what is a reasonable upper limit here?
		boolSlicePool: pool.New(1, 100000, 10, func(size int) interface{} {
			return nil
		}),
	}
}

func (p *Pool) GetFPointSlice(size int) []promql.FPoint {
	return p.fPointSlicePool.Get(size).([]promql.FPoint)
}

func (p *Pool) PutFPointSlice(s []promql.FPoint) {
	if s != nil {
		p.fPointSlicePool.Put(s)
	}
}

func (p *Pool) GetMatrix(size int) promql.Matrix {
	return p.matrixPool.Get(size).(promql.Matrix)
}

func (p *Pool) PutMatrix(m promql.Matrix) {
	if m != nil {
		p.matrixPool.Put(m)
	}
}

func (p *Pool) GetVector(size int) promql.Vector {
	return p.vectorPool.Get(size).(promql.Vector)
}

func (p *Pool) PutVector(v promql.Vector) {
	if v != nil {
		p.vectorPool.Put(v)
	}
}

func (p *Pool) GetSeriesMetadataSlice(size int) []SeriesMetadata {
	return p.seriesMetadataSlicePool.Get(size).([]SeriesMetadata)
}

func (p *Pool) PutSeriesMetadataSlice(s []SeriesMetadata) {
	if s != nil {
		p.seriesMetadataSlicePool.Put(s)
	}
}

func (p *Pool) GetSeriesBatch() *SeriesBatch {
	return p.seriesBatchPool.Get().(*SeriesBatch)
}

func (p *Pool) PutSeriesBatch(b *SeriesBatch) {
	if b != nil {
		b.series = b.series[:0]
		b.next = nil
		p.seriesBatchPool.Put(b)
	}
}

func (p *Pool) GetFloatSlice(size int) []float64 {
	s := p.floatSlicePool.Get(size)
	if s != nil {
		return zeroFloatSlice(s.([]float64), size)
	}

	return make([]float64, 0, size)
}

func (p *Pool) PutFloatSlice(s []float64) {
	if s != nil {
		p.floatSlicePool.Put(s)
	}
}

func (p *Pool) GetBoolSlice(size int) []bool {
	s := p.boolSlicePool.Get(size)

	if s != nil {
		return zeroBoolSlice(s.([]bool), size)
	}

	return make([]bool, 0, size)
}

func (p *Pool) PutBoolSlice(s []bool) {
	if s != nil {
		p.boolSlicePool.Put(s)
	}
}

// TODO: use vectorisation to make this faster
func zeroFloatSlice(s []float64, size int) []float64 {
	s = s[:size]

	for i := range s {
		s[i] = 0
	}

	return s[:0]
}

// TODO: use vectorisation to make this faster
func zeroBoolSlice(s []bool, size int) []bool {
	s = s[:size]

	for i := range s {
		s[i] = false
	}

	return s[:0]
}
