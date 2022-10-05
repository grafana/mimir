// SPDX-License-Identifier: AGPL-3.0-only

package client

import "sync"

const (
	maxPoolResponseTimeseries  = 10_000
	maxPoolResponseChunkseries = 100_000
)

// QueryStreamPool is a pool for QueryStreamResponse objects.
type QueryStreamPool interface {
	Get() *QueryStreamResponse
	Put(*QueryStreamResponse)
}

type queryStreamPool struct {
	p sync.Pool
}

// NewQueryStreamPool makes a new QueryStreamPool.
func NewQueryStreamPool() QueryStreamPool {
	return &queryStreamPool{
		p: sync.Pool{
			New: func() interface{} {
				return &QueryStreamResponse{}
			},
		},
	}
}

// Get gets a QueryStreamResponse from the pool.
func (p *queryStreamPool) Get() *QueryStreamResponse {
	return p.p.Get().(*QueryStreamResponse)
}

// Put puts a QueryStreamResponse back into the pool.
func (p *queryStreamPool) Put(r *QueryStreamResponse) {
	if len(r.Timeseries) > maxPoolResponseTimeseries || len(r.Chunkseries) > maxPoolResponseChunkseries {
		return // don't put back in pool if it's too big
	}
	r.Timeseries = r.Timeseries[:0]
	r.Chunkseries = r.Chunkseries[:0]
	p.p.Put(r)
}
