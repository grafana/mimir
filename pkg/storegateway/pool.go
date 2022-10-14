// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storegateway/chunk_bytes_pool.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package storegateway

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/util/pool"
)

const (
	chunkSliceSize = 16_384 // An arbitrary size limit, susceptible to change.
)

type chunkBytesPool struct {
	pool *pool.BucketedBytes

	// Metrics.
	requestedBytes prometheus.Counter
	returnedBytes  prometheus.Counter
}

func newChunkBytesPool(minBucketSize, maxBucketSize int, maxChunkPoolBytes uint64, reg prometheus.Registerer) (*chunkBytesPool, error) {
	upstream, err := pool.NewBucketedBytes(minBucketSize, maxBucketSize, 2, maxChunkPoolBytes)
	if err != nil {
		return nil, err
	}

	return &chunkBytesPool{
		pool: upstream,
		requestedBytes: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_bucket_store_chunk_pool_requested_bytes_total",
			Help: "Total bytes requested to chunk bytes pool.",
		}),
		returnedBytes: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_bucket_store_chunk_pool_returned_bytes_total",
			Help: "Total bytes returned by the chunk bytes pool.",
		}),
	}, nil
}

func (p *chunkBytesPool) Get(sz int) (*[]byte, error) {
	buffer, err := p.pool.Get(sz)
	if err != nil {
		return buffer, err
	}

	p.requestedBytes.Add(float64(sz))
	p.returnedBytes.Add(float64(cap(*buffer)))

	return buffer, err
}

func (p *chunkBytesPool) Put(b *[]byte) {
	p.pool.Put(b)
}

// chunkSlicePool is a memory pool of chunk slices.
type chunkSlicePool struct {
	pool sync.Pool
}

// newChunksSlicePool creates a new chunks pool.
func newChunksSlicePool() *chunkSlicePool {
	return &chunkSlicePool{
		pool: sync.Pool{
			New: func() interface{} {
				return &chunkSlice{
					chunks: make([]storepb.Chunk, chunkSliceSize),
				}
			},
		},
	}
}

// get returns a chunk slice from the pool.
func (p *chunkSlicePool) get() *chunkSlice {
	return p.pool.Get().(*chunkSlice)
}

// put returns a chunk slice to the pool.
func (p *chunkSlicePool) put(slice *chunkSlice) {
	slice.i = 0
	p.pool.Put(slice)
}

type chunkSlice struct {
	chunks []storepb.Chunk
	i      int
}

func (s *chunkSlice) next() *storepb.Chunk {
	if s.isExhausted() {
		return nil
	}
	chk := &s.chunks[s.i]
	chk.Data = nil
	s.i++
	return chk
}

func (s *chunkSlice) isExhausted() bool {
	return s.i >= len(s.chunks)
}
