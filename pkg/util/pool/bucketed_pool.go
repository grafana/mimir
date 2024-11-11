// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/util/pool/pool.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package pool

import (
	"github.com/prometheus/prometheus/util/zeropool"
)

// BucketedPool is a bucketed pool for variably sized slices.
// It is similar to prometheus/prometheus' pool.Pool, but uses zeropool.Pool internally, and
// generics to avoid reflection.
type BucketedPool[T ~[]E, E any] struct {
	buckets []zeropool.Pool[T]
	sizes   []int
	// make is the function used to create an empty slice when none exist yet.
	make func(int) T
}

// NewBucketedPool returns a new BucketedPool with size buckets for minSize to maxSize
// increasing by the given factor.
func NewBucketedPool[T ~[]E, E any](minSize, maxSize int, factor int, makeFunc func(int) T) *BucketedPool[T, E] {
	if minSize < 1 {
		panic("invalid minimum pool size")
	}
	if maxSize < 1 {
		panic("invalid maximum pool size")
	}
	if factor < 1 {
		panic("invalid factor")
	}

	var sizes []int

	for s := minSize; s <= maxSize; s = s * factor {
		sizes = append(sizes, s)
	}

	p := &BucketedPool[T, E]{
		buckets: make([]zeropool.Pool[T], len(sizes)),
		sizes:   sizes,
		make:    makeFunc,
	}

	return p
}

// Get returns a new slice with capacity greater than or equal to size.
func (p *BucketedPool[T, E]) Get(size int) T {
	for i, bktSize := range p.sizes {
		if size > bktSize {
			continue
		}
		b := p.buckets[i].Get()
		if b == nil {
			b = p.make(bktSize)
		}
		return b
	}
	return p.make(size)
}

// Put adds a slice to the right bucket in the pool.
// If the slice does not belong to any bucket in the pool, it is ignored.
func (p *BucketedPool[T, E]) Put(s T) {
	if cap(s) < p.sizes[0] {
		return
	}

	for i, size := range p.sizes {
		if cap(s) > size {
			continue
		}

		if cap(s) == size {
			// Slice is exactly the minimum size for this bucket. Add it to this bucket.
			p.buckets[i].Put(s[0:0])
		} else {
			// Slice belongs in previous bucket.
			p.buckets[i-1].Put(s[0:0])
		}

		return
	}
}
