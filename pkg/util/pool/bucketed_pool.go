// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/util/pool/pool.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package pool

import (
	"fmt"
	"math/bits"

	"github.com/prometheus/prometheus/util/zeropool"
)

// BucketedPool is a bucketed pool for variably sized slices.
// It is similar to prometheus/prometheus' pool.Pool, but:
// - uses zeropool.Pool internally
// - uses generics to avoid reflection
// - only supports using a factor of 2
type BucketedPool[T ~[]E, E any] struct {
	buckets []zeropool.Pool[T]
	maxSize uint
	// make is the function used to create an empty slice when none exist yet.
	make func(int) T
}

// NewBucketedPool returns a new BucketedPool with buckets separated by a factor of 2 up to maxSize.
func NewBucketedPool[T ~[]E, E any](maxSize uint, makeFunc func(int) T) *BucketedPool[T, E] {
	if maxSize <= 1 {
		panic("invalid maximum pool size")
	}

	bucketCount := bits.Len(maxSize)

	p := &BucketedPool[T, E]{
		buckets: make([]zeropool.Pool[T], bucketCount),
		maxSize: maxSize,
		make:    makeFunc,
	}

	return p
}

// Get returns a new slice with capacity greater than or equal to size.
// If no bucket large enough exists, a slice larger than the requested size
// of the next power of two is returned.
// Get guarantees the resulting slice always has a capacity in power of twos.
func (p *BucketedPool[T, E]) Get(size int) T {
	if size < 0 {
		panic(fmt.Sprintf("BucketedPool.Get with negative size %v", size))
	}

	if size == 0 {
		return nil
	}

	bucketIndex := bits.Len(uint(size - 1))

	// If bucketIndex exceeds the number of available buckets, return a slice of the next power of two.
	if bucketIndex >= len(p.buckets) {
		nextPowerOfTwo := 1 << bucketIndex
		return p.make(nextPowerOfTwo)
	}

	s := p.buckets[bucketIndex].Get()

	if s == nil {
		nextPowerOfTwo := 1 << bucketIndex
		s = p.make(nextPowerOfTwo)
	}

	return s
}

// Put adds a slice to the right bucket in the pool.
// If the slice does not belong to any bucket in the pool, it is ignored.
func (p *BucketedPool[T, E]) Put(s T) {
	size := uint(cap(s))

	if size == 0 || size > p.maxSize {
		return
	}

	bucketIndex := bits.Len(size - 1)
	if bucketIndex >= len(p.buckets) {
		return // Ignore slices larger than the largest bucket
	}

	// Ignore slices that do not align to the current power of 2
	// (this will only happen where a slice did not originally come from the pool).
	if size != (1 << bucketIndex) {
		return
	}

	p.buckets[bucketIndex].Put(s[0:0])
}
