// Copyright 2017 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pool

import (
	"sync"
)

// BucketedPool is a bucketed pool for variably sized slices.
type BucketedPool[T any] struct {
	buckets []sync.Pool
	sizes   []int
	// make is the function used to create an empty slice when none exist yet.
	make func(int) []T
}

// NewBucketedPool returns a new BucketedPool with size buckets for minSize to maxSize
// increasing by the given factor.
func NewBucketedPool[T any](minSize, maxSize int, factor float64, makeFunc func(int) []T) *BucketedPool[T] {
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

	for s := minSize; s <= maxSize; s = int(float64(s) * factor) {
		sizes = append(sizes, s)
	}

	p := &BucketedPool[T]{
		buckets: make([]sync.Pool, len(sizes)),
		sizes:   sizes,
		make:    makeFunc,
	}
	return p
}

// Get returns a new slice with capacity greater than or equal to size.
func (p *BucketedPool[T]) Get(size int) []T {
	for i, bktSize := range p.sizes {
		if size > bktSize {
			continue
		}
		buff := p.buckets[i].Get()
		if buff == nil {
			buff = p.make(bktSize)
		}
		return buff.([]T)
	}
	return p.make(size)
}

// Put adds a slice to the right bucket in the pool.
// If the slice does not belong to any bucket in the pool, it is ignored.
func (p *BucketedPool[T]) Put(s []T) {
	sCap := cap(s)
	if sCap < p.sizes[0] {
		return
	}

	for i, size := range p.sizes {
		if sCap > size {
			continue
		}

		if sCap == size {
			// Buffer is exactly the minimum size for this bucket. Add it to this bucket.
			p.buckets[i].Put(s)
		} else {
			// Buffer belongs in previous bucket.
			p.buckets[i-1].Put(s)
		}
		return
	}
}


