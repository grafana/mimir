// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/pool/pool.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package pool

import (
	"sync"

	"github.com/pkg/errors"
)

// Interface defines the same functions of sync.Pool.
type Interface interface {
	// Put is sync.Pool.Put().
	Put(x any)
	// Get is sync.Pool.Get().
	Get() any
}

type NoopPool struct{}

func (NoopPool) Put(any)  {}
func (NoopPool) Get() any { return nil }

// Bytes is a pool of bytes that can be reused.
type Bytes interface {
	// Get returns a new byte slices that fits the given size.
	Get(sz int) (*[]byte, error)
	// Put returns a byte slice to the right bucket in the pool.
	Put(b *[]byte)
}

// NoopBytes is pool that always allocated required slice on heap and ignore puts.
type NoopBytes struct{}

func (p NoopBytes) Get(sz int) (*[]byte, error) {
	b := make([]byte, 0, sz)
	return &b, nil
}

func (p NoopBytes) Put(*[]byte) {}

// BucketedBytes is a bucketed pool for variably sized byte slices. It can be configured to not allow
// more than a maximum number of bytes being used at a given time.
// Every byte slice obtained from the pool must be returned.
type BucketedBytes struct {
	buckets   []sync.Pool
	sizes     []int
	maxTotal  uint64
	usedTotal uint64
	mtx       sync.Mutex

	new func(s int) *[]byte
}

// NewBucketedBytes returns a new Bytes with size buckets for minSize to maxSize
// increasing by the given factor and maximum number of used bytes.
// No more than maxTotal bytes can be used at any given time unless maxTotal is set to 0.
func NewBucketedBytes(minSize, maxSize int, factor float64, maxTotal uint64) (*BucketedBytes, error) {
	if minSize < 1 {
		return nil, errors.New("invalid minimum pool size")
	}
	if maxSize < 1 {
		return nil, errors.New("invalid maximum pool size")
	}
	if factor < 1 {
		return nil, errors.New("invalid factor")
	}

	var sizes []int

	for s := minSize; s <= maxSize; s = int(float64(s) * factor) {
		sizes = append(sizes, s)
	}
	p := &BucketedBytes{
		buckets:  make([]sync.Pool, len(sizes)),
		sizes:    sizes,
		maxTotal: maxTotal,
		new: func(sz int) *[]byte {
			s := make([]byte, 0, sz)
			return &s
		},
	}
	return p, nil
}

// ErrPoolExhausted is returned if a pool cannot provide the request bytes.
var ErrPoolExhausted = errors.New("pool exhausted")

// Get returns a new byte slice that fits the given size.
func (p *BucketedBytes) Get(sz int) (*[]byte, error) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	if p.maxTotal > 0 && p.usedTotal+uint64(sz) > p.maxTotal {
		return nil, ErrPoolExhausted
	}

	for i, bktSize := range p.sizes {
		if sz > bktSize {
			continue
		}
		b, ok := p.buckets[i].Get().(*[]byte)
		if !ok {
			b = p.new(bktSize)
		}

		p.usedTotal += uint64(cap(*b))
		return b, nil
	}

	// The requested size exceeds that of our highest bucket, allocate it directly.
	p.usedTotal += uint64(sz)
	return p.new(sz), nil
}

// Put returns a byte slice to the right bucket in the pool.
func (p *BucketedBytes) Put(b *[]byte) {
	if b == nil {
		return
	}

	sz := cap(*b)
	for i, bktSize := range p.sizes {
		if sz > bktSize {
			continue
		}
		*b = (*b)[:0]
		p.buckets[i].Put(b)
		break
	}

	p.mtx.Lock()
	defer p.mtx.Unlock()
	// We could assume here that our users will not make the slices larger
	// but lets be on the safe side to avoid an underflow of p.usedTotal.
	if uint64(sz) >= p.usedTotal {
		p.usedTotal = 0
	} else {
		p.usedTotal -= uint64(sz)
	}
}

// SlabPool wraps Interface and adds support to get a sub-slice of the data type T
// from the pool, trying to fit the slices picked from the pool as much as possible.
//
// The slices returned by SlabPool.Get() will be released back to the pool once
// SlabPool.Release() is called.
//
// SlabPool is NOT concurrency safe.
type SlabPool[T any] struct {
	delegate Interface
	slabSize int
	slabs    []*[]T
}

func NewSlabPool[T any](delegate Interface, slabSize int) *SlabPool[T] {
	return &SlabPool[T]{
		delegate: delegate,
		slabSize: slabSize,
	}
}

// Release all slices returned by Get. It's unsafe to access slices previously returned by Get
// after calling Release().
func (b *SlabPool[T]) Release() {
	for _, slab := range b.slabs {
		// The slab length will be reset to 0 in the Get().
		b.delegate.Put(slab)
	}

	b.slabs = b.slabs[:0]
}

// Get returns a slice of T with the given length and capacity (both matches).
func (b *SlabPool[T]) Get(size int) []T {
	const lookback = 3

	if size <= 0 {
		return nil
	}

	// If the requested size is bigger than the slab size, then the slice
	// can't be handled by this pool and will be allocated outside it.
	if size > b.slabSize {
		return make([]T, size)
	}

	var slab *[]T

	// Look in the last few slabs if there's any space left.
	for i := len(b.slabs) - 1; i >= len(b.slabs)-lookback && i >= 0; i-- {
		if cap(*b.slabs[i])-len(*b.slabs[i]) >= size {
			slab = b.slabs[i]
			break
		}
	}

	// Get a new one if there's no space available in the last few slabs.
	if slab == nil {
		if reused := b.delegate.Get(); reused != nil {
			slab = reused.(*[]T)
			*slab = (*slab)[:0]
		} else {
			newSlab := make([]T, 0, b.slabSize)
			slab = &newSlab
		}

		// Add the slab to the list of slabs.
		b.slabs = append(b.slabs, slab)
	}

	// Resize the slab length to include the requested size
	*slab = (*slab)[:len(*slab)+size]
	// Create a subslice of the slab with length and capacity of size
	return (*slab)[len(*slab)-size : len(*slab) : len(*slab)]
}

// SafeSlabPool wraps SlabPool to make it safe for concurrent use from multiple goroutines
type SafeSlabPool[T any] struct {
	wrappedMx sync.Mutex
	wrapped   *SlabPool[T]
}

func NewSafeSlabPool[T any](delegate Interface, slabSize int) *SafeSlabPool[T] {
	return &SafeSlabPool[T]{
		wrapped: NewSlabPool[T](delegate, slabSize),
	}
}

func (b *SafeSlabPool[T]) Release() {
	b.wrappedMx.Lock()
	defer b.wrappedMx.Unlock()

	b.wrapped.Release()
}

func (b *SafeSlabPool[T]) Get(size int) []T {
	b.wrappedMx.Lock()
	defer b.wrappedMx.Unlock()

	return b.wrapped.Get(size)
}

type SafeSlabPoolAllocator struct {
	pool *SafeSlabPool[byte]
}

// NewSafeSlabPoolAllocator wraps the input SafeSlabPool[byte] into an allocator suitable to be used with
// a cache client. This function returns nil if the input SafeSlabPool[byte] is nil.
func NewSafeSlabPoolAllocator(pool *SafeSlabPool[byte]) *SafeSlabPoolAllocator {
	if pool == nil {
		return nil
	}

	return &SafeSlabPoolAllocator{
		pool: pool,
	}
}

func (a *SafeSlabPoolAllocator) Get(sz int) *[]byte {
	b := a.pool.Get(sz)
	return &b
}

func (a *SafeSlabPoolAllocator) Put(_ *[]byte) {
	// no-op
}
