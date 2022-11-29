// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/pool/pool.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package pool

import (
	"sync"

	"github.com/pkg/errors"
)

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

// BatchBytes uses a Bytes pool to hand out byte slices, but they don't need to be
// individually put back into the pool. Instead, they can be all released at once.
// Additionally, BatchBytes combines results from the other two.
// BatchBytes is concurrency safe.
type BatchBytes struct {
	Delegate Bytes

	mtx   sync.Mutex
	slabs []*[]byte
}

func (b *BatchBytes) Release() {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	for _, slab := range b.slabs {
		b.Delegate.Put(slab)
	}
	b.slabs = b.slabs[:0]
}

func (b *BatchBytes) Get(sz int) ([]byte, error) {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	if len(b.slabs) == 0 ||
		// Ensure we never grow slab beyond original capacity.
		cap(*b.slabs[len(b.slabs)-1])-len(*b.slabs[len(b.slabs)-1]) < sz {
		s, err := b.Delegate.Get(sz)
		if err != nil {
			return nil, errors.Wrap(err, "allocate chunk bytes")
		}
		b.slabs = append(b.slabs, s)
	}
	slab := b.slabs[len(b.slabs)-1]
	*slab = (*slab)[:len(*slab)+sz]
	return (*slab)[len(*slab)-sz : len(*slab) : len(*slab)], nil
}
