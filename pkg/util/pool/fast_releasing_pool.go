package pool

import (
	"sync"
)

// FastReleasingSlabPool is similar to SlabPool, but allows for fast release of slabs if they are not used anymore.
type FastReleasingSlabPool[T any] struct {
	delegate Interface
	slabSize int

	mtx   sync.Mutex
	slabs []*trackedSlab[T] // All slabs. Slab ID is an index into this slice.
}

type trackedSlab[T any] struct {
	slab          []T
	references    int // How many slices from this slab were returned via Get.
	nextFreeIndex int
}

func NewFastReleasingSlabPool[T any](delegate Interface, slabSize int) *FastReleasingSlabPool[T] {
	return &FastReleasingSlabPool[T]{
		delegate: delegate,
		slabSize: slabSize,
		slabs:    []*trackedSlab[T]{nil}, // slabId = 0 is invalid.
	}
}

const (
	freeSlabChecks = 3
)

// Release decreases reference counter for given slab.
// If reference counter is 0, slab may be returned to the delegate pool.
func (b *FastReleasingSlabPool[T]) Release(slabId int) {
	if slabId <= 0 {
		return
	}

	var slabToRelease []T
	defer func() {
		// Return of the slab is done via defer, so that it can be done without the lock.
		if slabToRelease != nil {
			b.delegate.Put(slabToRelease)
		}
	}()

	b.mtx.Lock()
	defer b.mtx.Unlock()

	if slabId >= len(b.slabs) {
		panic("invalid slabId")
	}

	ts := b.slabs[slabId]
	if ts == nil || ts.references <= 0 {
		panic("invalid reference count")
	}

	ts.references--
	if ts.references == 0 {
		b.slabs[slabId] = nil
		slabToRelease = ts.slab
	}
}

// Get returns a slice of T with the given length and capacity (both matches).
func (b *FastReleasingSlabPool[T]) Get(size int) ([]T, int) {
	if size <= 0 {
		return nil, 0
	}

	// If the requested size is bigger than the slab size, then the slice
	// can't be handled by this pool and will be allocated outside it.
	if size > b.slabSize {
		return make([]T, size), 0
	}

	b.mtx.Lock()
	defer b.mtx.Unlock()

	slabId := 0
	ts := (*trackedSlab[T])(nil)

	// Look at last few slabs (with most free space).
	for sid := len(b.slabs) - 1; sid >= len(b.slabs)-freeSlabChecks && sid >= 0; sid-- {
		s := b.slabs[sid]
		if s != nil && len(s.slab)-s.nextFreeIndex >= size {
			slabId = sid
			ts = s
			break
		}
	}

	if slabId == 0 {
		var slab []T

		if fromDelegate := b.delegate.Get(); fromDelegate == nil {
			slab = make([]T, b.slabSize, b.slabSize)
		} else {
			slab = (fromDelegate).([]T)
		}

		ts = &trackedSlab[T]{
			slab: slab,
		}
		slabId = len(b.slabs)
		b.slabs = append(b.slabs, ts)
	}

	out := ts.slab[ts.nextFreeIndex : ts.nextFreeIndex+size : ts.nextFreeIndex+size]
	ts.nextFreeIndex += size
	ts.references++

	return out, slabId
}
