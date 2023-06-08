package pool

import (
	"fmt"
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

// NewFastReleasingSlabPool returns a new "fast-releasing" slab pool.
func NewFastReleasingSlabPool[T any](delegate Interface, slabSize int) *FastReleasingSlabPool[T] {
	return &FastReleasingSlabPool[T]{
		delegate: delegate,
		slabSize: slabSize,
		// slabID = 0 is invalid, next valid slab we add will have index 1.
		slabs: make([]*trackedSlab[T], 1, 10),
	}
}

const (
	freeSlabChecks = 3
)

// Release decreases reference counter for given slab ID. (Slab ids equal to or less than 0 are ignored).
// If reference counter is 0, slab may be returned to the delegate pool.
func (b *FastReleasingSlabPool[T]) Release(slabID int) {
	if slabID <= 0 {
		return
	}

	var slabToRelease []T
	defer func() {
		// Return of the slab is done via defer, so that it can be done outside the lock.
		if slabToRelease != nil {
			b.delegate.Put(slabToRelease)
		}
	}()

	b.mtx.Lock()
	defer b.mtx.Unlock()

	if slabID >= len(b.slabs) {
		panic(fmt.Sprintf("invalid slab id: %d", slabID))
	}

	ts := b.slabs[slabID]
	if ts == nil {
		panic("nil slab")
	}
	if ts.references <= 0 {
		// This should never happen, because we release slabs with 0 references immediately.
		panic(fmt.Sprintf("invalid reference count: %d", ts.references))
	}

	ts.references--
	if ts.references == 0 {
		b.slabs[slabID] = nil
		slabToRelease = ts.slab
	}
}

// Get returns a slice of T with the given length and capacity (both match), and slab ID that needs be used in Release call.
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

	slabID := 0
	ts := (*trackedSlab[T])(nil)

	// Look at last few slabs, since we assume that these slabs have the most free space.
	for sid := len(b.slabs) - 1; sid >= len(b.slabs)-freeSlabChecks && sid >= 0; sid-- {
		s := b.slabs[sid]
		if s != nil && len(s.slab)-s.nextFreeIndex >= size {
			slabID = sid
			ts = s
			break
		}
	}

	if slabID == 0 {
		var slab []T

		if fromDelegate := b.delegate.Get(); fromDelegate != nil {
			slab = (fromDelegate).([]T)
		} else {
			slab = make([]T, b.slabSize)
		}

		ts = &trackedSlab[T]{
			slab: slab,
		}
		slabID = len(b.slabs)
		b.slabs = append(b.slabs, ts)
	}

	out := ts.slab[ts.nextFreeIndex : ts.nextFreeIndex+size : ts.nextFreeIndex+size]
	ts.nextFreeIndex += size
	ts.references++

	return out, slabID
}
