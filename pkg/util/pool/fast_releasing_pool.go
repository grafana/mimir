package pool

import (
	"fmt"
	"sync"
)

// FastReleasingSlabPool is similar to SlabPool, but allows for fast release of slabs if they are not used anymore.
type FastReleasingSlabPool[T any] struct {
	delegate Interface
	slabSize int

	mtx       sync.Mutex
	allSlabs  map[int]*trackedSlab[T] // All slabs.
	freeSlabs map[int]*trackedSlab[T] // Slabs with free space.
	lastSlab  int                     // ID of last created trackedSlab.
}

type trackedSlab[T any] struct {
	slab          []T
	references    int // How many slices from this slab were returned via Get.
	nextFreeIndex int
}

func NewFastReleasingSlabPool[T any](delegate Interface, slabSize int) *FastReleasingSlabPool[T] {
	return &FastReleasingSlabPool[T]{
		delegate:  delegate,
		slabSize:  slabSize,
		allSlabs:  map[int]*trackedSlab[T]{},
		freeSlabs: map[int]*trackedSlab[T]{},
	}
}

const (
	freeSlabChecks = 3
	freeSlabFactor = float64(2) / 3
)

// Release decreases reference counter for given slab.
// If reference counter is 0, slab may be returned to the delegate pool, but it may
// also be preserved for later allocations. See ReleaseAll to return all slabs.
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

	ts := b.allSlabs[slabId]
	if ts == nil || ts.references <= 0 {
		panic("invalid reference count")
	}

	ts.references--
	if ts.references == 0 {
		// If this slab is too full or or there are too many free slabs, return slab back to delegate.
		// Otherwise keep it for future Get calls.
		if ts.nextFreeIndex >= int(freeSlabFactor*float64(len(ts.slab))) || len(b.freeSlabs) > freeSlabChecks {
			delete(b.allSlabs, slabId)
			delete(b.freeSlabs, slabId)
			slabToRelease = ts.slab
		}
	}
}

// ReleaseAll releases all slabs. This method expects that all slabs have no references anymore,
// and it will panic if there is still a slab with non-zero reference counter.
func (b *FastReleasingSlabPool[T]) ReleaseAll() {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	for id, ts := range b.allSlabs {
		if ts.references > 0 {
			panic(fmt.Sprintf("slab is still being used, references: %d", ts.references))
		}

		delete(b.allSlabs, id)
		delete(b.freeSlabs, id)
		b.delegate.Put(ts.slab)
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

	count := 0
	for sid, s := range b.freeSlabs {
		if len(s.slab)-s.nextFreeIndex >= size {
			slabId = sid
			ts = s
			break
		}
		count++
		// No space found in few slabs, get a new one.
		if count >= freeSlabChecks {
			break
		}
	}

	if slabId == 0 {
		var slab []T

		fromDelegate := b.delegate.Get()
		if fromDelegate == nil {
			slab = make([]T, b.slabSize, b.slabSize)
		} else {
			slab = (fromDelegate).([]T)
		}

		ts = &trackedSlab[T]{
			slab: slab,
		}
		b.lastSlab++
		slabId = b.lastSlab
		b.allSlabs[slabId] = ts
		b.freeSlabs[slabId] = ts
	}

	out := ts.slab[ts.nextFreeIndex : ts.nextFreeIndex+size : ts.nextFreeIndex+size]
	ts.nextFreeIndex += size
	ts.references++

	if ts.nextFreeIndex >= len(ts.slab) {
		// this slab is no longer free, remove it from free slabs.
		delete(b.freeSlabs, slabId)
	}

	return out, slabId
}
