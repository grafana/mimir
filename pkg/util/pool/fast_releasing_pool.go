package pool

import "sync"

// FastReleasingSlabPool is similar to SlabPool, but allows for fast release of slabs if they are not used anymore.
type FastReleasingSlabPool[T any] struct {
	delegate Interface
	slabSize int

	mtx       sync.Mutex
	allSlabs  map[int]*trackedSlab[T]
	freeSlabs map[int]*trackedSlab[T]
	lastSlab  int
}

type trackedSlab[T any] struct {
	slab          []T
	references    int
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

func (b *FastReleasingSlabPool[T]) Release(slabId int) {
	if slabId <= 0 {
		return
	}

	var slabToRelease []T

	defer func() {
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
		delete(b.allSlabs, slabId)
		delete(b.freeSlabs, slabId)
	}
}

// Get returns a slice of T with the given length and capacity (both matches).
func (b *FastReleasingSlabPool[T]) Get(size int) ([]T, int) {
	const freeSlabChecks = 3

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
		slab := b.delegate.Get().([]T)
		if slab == nil {
			slab = make([]T, 0, b.slabSize)
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
