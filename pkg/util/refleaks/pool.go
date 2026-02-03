// SPDX-License-Identifier: AGPL-3.0-only

package refleaks

import (
	"reflect"
	"sync"

	"github.com/prometheus/prometheus/util/zeropool"
)

type poolInstrumenter[T any] struct {
	tracker         *Tracker
	reserver        reserver
	pageAlignedSize int
	item            func(Allocator) T

	instrumentedMtx sync.RWMutex
	instrumented    map[uintptr][]byte
}

func newPoolInstrumenter[T any](t *Tracker, item func(Allocator) T) poolInstrumenter[T] {
	var reserver reserver
	item(&reserver)

	return poolInstrumenter[T]{
		tracker:         t,
		reserver:        reserver,
		pageAlignedSize: reserver.pageAlignedSize(),
		item:            item,
		instrumented:    make(map[uintptr][]byte),
	}
}

func (p *poolInstrumenter[T]) maybeGetInstrumented() (T, bool) {
	var item T
	if !p.tracker.shouldInstrument(p.pageAlignedSize) {
		return item, false
	}

	alloc, b := p.reserver.allocator()

	item = p.item(alloc)

	p.instrumentedMtx.Lock()
	p.instrumented[reflect.ValueOf(item).Pointer()] = b
	p.instrumentedMtx.Unlock()

	return item, true
}

func (p *poolInstrumenter[T]) maybePutInstrumented(item T) bool {
	pitem := reflect.ValueOf(item).Pointer()

	p.instrumentedMtx.RLock()
	_, ok := p.instrumented[pitem]
	p.instrumentedMtx.RUnlock()
	if !ok {
		return false
	}

	p.instrumentedMtx.Lock()
	buf, ok := p.instrumented[pitem]
	if !ok {
		// If this _was_ instrumented above but now isn't, this must have been
		// double-put, which is a bug.
		panic("item was already put back in the pool")
	}
	delete(p.instrumented, pitem)
	p.instrumentedMtx.Unlock()

	p.tracker.free(buf)
	return true
}

// SyncPool wraps a [sync.Pool] to instrument reference leaks.
//
// The Get method may return, instead of an object from the pool, a new object
// allocated in such a manner that trying to access its memory after it's been
// put back into the pool will cause a segmentation fault.
type SyncPool struct {
	p sync.Pool
	poolInstrumenter[any]
}

// NewInstrumentedSyncPool returns a [SyncPool].
//
// The item function must use the provided [Allocator], through [New] and
// [MakeSlice], to allocate the memory that will be instrumented for reference
// leaks. Note that allocation will panic if not enough memory has been reserved.
//
// The item function must allocate exactly the same amount of memory every time
// it's called.
//
// The item's type must be pointer-like. See [reflect.Value.Pointer].
//
// According to the tracker configuration, not all objects will be constructed
// to be instrumented; for those that aren't, the allocator is just a shim
// for [builtin.new] and [builtin.make].
func NewInstrumentedSyncPool(t *Tracker, item func(Allocator) any) SyncPool {
	return SyncPool{
		p: sync.Pool{New: func() any {
			return item(nil)
		}},
		poolInstrumenter: newPoolInstrumenter(t, item),
	}
}

func (p *SyncPool) Get() any {
	if item, ok := p.maybeGetInstrumented(); ok {
		return item
	}
	return p.p.Get()
}

func (p *SyncPool) Put(item any) {
	if !p.maybePutInstrumented(item) {
		p.p.Put(item)
	}
}

// ZeroPool wraps a [zeropool.Pool] to instrument reference leaks.
//
// The Get method may return, instead of an object from the pool, a new object
// allocated in such a manner that trying to access its memory after it's been
// put back into the pool will cause a segmentation fault.
type ZeroPool[T any] struct {
	p zeropool.Pool[T]
	poolInstrumenter[T]
}

// NewInstrumentedZeroPool returns a [ZeroPool].
//
// The item function must use the provided [Allocator], through [New] and
// [MakeSlice], to allocate the memory that will be instrumented for reference
// leaks. Note that allocation will panic if not enough memory has been reserved.
//
// The item function must allocate exactly the same amount of memory every time
// it's called.
//
// The item's type must be pointer-like. See [reflect.Value.Pointer].
//
// According to the tracker configuration, not all objects will be constructed
// to be instrumented; for those that aren't, the allocator is just a shim
// for [builtin.new] and [builtin.make].
func NewInstrumentedZeroPool[T any](t *Tracker, item func(Allocator) T) ZeroPool[T] {
	return ZeroPool[T]{
		p: zeropool.New(func() T {
			return item(nil)
		}),
		poolInstrumenter: newPoolInstrumenter(t, item),
	}
}

func (p *ZeroPool[T]) Get() T {
	if item, ok := p.maybeGetInstrumented(); ok {
		return item
	}
	return p.p.Get()
}

func (p *ZeroPool[T]) Put(item T) {
	if !p.maybePutInstrumented(item) {
		p.p.Put(item)
	}
}
