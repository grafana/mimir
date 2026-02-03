// SPDX-License-Identifier: AGPL-3.0-only

package refleaks

import (
	"runtime/debug"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

type obj struct {
	value int
	items []int
}

func newObj(a Allocator) *obj {
	o := New[obj](a)
	o.value = 1337
	o.items = MakeSlice[int](a, 50, 100)
	return o
}

func TestPoolInstrumenter(t *testing.T) {
	for _, impl := range []struct {
		name string
		new  func(t *Tracker, item func(Allocator) *obj) any
		get  func(pool any) *obj
		put  func(pool any, o *obj)
	}{{
		name: "SyncPool",
		new: func(t *Tracker, item func(Allocator) *obj) any {
			p := NewInstrumentedSyncPool(t, func(a Allocator) any {
				return item(a)
			})
			return &p
		},
		get: func(pool any) *obj {
			return pool.(*SyncPool).Get().(*obj)
		},
		put: func(pool any, o *obj) {
			pool.(*SyncPool).Put(o)
		},
	}, {
		name: "ZeroPool",
		new: func(t *Tracker, item func(Allocator) *obj) any {
			p := NewInstrumentedZeroPool(t, item)
			return &p
		},
		get: func(pool any) *obj {
			return pool.(*ZeroPool[*obj]).Get()
		},
		put: func(pool any, o *obj) {
			pool.(*ZeroPool[*obj]).Put(o)
		},
	}} {
		t.Run(impl.name, func(t *testing.T) {
			prev := debug.SetPanicOnFault(true)
			defer debug.SetPanicOnFault(prev)

			tracker := InstrumentConfig{Percentage: 100}.Tracker(prometheus.NewRegistry())
			p := impl.new(tracker, newObj)

			o := impl.get(p)
			require.Equal(t, 1337, o.value)
			require.Equal(t, 50, len(o.items))
			require.Equal(t, 100, cap(o.items))

			// Every object musts be a separate allocation.
			o2 := impl.get(p)
			require.NotSame(t, o, o2)
			impl.put(p, o2)

			// Accessing the instrumented object panics after it's been returned to the
			// pool.
			impl.put(p, o)
			requireAccessPanics(t, func() any { return o.value })
			requireAccessPanics(t, func() any { return o.items[13] })

			// No instrumentation
			tracker = InstrumentConfig{Percentage: 0}.Tracker(prometheus.NewRegistry())
			p = impl.new(tracker, newObj)

			o = impl.get(p)
			require.Equal(t, 1337, o.value)
			require.Equal(t, 50, len(o.items))
			require.Equal(t, 100, cap(o.items))
			impl.put(p, o)
			require.Equal(t, 1337, o.value) // No panic
		})
	}
}
