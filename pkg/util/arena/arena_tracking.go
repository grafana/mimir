// SPDX-License-Identifier: AGPL-3.0-only

//go:build mimir.tracking_arena

package arena

import (
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"unsafe"
	"weak"
)

type Arena struct {
	a      *arena
	allocs []alloc
}

func (a *Arena) inner() *arena {
	if a == nil {
		return nil
	}
	return a.a
}

type alloc struct {
	typ     reflect.Type
	caller  callSite
	isFreed func() bool
}

func NewArena() *Arena {
	return &Arena{a: newArena(), allocs: make([]alloc, 0, 100_000)}
}

func New[T any](a *Arena) *T {
	if a == nil {
		return new(T)
	}

	v := allocate[T](a.a)
	weakPtr := weak.Make(v)
	a.allocs = append(a.allocs, alloc{
		typ:    reflect.TypeOf(v).Elem(),
		caller: caller(),
		isFreed: func() bool {
			return weakPtr.Value() == nil
		},
	})
	return v
}

func MakeSlice[T any](a *Arena, len, cap int) []T {
	if a == nil {
		return make([]T, len, cap)
	}

	v := makeSlice[T](a.a, len, cap)
	weakPtr := weak.Make(unsafe.SliceData(v))
	a.allocs = append(a.allocs, alloc{
		typ:    reflect.TypeOf(v),
		caller: caller(),
		isFreed: func() bool {
			return weakPtr.Value() == nil
		},
	})
	return v
}

func Clone[T any](s T) T {
	return clone(s)
}

func caller() callSite {
	_, file, line, ok := runtime.Caller(2)
	return callSite{file: file, line: line, ok: ok}
}

type callSite struct {
	file string
	line int
	ok   bool
}

func (c callSite) String() string {
	if !c.ok {
		return "?"
	}
	return fmt.Sprintf("%s:%d", c.file, c.line)
}

func (a *Arena) Free() {
	a.a.Free()

	runtime.GC()
	runtime.GC()
	var liveAllocs strings.Builder
	for _, a := range a.allocs {
		if !a.isFreed() {
			fmt.Fprintf(&liveAllocs, "%s\n\t%s\n", a.typ, a.caller)
		}
	}
	if liveAllocs.Len() > 0 {
		panic("references to arena allocations outlive the arena:\n\n" + liveAllocs.String())
	}
	a.allocs = nil
}
