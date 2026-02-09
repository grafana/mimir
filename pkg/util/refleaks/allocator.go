// SPDX-License-Identifier: AGPL-3.0-only

package refleaks

import (
	"fmt"
	"syscall"
	"unsafe"
)

// An [Allocator] is a handle for a fixed-size allocator into a memory region
// instrumented for reference leaks.
//
// Use [New] and [MakeSlice] to allocate memory with it.
type Allocator interface {
	makeSlice(typeSize, cap int) unsafe.Pointer
}

// A reserver is an implementation of [Allocator] that just records how much
// memory should be reserved for an [allocator] backed by a fixed amount of
// memory pages.
//
// It doesn't actually allocate anything. [allocator] should be used instead.
type reserver struct {
	bytesCap int
}

var dummy int

func (r *reserver) makeSlice(typeSize, cap int) unsafe.Pointer {
	r.bytesCap += padToWordAligned(cap * typeSize)
	return unsafe.Pointer(&dummy)
}

func (r *reserver) pageAlignedSize() int {
	return roundUpToMultiple(r.bytesCap, pageSize)
}

func (r *reserver) allocator() (*allocator, []byte) {
	cap := r.pageAlignedSize()
	// Allocate separate pages for this buffer. We'll detect ref leaks by
	// munmaping the pages on Free, after which trying to access them will
	// segfault.
	b, err := syscall.Mmap(-1, 0, cap, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_PRIVATE|syscall.MAP_ANON)
	if err != nil {
		panic(fmt.Errorf("mmap: %w", err))
	}

	// Restrict the raw bytes slice to the size we actually need, without the
	// page-aligning padding. Otherwise, calling instrumentedBuf.ReaOnlyData()
	// will return uninitialized memory.
	b = b[:r.bytesCap:r.bytesCap]

	return &allocator{b: unsafe.Pointer(unsafe.SliceData(b)), cap: len(b)}, b
}

type allocator struct {
	// b is a pointer to the beginning of the fixed-size arena.
	b    unsafe.Pointer
	cap  int
	used int
}

func (a *allocator) makeSlice(typeSize, capacity int) unsafe.Pointer {
	bytesCap := capacity * typeSize
	if left := a.cap - a.used; left < bytesCap {
		panic(fmt.Errorf("MakeSlice: tried to allocate %d bytes in an Allocator with only %d left", bytesCap, left))
	}
	p := unsafe.Add(a.b, a.used)
	a.used += padToWordAligned(bytesCap)
	return p
}

// MakeSlice allocates a slice in an [Allocator]. If the allocator is nil, it
// falls back to [builtin.make].
func MakeSlice[T any](a Allocator, length, capacity int) []T {
	if a == nil {
		return make([]T, length, capacity)
	}

	var zero T
	typeSize := int(unsafe.Sizeof(zero))
	sp := (*T)(a.makeSlice(typeSize, capacity))
	s := unsafe.Slice(sp, capacity)
	s = s[:length]

	return s
}

// New allocates a value in an [Allocator]. If the allocator is nil, it falls
// back to [builtin.new].
func New[T any](a Allocator) *T {
	if a == nil {
		return new(T)
	}

	s := MakeSlice[T](a, 1, 1)
	return &s[0]
}

var pageSize = syscall.Getpagesize()

// padToWordAligned adds padding to a number so that it becomes a multiple of
// the word size.
//
// Used to ensure object addresses in the instrumented memory region remain
// word-aligned.
func padToWordAligned(i int) int {
	const wordSize = int(unsafe.Sizeof(uintptr(0)))
	return ((i + wordSize - 1) / wordSize) * wordSize
}
