// SPDX-License-Identifier: AGPL-3.0-only

//go:build !mimir.tracking_arena

package arena

type Arena struct {
	a *arena
}

func NewArena() *Arena {
	return &Arena{a: newArena()}
}

func New[T any](a *Arena) *T {
	if a == nil {
		return new(T)
	}
	return allocate[T](a.a)
}

func MakeSlice[T any](a *Arena, len, cap int) []T {
	if a == nil {
		return make([]T, len, cap)
	}
	return makeSlice[T](a.a, len, cap)
}

func Clone[T any](s T) T {
	return clone(s)
}

func (a *Arena) Free() {
	if a == nil {
		return
	}
	a.a.Free()
}
