// SPDX-License-Identifier: AGPL-3.0-only

//go:build !goexperiment.arenas

package arena

type arena struct{}

func newArena() *arena {
	return nil
}

func allocate[T any](_ *arena) *T {
	return new(T)
}

func makeSlice[T any](_ *arena, len, cap int) []T {
	return make([]T, len, cap)
}

func clone[T any](s T) T {
	return s
}

func (a *arena) Free() {}
